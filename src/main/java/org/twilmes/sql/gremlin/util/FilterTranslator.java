/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.twilmes.sql.gremlin.util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.twilmes.sql.gremlin.schema.TableDef;
import org.twilmes.sql.gremlin.schema.TableUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This class builds heavily upon Apache Calcite's Mongo adapter Translator class.  It just has a few tweaks to
 * do predicate conversions to Gremlin.  Any bugs are my own and not in the original!
 *
 * @see <a href="https://github.com/apache/calcite/blob/master/mongodb/src/main/java/org/apache/calcite/adapter/mongodb/MongoFilter.java">Calcite Mongo Translator</a>
 * <p>
 * Created by twilmes on xx/xx/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class FilterTranslator {
    final JsonBuilder builder = new JsonBuilder();
    final Multimap<String, Pair<String, RexLiteral>> multimap = HashMultimap.create();
    final Map<String, RexLiteral> eqMap = new LinkedHashMap<>();
    private final List<String> fieldNames;
    private final TableDef tableDef;

    public FilterTranslator(final TableDef tableDef, final List<String> fieldNames) {
        this.fieldNames = fieldNames;
        this.tableDef = tableDef;
    }

    private static Object literalValue(final RexLiteral literal) {
        return literal.getValue2();
    }

    public GraphTraversal translateMatch(final RexNode condition) {
        final GraphTraversal traversal = translateOr(condition);
        return traversal;
    }

    private GraphTraversal translateOr(final RexNode condition) {
        final List<GraphTraversal> list = new ArrayList<>();
        for (final RexNode node : RelOptUtil.disjunctions(condition)) {
            list.addAll(translateAnd(node));
        }
        switch (list.size()) {
            case 1:
                return list.get(0);
            default:
                final Map<String, Object> map = builder.map();
                map.put("$or", list);
                return __.__().or(list.toArray(new GraphTraversal[list.size()]));
        }
    }

    /**
     * Translates a condition that may be an AND of other conditions. Gathers
     * together conditions that apply to the same field.
     */
    private List<GraphTraversal> translateAnd(final RexNode node0) {
        eqMap.clear();
        multimap.clear();
        for (final RexNode node : RelOptUtil.conjunctions(node0)) {
            translateMatch2(node);
        }
        final Map<String, Object> map = builder.map();
        for (final Map.Entry<String, RexLiteral> entry : eqMap.entrySet()) {
            multimap.removeAll(entry.getKey());
            map.put(entry.getKey(), literalValue(entry.getValue()));
        }
        for (final Map.Entry<String, Collection<Pair<String, RexLiteral>>> entry
                : multimap.asMap().entrySet()) {
            final Map<String, Object> map2 = builder.map();
            for (final Pair<String, RexLiteral> s : entry.getValue()) {
                addPredicate(map2, s.left, literalValue(s.right));
            }
            map.put(entry.getKey(), map2);
        }

        final List<GraphTraversal> traversals = new ArrayList<>();
        final GraphTraversal andTraversal = __.identity();
        // process map
        for (final Map.Entry<String, Object> entry : map.entrySet()) {
            final String fieldName = entry.getKey().toLowerCase();
            Object value = entry.getValue();
            if (value instanceof Map) {
                final Map<String, Object> mapValue = (Map) value;
                for (final Map.Entry<String, Object> valEntry : mapValue.entrySet()) {
                    final String op = valEntry.getKey();
                    Object val = valEntry.getValue();
                    val = TableUtil.convertType(val, tableDef.getColumn(fieldName));
                    P predicate = null;
                    switch (op) {
                        case "$gt":
                            predicate = P.gt(val);
                            break;
                        case "$gte":
                            predicate = P.gte(val);
                            break;
                        case "$lt":
                            predicate = P.lt(val);
                            break;
                        case "$lte":
                            predicate = P.lte(val);
                            break;
                        case "$ne":
                            predicate = P.neq(val);
                            break;
                        default:
                            break;
                    }
                    if (fieldName.endsWith("_id")) {
                        andTraversal.has(T.id, predicate);
                    } else {
                        final String propertyKey = TableUtil.getProperty(tableDef, fieldName);
                        andTraversal.has(propertyKey, predicate);
                    }
                }
            } else {
                if (fieldName.endsWith("_id")) {
                    andTraversal.has(T.id, value);
                } else {
                    final String propertyKey = TableUtil.getProperty(tableDef, fieldName);
                    value = TableUtil.convertType(value, tableDef.getColumn(fieldName));
                    andTraversal.has(propertyKey, value);
                }
            }
        }
        traversals.add(andTraversal);

        return traversals;
    }

    private void addPredicate(final Map<String, Object> map, final String op, final Object v) {
        if (map.containsKey(op) && stronger(op, map.get(op), v)) {
            return;
        }
        map.put(op, v);
    }

    /**
     * Returns whether {@code v0} is a stronger value for operator {@code key}
     * than {@code v1}.
     *
     * <p>For example, {@code stronger("$lt", 100, 200)} returns true, because
     * "&lt; 100" is a more powerful condition than "&lt; 200".
     */
    private boolean stronger(final String key, final Object v0, final Object v1) {
        if (key.equals("$lt") || key.equals("$lte")) {
            if (v0 instanceof Number && v1 instanceof Number) {
                return ((Number) v0).doubleValue() < ((Number) v1).doubleValue();
            }
            if (v0 instanceof String && v1 instanceof String) {
                return v0.toString().compareTo(v1.toString()) < 0;
            }
        }
        if (key.equals("$gt") || key.equals("$gte")) {
            return stronger("$lt", v1, v0);
        }
        return false;
    }

    private Void translateMatch2(final RexNode node) {
        switch (node.getKind()) {
            case EQUALS:
                return translateBinary(null, null, (RexCall) node);
            case LESS_THAN:
                return translateBinary("$lt", "$gt", (RexCall) node);
            case LESS_THAN_OR_EQUAL:
                return translateBinary("$lte", "$gte", (RexCall) node);
            case NOT_EQUALS:
                return translateBinary("$ne", "$ne", (RexCall) node);
            case GREATER_THAN:
                return translateBinary("$gt", "$lt", (RexCall) node);
            case GREATER_THAN_OR_EQUAL:
                return translateBinary("$gte", "$lte", (RexCall) node);
            default:
                throw new AssertionError("cannot translate " + node);
        }
    }

    /**
     * Translates a call to a binary operator, reversing arguments if
     * necessary.
     */
    private Void translateBinary(final String op, final String rop, final RexCall call) {
        final RexNode left = call.operands.get(0);
        final RexNode right = call.operands.get(1);
        boolean b = translateBinary2(op, left, right);
        if (b) {
            return null;
        }
        b = translateBinary2(rop, right, left);
        if (b) {
            return null;
        }
        throw new AssertionError("cannot translate op " + op + " call " + call);
    }

    /**
     * Translates a call to a binary operator. Returns whether successful.
     */
    private boolean translateBinary2(final String op, final RexNode left, final RexNode right) {
        final RexLiteral rightLiteral;
        switch (right.getKind()) {
            case CAST:
                rightLiteral = (RexLiteral) ((RexCall) right).getOperands().get(0);
                break;
            case LITERAL:
                rightLiteral = (RexLiteral) right;
                break;
            default:
                return false;
        }

        switch (left.getKind()) {
            case INPUT_REF:
                final RexInputRef left1 = (RexInputRef) left;
                final String name = fieldNames.get(left1.getIndex());
                translateOp2(op, name, rightLiteral);
                return true;
            case CAST:
                return translateBinary2(op, ((RexCall) left).operands.get(0), right);
            case OTHER_FUNCTION:
                // fall through
            default:
                return false;
        }
    }

    private void translateOp2(final String op, final String name, final RexLiteral right) {
        if (op == null) {
            // E.g.: name = 'George'
            eqMap.put(name, right);
        } else {
            // E.g. year < 100
            multimap.put(name, Pair.of(op, right));
        }
    }
}
