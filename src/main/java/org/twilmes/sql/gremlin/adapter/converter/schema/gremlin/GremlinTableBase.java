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

package org.twilmes.sql.gremlin.adapter.converter.schema.gremlin;

import lombok.Getter;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.util.Pair;
import org.twilmes.sql.gremlin.adapter.converter.schema.calcite.GremlinRel;
import org.twilmes.sql.gremlin.adapter.converter.schema.calcite.GremlinTableScan;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
public class GremlinTableBase extends AbstractQueryableTable implements TranslatableTable {
    public static String ID = "_ID";
    public static String IN_ID = "_IN" + ID;
    public static String OUT_ID = "_OUT" + ID;
    private final String label;
    private final Boolean isVertex;
    private final Map<String, GremlinProperty> columns;

    public GremlinTableBase(final String label, final Boolean isVertex,
                            final Map<String, GremlinProperty> columns) {
        super(Object[].class);
        this.label = label;
        this.isVertex = isVertex;
        this.columns = columns;
    }

    public GremlinProperty getColumn(final String column) throws SQLException {
        for (final String key : columns.keySet()) {
            if (key.equalsIgnoreCase(column)) {
                return columns.get(key);
            }
        }
        throw new SQLException(String.format(
                "Error: Could not find column '%s' on %s with label '%s'.", column, isVertex ? "vertex" : "edge",
                label));
    }

    @Override
    public <T> Queryable<T> asQueryable(final QueryProvider queryProvider, final SchemaPlus schema,
                                        final String tableName) {
        return null;
    }

    @Override
    public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable relOptTable) {
        final int[] fields = new int[columns.size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = i;
        }
        return new GremlinTableScan(context.getCluster(), context.getCluster().traitSetOf(GremlinRel.CONVENTION),
                relOptTable, fields);
    }

    @Override
    public RelDataType getRowType(final RelDataTypeFactory relDataTypeFactory) {
        final List<String> names = new ArrayList<>();
        final List<RelDataType> types = new ArrayList<>();
        for (final Map.Entry<String, GremlinProperty> entry : columns.entrySet()) {
            names.add(entry.getKey());
            types.add(relDataTypeFactory.createJavaType(getType(entry.getValue().getType())));
        }
        return relDataTypeFactory.createStructType(Pair.zip(names, types));
    }

    private Class<?> getType(final String className) {
        if (className.equalsIgnoreCase("string")) {
            return String.class;
        } else if (className.equalsIgnoreCase("integer")) {
            return Integer.class;
        } else if (className.equalsIgnoreCase("double")) {
            return Double.class;
        } else if (className.equalsIgnoreCase("long")) {
            return Long.class;
        } else if (className.equalsIgnoreCase("boolean")) {
            return Boolean.class;
        } else if (className.equalsIgnoreCase("date") || className.equalsIgnoreCase("long_date")) {
            return java.sql.Date.class;
        } else if (className.equalsIgnoreCase("timestamp") || className.equalsIgnoreCase("long_timestamp")) {
            return java.sql.Timestamp.class;
        } else {
            return null;
        }
    }
}
