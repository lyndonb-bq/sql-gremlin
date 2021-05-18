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

package org.twilmes.sql.gremlin.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import java.util.Arrays;
import java.util.List;

/**
 * Created by twilmes on 9/25/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class GremlinTableScan extends TableScan implements GremlinRel {
    private final GremlinTable gremlinTable;
    private final int[] fields;

    protected GremlinTableScan(final RelOptCluster cluster, final RelTraitSet traitSet,
                               final RelOptTable table, final GremlinTable gremlinTable, final int[] fields) {
        super(cluster, traitSet, table);
        this.gremlinTable = gremlinTable;
        this.fields = fields;

        assert gremlinTable != null;
        assert getConvention() == CONVENTION;
    }

    public GremlinTable getGremlinTable() {
        return gremlinTable;
    }

    @Override
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override
    public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        final RelDataTypeFactory.FieldInfoBuilder builder =
                getCluster().getTypeFactory().builder();
        for (final int field : fields) {
            builder.add(fieldList.get(field));
        }
        return builder.build();
    }

    @Override
    public void register(final RelOptPlanner planner) {
        planner.addRule(GremlinToEnumerableConverterRule.INSTANCE);
        for (final RelOptRule rule : GremlinRules.RULES) {
            planner.addRule(rule);
        }
    }

    @Override
    public void implement(final Implementor implementor) {
        implementor.gremlinTable = gremlinTable;
        implementor.table = table;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final GremlinTableScan that = (GremlinTableScan) o;

        if (!Arrays.equals(fields, that.fields)) {
            return false;
        }
        return gremlinTable.equals(that.gremlinTable);
    }

    @Override
    public int hashCode() {
        int result = gremlinTable.hashCode();
        result = 31 * result + Arrays.hashCode(fields);
        return result;
    }
}
