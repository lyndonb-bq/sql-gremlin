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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;

/**
 * List of rules that get pushed down and converted into GremlinTraversals.  Right now
 * only filter is pushed down using rules.  Joins are converted, but handled the by RelWalker
 * utilities.
 * <p>
 * Created by twilmes on 11/14/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class GremlinRules {

    public static final RelOptRule[] RULES = {
            GremlinFilterRule.INSTANCE
    };

    abstract static class GremlinConverterRule extends ConverterRule {
        protected final Convention out;

        public GremlinConverterRule(
                final Class<? extends RelNode> clazz,
                final RelTrait in,
                final Convention out,
                final String description) {
            super(clazz, in, out, description);
            this.out = out;
        }
    }

    private static class GremlinFilterRule extends GremlinConverterRule {
        private static final GremlinFilterRule INSTANCE = new GremlinFilterRule();

        private GremlinFilterRule() {
            super(LogicalFilter.class, Convention.NONE, GremlinRel.CONVENTION,
                    "GremlinFilterRule");
        }

        public RelNode convert(final RelNode rel) {
            final LogicalFilter filter = (LogicalFilter) rel;
            final RelTraitSet traitSet = filter.getTraitSet().replace(out);
            return new GremlinFilter(
                    rel.getCluster(),
                    traitSet,
                    convert(filter.getInput(), out),
                    filter.getCondition());
        }
    }

}
