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

package org.twilmes.sql.gremlin.processor;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.twilmes.sql.gremlin.ParseException;

/**
 * Created by twilmes on 11/14/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class QueryPlanner {

    private final Planner planner;

    public QueryPlanner(final FrameworkConfig frameworkConfig) {
        this.planner = Frameworks.getPlanner(frameworkConfig);
    }

    public RelNode plan(String sql) {
        try {
            // hack to capitalize group by columns...not sure why the parser complains if the group by columns
            // are not capitalized
            final int groupByIndex = sql.toLowerCase().indexOf("group by");
            if (groupByIndex > -1) {
                final String firstPart = sql.substring(0, groupByIndex + "group by".length());
                final String groupPart = sql.substring(groupByIndex + "group by".length());
                sql = firstPart + groupPart.toUpperCase();
                // from index
                final int fromIndex = sql.toLowerCase().indexOf("from");
                sql = sql.substring(0, fromIndex).toUpperCase() + sql.substring(fromIndex);
            }
            final SqlNode parse = planner.parse(sql);

            final SqlNode validate = planner.validate(parse);
            final RelRoot convert = planner.rel(validate);

            // final RelNode convert = planner.convert(validate);
            final RelTraitSet traitSet = planner.getEmptyTraitSet()
                    .replace(EnumerableConvention.INSTANCE);

            return planner.transform(0, traitSet, convert.project());
        } catch (final Exception e) {
            throw new ParseException("Error parsing: " + sql + " - " + e, e);
        }
    }

    public String explain(final RelNode node) {
        return RelOptUtil.dumpPlan("", node, false,
                SqlExplainLevel.DIGEST_ATTRIBUTES);
    }
}
