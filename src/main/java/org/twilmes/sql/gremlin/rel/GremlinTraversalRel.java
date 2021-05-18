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
import org.apache.calcite.rel.RelNode;
import java.util.List;

/**
 * Created by twilmes on 11/26/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public interface GremlinTraversalRel extends RelNode {

    /**
     * Calling convention for relational operations that occur in Graph.
     */
    Convention CONVENTION = new Convention.Impl("TRAVERSAL", GremlinTraversalRel.class);

    void implement(Implementor implementor);

    class Implementor {
        List<Object> rows;

        public void visitChild(final int ordinal, final RelNode input) {
            assert ordinal == 0;
            ((GremlinTraversalRel) input).implement(this);
        }
    }
}