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

import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.rel.RelNode;

/**
 * Created by twilmes on 12/8/15.
 * Modified by lyndonb-bq on 05/17/21.
 */
public class RelUtils {

    /**
     * Is this a RelNode we can convert into Gremlin?
     *
     * @param node the node to check
     * @return true if a convert operation is supported
     */
    public static Boolean isConvertable(final RelNode node) {
        return !(node instanceof EnumerableAggregate || node instanceof EnumerableCalc
                || node instanceof EnumerableLimit || node instanceof EnumerableSort);
    }
}
