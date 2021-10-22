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

package org.twilmes.sql.gremlin.adapter.results.pagination;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.twilmes.sql.gremlin.adapter.converter.ast.nodes.select.GremlinSqlSelectSingle;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinTableBase;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SimpleDataReader implements GetRowFromMap {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleDataReader.class);
    final String label;
    final List<String> columnNames;

    public SimpleDataReader(final GremlinTableBase tableDef, final List<String> columnNames) {
        System.out.println("SIMPLE DATA READER START");
        this.label = tableDef.getLabel();
        this.columnNames = columnNames;
        System.out.println("SIMPLE DATA READER END");
    }

    @Override
    public Object[] execute(final Map<String, Object> map) {
        System.out.println("SIMPLE DATA READER execute");
        final Object[] row = new Object[columnNames.size()];
        int i = 0;
        for (final String column : columnNames) {
            final Optional<String> tableKey =
                    map.keySet().stream().filter(key -> key.equalsIgnoreCase(label)).findFirst();
            if (!tableKey.isPresent()) {
                row[i++] = null;
                continue;
            }

            final Optional<String> columnKey = ((Map<String, Object>) map.get(tableKey.get())).keySet().stream()
                    .filter(key -> key.equalsIgnoreCase(column)).findFirst();
            if (!columnKey.isPresent()) {
                row[i++] = null;
                continue;
            }
            row[i++] = ((Map<String, Object>) map.get(tableKey.get())).getOrDefault(columnKey.get(), null);
        }
        return row;
    }
}
