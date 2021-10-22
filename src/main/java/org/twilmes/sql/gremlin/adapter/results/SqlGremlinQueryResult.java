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

package org.twilmes.sql.gremlin.adapter.results;

import lombok.Getter;
import lombok.Setter;
import org.twilmes.sql.gremlin.adapter.converter.SqlMetadata;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinTableBase;
import org.twilmes.sql.gremlin.adapter.converter.schema.gremlin.GremlinProperty;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Getter
public class SqlGremlinQueryResult {
    private final List<String> columns;
    private final List<String> columnTypes = new ArrayList<>();
    private final Object assertEmptyLock = new Object();
    private final BlockingQueue<List<Object>> blockingQueueRows = new LinkedBlockingQueue<>();
    private boolean isEmpty = false;
    private SQLException paginationException = null;
    private Thread currThread = null;
    public static String EMPTY_MESSAGE = "No more results.";

    public SqlGremlinQueryResult(final List<String> columns, final List<GremlinTableBase> gremlinTableBases) throws SQLException {
        this.columns = columns;

        for (final String column : columns) {
            GremlinProperty col = null;
            for (final GremlinTableBase gremlinTableBase : gremlinTableBases) {
                if (gremlinTableBase.getColumns().containsKey(column)) {
                    col = gremlinTableBase.getColumn(column);
                    break;
                }
            }
            columnTypes.add((col == null || col.getType() == null) ? "string" : col.getType());
        }
    }

    public SqlGremlinQueryResult(final List<String> columns, final List<GremlinTableBase> gremlinTableBases,
                                 final SqlMetadata sqlMetadata) throws SQLException {
        System.out.println("SqlGremlinQueryResult start");
        this.columns = columns;
        for (final String column : columns) {
            GremlinProperty col = null;
            for (final GremlinTableBase gremlinTableBase : gremlinTableBases) {
                if (sqlMetadata.getTableHasColumn(gremlinTableBase, column)) {
                    col = gremlinTableBase.getColumn(column);
                    break;
                }
            }
            columnTypes.add((col == null || col.getType() == null) ? "string" : col.getType());
        }
        System.out.println("SqlGremlinQueryResult end");
    }

    public void setPaginationException(final SQLException e) {
        System.out.println("setPaginationException");
        synchronized (assertEmptyLock) {
            System.out.println("setPaginationException assertEmptyLock");
            paginationException = e;
            if (currThread != null && blockingQueueRows.size() == 0) {
                currThread.interrupt();
            }
        }
    }

    public boolean getIsEmpty() throws SQLException {
        System.out.println("getIsEmpty");
        if (paginationException == null) {
            return isEmpty;
        }
        throw paginationException;
    }

    public void assertIsEmpty() {
        System.out.println("assertIsEmpty");
        synchronized (assertEmptyLock) {
            if (currThread != null && blockingQueueRows.size() == 0) {
                currThread.interrupt();
            }
            isEmpty = true;
        }
    }

    public void addResults(final List<List<Object>> rows) {
        System.out.println("addResults");
        blockingQueueRows.addAll(rows);
    }

    public Object getResult() throws SQLException {
        System.out.println("getResult");
        try {
            synchronized (assertEmptyLock) {
                System.out.println("getResult assertEmptyLock");
                // Pass current thread in, and interrupt in assertIsEmpty.
                this.currThread = Thread.currentThread();
                if (getIsEmpty() && blockingQueueRows.size() == 0) {
                    throw new SQLException(EMPTY_MESSAGE);
                }
            }
            System.out.println("blockingQueueRows.take()");
            return this.blockingQueueRows.take();
        } catch (final InterruptedException ignored) {
            final boolean isEmpty;
            synchronized (assertEmptyLock) {
                isEmpty = getIsEmpty();
            }
            if (!isEmpty && (paginationException == null)) {
                // Interrupt is not relevant - retry.
                System.out.println("Interrupt is not relevant - retry");
                return getResult();
            }
            System.out.println("InterruptedException ignored " + isEmpty + " - " + paginationException);
            if (paginationException != null) {
                throw paginationException;
            }
            throw new SQLException(EMPTY_MESSAGE);
        }
    }
}
