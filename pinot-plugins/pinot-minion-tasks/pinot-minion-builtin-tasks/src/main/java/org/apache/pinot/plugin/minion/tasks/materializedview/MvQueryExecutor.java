/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.minion.tasks.materializedview;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;


/**
 * Abstraction for executing SQL queries against Pinot and returning typed results.
 *
 * <p>Implementations may use different transport protocols (e.g. gRPC, Arrow Flight)
 * and are responsible for broker discovery, connection management, load balancing,
 * and response deserialization.
 *
 * <p>Instances are expected to be long-lived and thread-safe so that they can be
 * shared across multiple task executions.
 */
public interface MvQueryExecutor extends Closeable {

  /**
   * Executes a SQL query and returns the result as a {@link QueryResult}.
   *
   * @param sql         the SQL query to execute
   * @param authHeaders authentication headers to include in the request
   * @return the query result containing a data schema and rows
   * @throws IOException if communication with the broker fails
   */
  QueryResult executeQuery(String sql, Map<String, String> authHeaders)
      throws IOException;

  /**
   * Holds the result of a query execution: the data schema and the row data.
   */
  class QueryResult {
    private final DataSchema _dataSchema;
    private final List<Object[]> _rows;

    public QueryResult(DataSchema dataSchema, List<Object[]> rows) {
      _dataSchema = dataSchema;
      _rows = rows;
    }

    public DataSchema getDataSchema() {
      return _dataSchema;
    }

    public List<Object[]> getRows() {
      return _rows;
    }
  }
}
