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
package org.apache.pinot.udf.test;

import java.util.Iterator;
import java.util.stream.Stream;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/// An interface for executing queries in Pinot function tests.
///
/// For example, one implementation can start a local cluster on the same JVM while another can connect to a remote
/// cluster.
public interface UdfTestCluster extends AutoCloseable {

  void start();

  /// Adds a table to the cluster with the given schema and table configuration.
  /// Implementations can assume that the table doesn not exist in the cluster.
  void addTable(Schema schema, TableConfig tableConfig);

  /// Inserts the given rows into the specified table.
  void addRows(String tableName, Schema schema, Stream<GenericRow> rows);

  /// Executes a query and returns an iterator over the results.
  Iterator<GenericRow> query(ExecutionContext context, String sql);

  /// Closes the cluster and releases any resources that were allocated on start.
  /// Implementations must be sure that any table created in the cluster is removed
  @Override
  void close()
      throws Exception;

  class ExecutionContext {
    private final UdfExample.NullHandling _nullHandlingMode;
    private final boolean _useMultistageEngine;

    public ExecutionContext(UdfExample.NullHandling nullHandlingMode, boolean useMultistageEngine) {
      _nullHandlingMode = nullHandlingMode;
      _useMultistageEngine = useMultistageEngine;
    }

    public UdfExample.NullHandling getNullHandlingMode() {
      return _nullHandlingMode;
    }

    public boolean isUseMultistageEngine() {
      return _useMultistageEngine;
    }
  }
}
