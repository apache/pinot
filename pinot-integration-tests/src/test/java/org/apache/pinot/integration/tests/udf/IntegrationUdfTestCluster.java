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
package org.apache.pinot.integration.tests.udf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.grpc.GrpcConnection;
import org.apache.pinot.client.grpc.GrpcUtils;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.controller.helix.ControllerRequestClient;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.udf.test.UdfTestCluster;

/// A [UdfTestCluster] implementation that starts a Pinot cluster for testing UDFs.
public class IntegrationUdfTestCluster extends BaseClusterIntegrationTest
    implements UdfTestCluster {
  private boolean _started;
  private GrpcConnection _grpcConnection;

  public void start() {
    if (_started) {
      return;
    }
    _started = true;
    // Start Zookeeper
    startZk();
    // Start the Pinot cluster
    try {
      startController();
      startBroker();
      startServer();
      startMinion();

      _grpcConnection = ConnectionFactory.fromHostListGrpc(new Properties(),
          List.of(getBrokerGrpcEndpoint()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    // Notice that this shutdown seems to leak some non-daemon threads.
    // This means we need to kill the JVM to stop the test.
    // That is fine for TestNG, but not for other callers (like main methods)
    if (!_started) {
      return;
    }
    _started = false;
    // Stop the Pinot cluster
    try {
      if (_grpcConnection != null) {
        _grpcConnection.close();
      }

      stopMinion();
      stopServer();
      stopBroker();
      stopController();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    // Stop Zookeeper
    stopZk();
  }

  @Override
  public void addTable(Schema schema, TableConfig tableConfig) {
    ControllerRequestClient client = getControllerRequestClient();
    try {
      client.addSchema(schema);
      client.addTableConfig(tableConfig);
    } catch (Exception e) {
      throw new RuntimeException("Failed to add table: " + tableConfig.getTableName(), e);
    }
  }

  @Override
  public void addRows(String tableName, Schema schema, Stream<GenericRow> rows) {
    try {
      ControllerRequestClient client = getControllerRequestClient();
      TableConfig tableConfig = client.getTableConfig(tableName, TableType.OFFLINE);

      int numRows = 0;
      File tempFile = File.createTempFile(tableName, ".avro");

      try (Stream<GenericRow> closeMe = rows;
          AvroSink sink = new AvroSink(schema, tempFile)) {

        Iterator<GenericRow> it = rows.iterator();
        while (it.hasNext()) {
          GenericRow row = it.next();
          try {
            sink.consume(row);
          } catch (Exception e) {
            throw new RuntimeException("Failed to add row " + row + " to table: " + tableName, e);
          }
          numRows++;
        }
      }

      try {
        createAndUploadSegmentFromFile(tableConfig, schema, tempFile, FileFormat.AVRO, numRows, 10_000);
      } catch (Exception e) {
        throw new RuntimeException("Failed to add rows to table: " + tableName, e);
      } finally {
        if (tempFile.exists()) {
          tempFile.delete();
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Iterator<GenericRow> query(ExecutionContext context, String sql) {
    Iterator<Broker.BrokerResponse> response = queryGrpc(context, sql);
    // Process metadata
    if (!response.hasNext()) {
      throw new RuntimeException("No response received from Pinot");
    }
    ObjectNode brokerResponseJson = null;
    try {
      brokerResponseJson = GrpcUtils.extractMetadataJson(response.next());
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot extract metadata from GRPC request", e);
    }
    // Directly return when there is exception
    if (brokerResponseJson.has("exceptions") && !brokerResponseJson.get("exceptions").isEmpty()) {
      JsonNode exceptions = brokerResponseJson.get("exceptions");
      QueryException toThrow = null;
      try {
        if (exceptions.isArray() && !exceptions.isEmpty()) {
          // If there is only one exception, throw it directly
          JsonNode exceptionNode = exceptions.get(0);
          QueryErrorCode errorCode = QueryErrorCode.fromErrorCode(exceptionNode.get("errorCode").asInt());
          toThrow = errorCode.asException(exceptionNode.get("message").toString());
        }
      } catch (Exception e) {
        // nothing to do here
      }
      throw toThrow != null ? toThrow : new RuntimeException(exceptions.toString());
    }
    // Process schema
    if (!response.hasNext()) {
      throw new RuntimeException("No schema found in the response");
    }
    DataSchema dataSchema;
    try {
      dataSchema = GrpcUtils.extractSchema(response.next());
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot extract schema from GRPC request", e);
    }
    return new Iterator<GenericRow>() {
      private Iterator<Object[]> _currentBlock = null;

      @Override
      public boolean hasNext() {
        return response.hasNext() || (_currentBlock != null && _currentBlock.hasNext());
      }

      @Override
      public GenericRow next() {
        if (_currentBlock != null && _currentBlock.hasNext()) {
          Object[] row = _currentBlock.next();
          GenericRow genericRow = new GenericRow();
          for (int i = 0; i < dataSchema.size(); i++) {
            String columnName = dataSchema.getColumnName(i);
            genericRow.putValue(columnName, row[i]);
          }
          return genericRow;
        }
        try {
          _currentBlock = GrpcUtils.extractResultTable(response.next(), dataSchema).getRows().iterator();
        } catch (Exception e) {
          throw new RuntimeException("Failed to extract row from GRPC response", e);
        }
        return next();
      }
    };
  }

  public Iterator<Broker.BrokerResponse> queryGrpc(ExecutionContext context, String sql) {
    String prefix = "";
    if (context.getNullHandlingMode() == UdfExample.NullHandling.ENABLED) {
      prefix = "SET enableNullHandling=true;\n" + prefix;
    }
    if (context.isUseMultistageEngine()) {
      prefix = "SET useMultistageEngine=true;\n" + prefix;
    }
    return _grpcConnection.executeWithIterator(prefix + sql);
  }
}
