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
package org.apache.pinot.client.grpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.client.ExecutionStats;
import org.apache.pinot.client.PinotClientException;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.utils.DataSchema;


/**
 * A Pinot result set group, containing the results given back by Pinot for a given query.
 */
public class GrpcResultSetGroup {
  private final DataSchema _schema;
  private final ExecutionStats _executionStats;
  private final List<PinotClientException> _exceptions;
  private final Iterator<Broker.BrokerResponse> _brokerResponseIterator;
  private ResultSet _currentResultSet;

  public GrpcResultSetGroup(Iterator<Broker.BrokerResponse> brokerResponseIterator)
      throws IOException {

    // Process metadata
    ObjectNode brokerResponseJson = GrpcUtils.extractMetadataJson(brokerResponseIterator.next());
    _executionStats = GrpcUtils.extractExecutionStats(brokerResponseJson);
    _exceptions = getPinotClientExceptions(brokerResponseJson.get("exceptions"));
    // Process schema
    if (brokerResponseIterator.hasNext()) {
      _schema = GrpcUtils.extractSchema(brokerResponseIterator.next());
    } else {
      _schema = new DataSchema(new String[0], new DataSchema.ColumnDataType[0]);
    }
    _brokerResponseIterator = brokerResponseIterator;
  }

  public boolean hasNextResultSet() {
    return _brokerResponseIterator.hasNext();
  }

  /**
   * Obtains the next result set from the iterator
   *
   * @return The result set
   */
  @Nullable
  public ResultSet getNextResultSet() {
    _currentResultSet = new GrpcResultSet(_schema, _brokerResponseIterator.next());
    return _currentResultSet;
  }

  private static List<PinotClientException> getPinotClientExceptions(
      @Nullable JsonNode exceptionsJson) {
    List<PinotClientException> exceptions = new ArrayList<>();
    if (exceptionsJson != null && exceptionsJson.isArray()) {
      for (int i = 0; i < exceptionsJson.size(); i++) {
        exceptions.add(new PinotClientException(exceptionsJson.get(i).toPrettyString()));
      }
    }
    return exceptions;
  }

  public ExecutionStats getExecutionStats() {
    return _executionStats;
  }

  public List<PinotClientException> getExceptions() {
    return _exceptions;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(_schema);
    sb.append("\n");
    sb.append(_currentResultSet);
    sb.append("\n");
    sb.append(_executionStats.toString());
    sb.append("\n");
    for (PinotClientException exception : _exceptions) {
      sb.append(exception);
      sb.append("\n");
    }
    return sb.toString();
  }
}
