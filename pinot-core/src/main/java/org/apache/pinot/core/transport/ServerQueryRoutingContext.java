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
package org.apache.pinot.core.transport;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


public class ServerQueryRoutingContext {
  private final BrokerRequest _brokerRequest;
  private final Pair<List<String>, List<String>> _segmentsToQuery;
  private final ServerRoutingInstance _serverRoutingInstance;
  private final TableType _tableType;

  public ServerQueryRoutingContext(BrokerRequest brokerRequest, Pair<List<String>, List<String>> segmentsToQuery,
      ServerRoutingInstance serverRoutingInstance) {
    // TODO(egalpin): somewhere, should we create a map that uses the BrokerRequest as the key and combines all
    //  segmentsToQuery? this would allow servers to combine results from segments that share a common query before
    //  sending back to broker
    _brokerRequest = brokerRequest;
    _segmentsToQuery = segmentsToQuery;
    _serverRoutingInstance = serverRoutingInstance;
    String tableName = _brokerRequest.getPinotQuery().getDataSource().getTableName();
    _tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
  }

  public BrokerRequest getBrokerRequest() {
    return _brokerRequest;
  }

  public Pair<List<String>, List<String>> getSegmentsToQuery() {
    return _segmentsToQuery;
  }

  public List<String> getRequiredSegmentsToQuery() {
    return getSegmentsToQuery().getLeft();
  }

  public List<String> getOptionalSegmentsToQuery() {
    return getSegmentsToQuery().getRight();
  }

  public TableType getTableType() {
    return _tableType;
  }

  public ServerRoutingInstance getServerRoutingInstance() {
    return _serverRoutingInstance;
  }
}
