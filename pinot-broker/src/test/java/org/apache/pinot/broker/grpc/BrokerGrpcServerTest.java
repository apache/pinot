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
package org.apache.pinot.broker.grpc;

import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.executor.sql.SqlQueryExecutor;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BrokerGrpcServerTest {

  @Test
  public void testMetadataQueryDelegatesToSqlQueryExecutor() {
    PinotConfiguration brokerConf = new PinotConfiguration();
    brokerConf.setProperty(CommonConstants.Broker.Grpc.KEY_OF_GRPC_PORT, 12345);

    BrokerRequestHandler brokerRequestHandler = Mockito.mock(BrokerRequestHandler.class);
    SqlQueryExecutor sqlQueryExecutor = Mockito.mock(SqlQueryExecutor.class);

    BrokerResponseNative metadataResponse = new BrokerResponseNative();
    DataSchema dataSchema =
        new DataSchema(new String[]{"tableName"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    metadataResponse.setResultTable(new ResultTable(dataSchema, Collections.emptyList()));
    Mockito.when(sqlQueryExecutor.executeMetadataStatement(ArgumentMatchers.any(SqlNodeAndOptions.class),
        ArgumentMatchers.anyMap())).thenReturn(metadataResponse);

    BrokerGrpcServer grpcServer =
        new BrokerGrpcServer(brokerConf, "broker_0", BrokerMetrics.get(), brokerRequestHandler, sqlQueryExecutor);

    Broker.BrokerRequest request =
        Broker.BrokerRequest.newBuilder()
            .setSql("SHOW TABLES")
            .putMetadata(CommonConstants.Broker.Grpc.COMPRESSION, "NONE")
            .putMetadata(CommonConstants.Broker.Grpc.ENCODING, "JSON")
            .build();

    CollectingObserver observer = new CollectingObserver();
    grpcServer.submit(request, observer);

    Mockito.verify(sqlQueryExecutor, Mockito.times(1))
        .executeMetadataStatement(ArgumentMatchers.any(SqlNodeAndOptions.class), ArgumentMatchers.anyMap());
    Mockito.verifyNoInteractions(brokerRequestHandler);

    Assert.assertNull(observer._error);
    Assert.assertTrue(observer._completed);
    Assert.assertEquals(observer._responses.size(), 2, "Expected metadata and schema blocks only");
  }

  private static class CollectingObserver implements StreamObserver<Broker.BrokerResponse> {
    private final List<Broker.BrokerResponse> _responses = new ArrayList<>();
    private volatile boolean _completed;
    private volatile Throwable _error;

    @Override
    public void onNext(Broker.BrokerResponse value) {
      _responses.add(value);
    }

    @Override
    public void onError(Throwable t) {
      _error = t;
    }

    @Override
    public void onCompleted() {
      _completed = true;
    }
  }
}
