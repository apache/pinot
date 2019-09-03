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
package org.apache.pinot.server.request;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAccumulator;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.core.common.datatable.DataTableImplV2;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.QueryScheduler;
import org.apache.pinot.core.query.scheduler.resources.UnboundedResourceManager;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.serde.SerDe;
import org.apache.thrift.protocol.TCompactProtocol;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


public class ScheduledRequestHandlerTest {
  private static final BrokerRequest DUMMY_BROKER_REQUEST =
      new Pql2Compiler().compileToBrokerRequest("SELECT * FROM myTable_OFFLINE");
  private static final Configuration DEFAULT_SCHEDULER_CONFIG = new PropertiesConfiguration();

  private ServerMetrics serverMetrics;
  private QueryScheduler queryScheduler;
  private QueryExecutor queryExecutor;
  private UnboundedResourceManager resourceManager;
  private LongAccumulator latestQueryTime;

  @BeforeClass
  public void setUp() {
    serverMetrics = new ServerMetrics(new MetricsRegistry());
    queryScheduler = mock(QueryScheduler.class);
    queryExecutor = new ServerQueryExecutorV1Impl();
    latestQueryTime = new LongAccumulator(Long::max, 0);
    resourceManager = new UnboundedResourceManager(new PropertiesConfiguration());
  }

  @Test
  public void testBadRequest()
      throws Exception {
    ScheduledRequestHandler handler = new ScheduledRequestHandler(queryScheduler, serverMetrics);
    String badRequest = "foobar";
    ListenableFuture<byte[]> response = handler.processRequest(badRequest.getBytes());
    // The handler method is expected to return immediately
    Assert.assertTrue(response.isDone());
    byte[] responseBytes = response.get();
    Assert.assertNull(responseBytes);
  }

  private InstanceRequest getInstanceRequest() {
    InstanceRequest request = new InstanceRequest();
    request.setRequestId(1);
    request.setQuery(DUMMY_BROKER_REQUEST);
    request.setSearchSegments(Arrays.asList("segment1", "segment2"));
    request.setEnableTrace(false);
    request.setBrokerId("broker");
    return request;
  }

  private byte[] getSerializedInstanceRequest(InstanceRequest request) {
    return new SerDe(new TCompactProtocol.Factory()).serialize(request);
  }

  @Test
  public void testQueryProcessingException()
      throws Exception {
    ScheduledRequestHandler handler = new ScheduledRequestHandler(
        new QueryScheduler(DEFAULT_SCHEDULER_CONFIG, queryExecutor, resourceManager, serverMetrics, latestQueryTime) {
          @Nonnull
          @Override
          public ListenableFuture<byte[]> submit(@Nonnull ServerQueryRequest queryRequest) {
            // The default version of Java 1.8 cannot recognize whether the submit method comes from ListeningExecutorService or Runnable.
            // Specifying it for less ambiguity.
            ListenableFuture<DataTable> dataTable = resourceManager.getQueryRunners().submit(new Callable<DataTable>() {
              @Override
              public DataTable call() {
                throw new RuntimeException("query processing error");
              }
            });
            ListenableFuture<DataTable> queryResponse = Futures.catching(dataTable, Throwable.class, input -> {
              DataTable result = new DataTableImplV2();
              result.addException(QueryException.INTERNAL_ERROR);
              return result;
            }, MoreExecutors.directExecutor());
            return serializeData(queryResponse);
          }

          @Override
          public void start() {

          }

          @Override
          public String name() {
            return "test";
          }
        }, serverMetrics);

    ListenableFuture<byte[]> responseFuture =
        handler.processRequest(getSerializedInstanceRequest(getInstanceRequest()));
    byte[] bytes = responseFuture.get(2, TimeUnit.SECONDS);
    // we get DataTable with exception information in case of query processing exception
    Assert.assertTrue(bytes.length > 0);
    DataTable expectedDT = new DataTableImplV2();
    expectedDT.addException(QueryException.INTERNAL_ERROR);
    Assert.assertEquals(bytes, expectedDT.toBytes());
  }

  @Test
  public void testValidQueryResponse()
      throws InterruptedException, ExecutionException, TimeoutException, IOException {
    ScheduledRequestHandler handler = new ScheduledRequestHandler(
        new QueryScheduler(DEFAULT_SCHEDULER_CONFIG, queryExecutor, resourceManager, serverMetrics, latestQueryTime) {
          @Nonnull
          @Override
          public ListenableFuture<byte[]> submit(@Nonnull ServerQueryRequest queryRequest) {
            ListenableFuture<DataTable> response = resourceManager.getQueryRunners().submit(() -> {
              String[] columnNames = new String[]{"foo", "bar"};
              DataSchema.ColumnDataType[] columnDataTypes =
                  new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT};
              DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
              DataTableBuilder dtBuilder = new DataTableBuilder(dataSchema);
              dtBuilder.startRow();
              dtBuilder.setColumn(0, "mars");
              dtBuilder.setColumn(1, 10);
              dtBuilder.finishRow();
              dtBuilder.startRow();
              dtBuilder.setColumn(0, "jupiter");
              dtBuilder.setColumn(1, 100);
              dtBuilder.finishRow();
              return dtBuilder.build();
            });
            return serializeData(response);
          }

          @Override
          public void start() {

          }

          @Override
          public String name() {
            return "test";
          }
        }, serverMetrics);

    ListenableFuture<byte[]> responseFuture =
        handler.processRequest(getSerializedInstanceRequest(getInstanceRequest()));
    byte[] responseBytes = responseFuture.get(2, TimeUnit.SECONDS);
    DataTable responseDT = DataTableFactory.getDataTable(responseBytes);
    Assert.assertEquals(responseDT.getNumberOfRows(), 2);
    Assert.assertEquals(responseDT.getString(0, 0), "mars");
    Assert.assertEquals(responseDT.getInt(0, 1), 10);
    Assert.assertEquals(responseDT.getString(1, 0), "jupiter");
    Assert.assertEquals(responseDT.getInt(1, 1), 100);
  }

  private ListenableFuture<byte[]> serializeData(ListenableFuture<DataTable> dataTable) {
    return Futures.transform(dataTable, input -> {
      try {
        Preconditions.checkNotNull(input);
        return input.toBytes();
      } catch (IOException e) {
        return new byte[0];
      }
    }, MoreExecutors.directExecutor());
  }
}
