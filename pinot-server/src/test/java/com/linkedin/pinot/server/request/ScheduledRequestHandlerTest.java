/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.server.request;

import com.google.common.util.concurrent.ListenableFuture;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder;
import com.linkedin.pinot.core.query.scheduler.QueryScheduler;
import com.linkedin.pinot.serde.SerDe;
import com.yammer.metrics.core.MetricsRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class ScheduledRequestHandlerTest {

  public static Logger LOGGER = LoggerFactory.getLogger(ScheduledRequestHandlerTest.class);

  private ServerMetrics serverMetrics;
  private ChannelHandlerContext channelHandlerContext;
  private QueryScheduler queryScheduler;

  @BeforeTest
  public void setupTestMethod() {
    serverMetrics = new ServerMetrics(new MetricsRegistry());
    channelHandlerContext = mock(ChannelHandlerContext.class, RETURNS_DEEP_STUBS);
    when(channelHandlerContext.channel().remoteAddress())
        .thenAnswer(new Answer<InetSocketAddress>() {
          @Override
          public InetSocketAddress answer(InvocationOnMock invocationOnMock)
              throws Throwable {
            return new InetSocketAddress("localhost", 60000);
          }
        });

    queryScheduler = mock(QueryScheduler.class);
  }

  @Test
  public void testBadRequest()
      throws Exception {
    ScheduledRequestHandler handler = new ScheduledRequestHandler(queryScheduler, serverMetrics);
    String requestBadString = "foobar";
    byte[] requestData = requestBadString.getBytes();
    ByteBuf buffer = Unpooled.wrappedBuffer(requestData);
    ListenableFuture<byte[]> response = handler.processRequest(channelHandlerContext, buffer);
    // The handler method is expected to return immediately
    Assert.assertTrue(response.isDone());
    byte[] responseBytes = response.get();
    Assert.assertTrue(responseBytes.length > 0);
    DataTable expectedDT = new DataTable();
    expectedDT.addException(QueryException.INTERNAL_ERROR);
    Assert.assertEquals(responseBytes, expectedDT.toBytes());
  }

  private InstanceRequest getInstanceRequest() {
    InstanceRequest request = new InstanceRequest();
    request.setBrokerId("broker");
    request.setEnableTrace(false);
    request.setRequestId(1);
    request.setSearchSegments(Arrays.asList("segment1", "segment2"));
    request.setQuery(new BrokerRequest());
    return request;
  }

  private ByteBuf getSerializedInstanceRequest(InstanceRequest request) {
    SerDe serDe = new SerDe(new TCompactProtocol.Factory());
    byte[] requestData = serDe.serialize(request);
    return Unpooled.wrappedBuffer(requestData);
  }

  @Test
  public void testQueryProcessingException()
      throws Exception {
    ScheduledRequestHandler handler = new ScheduledRequestHandler(new QueryScheduler(null) {
      @Override
      public ListenableFuture<DataTable> submit(QueryRequest queryRequest) {
        return queryWorkers.submit(new Callable<DataTable>() {
          @Override
          public DataTable call()
              throws Exception {
            throw new RuntimeException("query processing error");
          }
        });
      }
    }, serverMetrics);

    ByteBuf requestBuf = getSerializedInstanceRequest(getInstanceRequest());
    ListenableFuture<byte[]> responseFuture = handler.processRequest(channelHandlerContext, requestBuf);
    byte[] bytes = responseFuture.get(2, TimeUnit.SECONDS);
    // we get DataTable with exception information in case of query processing exception
    Assert.assertTrue(bytes.length > 0);
    DataTable expectedDT = new DataTable();
    expectedDT.addException(QueryException.INTERNAL_ERROR);
    Assert.assertEquals(bytes, expectedDT.toBytes());
  }

  @Test
  public void testValidQueryResponse()
      throws InterruptedException, ExecutionException, TimeoutException {
    ScheduledRequestHandler handler = new ScheduledRequestHandler(new QueryScheduler(null) {
      @Override
      public ListenableFuture<DataTable> submit(QueryRequest queryRequest) {
        return queryRunners.submit(new Callable<DataTable>() {
          @Override
          public DataTable call()
              throws Exception {
            String[] columns = new String[] { "foo", "bar"};
            FieldSpec.DataType[] columnTypes = new FieldSpec.DataType[] {
              FieldSpec.DataType.STRING,
              FieldSpec.DataType.INT
            };
            DataTableBuilder.DataSchema dataSchema = new DataTableBuilder.DataSchema(
                columns, columnTypes);
            DataTableBuilder dtBuilder = new DataTableBuilder(dataSchema);
            dtBuilder.open();
            dtBuilder.startRow();
            dtBuilder.setColumn(0, "mars");
            dtBuilder.setColumn(1, 10);
            dtBuilder.finishRow();
            dtBuilder.startRow();
            dtBuilder.setColumn(0, "jupiter");
            dtBuilder.setColumn(1, 100);
            dtBuilder.finishRow();
            dtBuilder.seal();
            return dtBuilder.build();
          }
        });
      }
    }, serverMetrics);

    ByteBuf requestBuf = getSerializedInstanceRequest(getInstanceRequest());
    ListenableFuture<byte[]> responseFuture = handler.processRequest(channelHandlerContext, requestBuf);
    byte[] responseBytes = responseFuture.get(2, TimeUnit.SECONDS);
    DataTable responseDT = new DataTable(responseBytes);
    Assert.assertEquals(responseDT.getNumberOfRows(), 2);
    Assert.assertEquals(responseDT.getString(0, 0), "mars");
    Assert.assertEquals(responseDT.getInt(0, 1), 10);
    Assert.assertEquals(responseDT.getString(1, 0), "jupiter");
    Assert.assertEquals(responseDT.getInt(1, 1), 100);
  }

}
