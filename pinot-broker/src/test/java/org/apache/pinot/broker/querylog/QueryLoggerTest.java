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
package org.apache.pinot.broker.querylog;

import com.google.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.requesthandler.BaseSingleStageBrokerRequestHandler.ServerStats;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.spi.trace.DefaultRequestContext;
import org.apache.pinot.spi.trace.RequestContext;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.MockitoAnnotations.openMocks;


@SuppressWarnings("UnstableApiUsage")
public class QueryLoggerTest {

  @Mock
  RateLimiter _logRateLimiter;
  @Mock
  RateLimiter _droppedRateLimiter;
  @Mock
  Logger _logger;

  private final List<String> _infoLog = new ArrayList<>();
  private final List<Long> _numDropped = new ArrayList<>();

  private AutoCloseable _closeMocks;

  @BeforeMethod
  public void setUp() {
    _closeMocks = openMocks(this);

    _infoLog.clear();
    _numDropped.clear();

    Mockito.doAnswer(invocationOnMock -> {
      _infoLog.add(invocationOnMock.getArgument(0));
      return null;
    }).when(_logger).info(Mockito.anyString());

    Mockito.doAnswer(inv -> {
      _numDropped.add(inv.getArgument(1));
      return null;
    }).when(_logger).warn(Mockito.anyString(), Mockito.anyLong(), Mockito.anyDouble());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _closeMocks.close();
  }

  @Test
  public void shouldFormatLogLineProperly() {
    // Given:
    Mockito.when(_logRateLimiter.tryAcquire()).thenReturn(true);
    QueryLogger.QueryLogParams params = generateParams(false, 0, 456);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, _logger, _droppedRateLimiter);

    // When:
    queryLogger.log(params);

    // Then:
    Assert.assertEquals(_infoLog.size(), 1);
    //@formatter:off
    Assert.assertEquals(_infoLog.get(0), "requestId=123,"
        + "table=table,"
        + "timeMs=456,"
        + "docs=1/2,"
        + "entries=3/4,"
        + "segments(queried/processed/matched/consumingQueried/consumingProcessed/consumingMatched/unavailable)"
        + ":5/6/7/8/9/10/21,"
        + "consumingFreshnessTimeMs=11,"
        + "servers=12/13,"
        + "groupLimitReached=false,"
        + "brokerReduceTimeMs=20,"
        + "exceptions=0,"
        + "serverStats=serverStats,"
        + "offlineThreadCpuTimeNs(total/thread/sysActivity/resSer):45/14/15/16,"
        + "realtimeThreadCpuTimeNs(total/thread/sysActivity/resSer):54/17/18/19,"
        + "clientIp=ip,"
        + "queryEngine=singleStage,"
        + "query=SELECT * FROM foo");
    //@formatter:on
  }

  @Test
  public void shouldOmitClientId() {
    // Given:
    Mockito.when(_logRateLimiter.tryAcquire()).thenReturn(true);
    QueryLogger.QueryLogParams params = generateParams(false, 0, 456);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, false, _logger, _droppedRateLimiter);

    // When:
    queryLogger.log(params);

    // Then:
    Assert.assertEquals(_infoLog.size(), 1);
    Assert.assertFalse(_infoLog.get(0).contains("clientId"),
        "did not expect to see clientId Logs. Got: " + _infoLog.get(0));
  }

  @Test
  public void shouldNotForceLog() {
    // Given:
    Mockito.when(_logRateLimiter.tryAcquire()).thenReturn(false);
    QueryLogger.QueryLogParams params = generateParams(false, 0, 456);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, _logger, _droppedRateLimiter);

    // When:
    queryLogger.log(params);

    // Then:
    Assert.assertEquals(_infoLog.size(), 0);
  }

  @Test
  public void shouldForceLogWhenNumGroupsLimitIsReached() {
    // Given:
    Mockito.when(_logRateLimiter.tryAcquire()).thenReturn(false);
    QueryLogger.QueryLogParams params = generateParams(true, 0, 456);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, _logger, _droppedRateLimiter);

    // When:
    queryLogger.log(params);

    // Then:
    Assert.assertEquals(_infoLog.size(), 1);
  }

  @Test
  public void shouldForceLogWhenExceptionsExist() {
    // Given:
    Mockito.when(_logRateLimiter.tryAcquire()).thenReturn(false);
    QueryLogger.QueryLogParams params = generateParams(false, 1, 456);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, _logger, _droppedRateLimiter);

    // When:
    queryLogger.log(params);

    // Then:
    Assert.assertEquals(_infoLog.size(), 1);
  }

  @Test
  public void shouldForceLogWhenTimeIsMoreThanOneSecond() {
    // Given:
    Mockito.when(_logRateLimiter.tryAcquire()).thenReturn(false);
    QueryLogger.QueryLogParams params = generateParams(false, 0, 1456);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, _logger, _droppedRateLimiter);

    // When:
    queryLogger.log(params);

    // Then:
    Assert.assertEquals(_infoLog.size(), 1);
  }

  @Test(timeOut = 10_000L)
  public void shouldHandleRaceConditionsWithDroppedQueries()
      throws InterruptedException {
    // Given:
    final CountDownLatch logLatch = new CountDownLatch(1);
    Mockito.when(_logRateLimiter.tryAcquire()).thenReturn(false)
        .thenReturn(true)   // this one will block when it hits tryAcquire()
        .thenReturn(false)  // this one just increments the dropped logs
        .thenAnswer(invocation -> {
          // this one will unblock the tryAcquire, but only after
          // the first thread has reached _droppedRateLimiter#tryAcquire()
          logLatch.await();
          return true;
        });

    // ensure that the tryAcquire only succeeds after three other
    // logs have went through (see logAndDecrement)
    final CountDownLatch dropLogLatch = new CountDownLatch(3);
    Mockito.when(_droppedRateLimiter.tryAcquire()).thenAnswer(invocation -> {
      logLatch.countDown();
      dropLogLatch.await();
      return true;
    }).thenReturn(true);

    QueryLogger.QueryLogParams params = generateParams(false, 0, 456);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, _logger, _droppedRateLimiter);

    ExecutorService executorService = Executors.newFixedThreadPool(4);

    // When:
    try {
      // use logAndDecrement on all invocations since any one of them could
      // be the one that blocks
      Runnable logAndDecrement = () -> {
        queryLogger.log(params);
        dropLogLatch.countDown();
      };

      executorService.submit(logAndDecrement); // 1 this one gets dropped
      executorService.submit(logAndDecrement); // 2 this one succeeds, but blocks
      executorService.submit(logAndDecrement); // 3 this one gets dropped
      executorService.submit(logAndDecrement); // 4 this one succeeds, and unblocks (2)
    } finally {
      executorService.shutdown();
      Assert.assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS), "expected shutdown to complete");
    }

    // Then:
    Assert.assertEquals(_numDropped.size(), 1); // the second successful one never logs to warn
    Assert.assertEquals((long) _numDropped.get(0), 2L);
  }

  private QueryLogger.QueryLogParams generateParams(boolean numGroupsLimitReached, int numExceptions, long timeUsedMs) {
    RequestContext requestContext = new DefaultRequestContext();
    requestContext.setRequestId(123);
    requestContext.setQuery("SELECT * FROM foo");
    requestContext.setNumUnavailableSegments(21);

    BrokerResponseNative response = new BrokerResponseNative();
    response.setNumGroupsLimitReached(numGroupsLimitReached);
    for (int i = 0; i < numExceptions; i++) {
      response.addException(new ProcessingException());
    }
    response.setTimeUsedMs(timeUsedMs);
    response.setNumDocsScanned(1);
    response.setTotalDocs(2);
    response.setNumEntriesScannedInFilter(3);
    response.setNumEntriesScannedPostFilter(4);
    response.setNumSegmentsQueried(5);
    response.setNumSegmentsProcessed(6);
    response.setNumSegmentsMatched(7);
    response.setNumConsumingSegmentsQueried(8);
    response.setNumConsumingSegmentsProcessed(9);
    response.setNumConsumingSegmentsMatched(10);
    response.setMinConsumingFreshnessTimeMs(11);
    response.setNumServersResponded(12);
    response.setNumServersQueried(13);
    response.setOfflineThreadCpuTimeNs(14);
    response.setOfflineSystemActivitiesCpuTimeNs(15);
    response.setOfflineResponseSerializationCpuTimeNs(16);
    response.setRealtimeThreadCpuTimeNs(17);
    response.setRealtimeSystemActivitiesCpuTimeNs(18);
    response.setRealtimeResponseSerializationCpuTimeNs(19);
    response.setBrokerReduceTimeMs(20);

    RequesterIdentity identity = new RequesterIdentity() {
      @Override
      public String getClientIp() {
        return "ip";
      }
    };

    ServerStats serverStats = new ServerStats();
    serverStats.setServerStats("serverStats");

    return new QueryLogger.QueryLogParams(requestContext, "table", response,
        QueryLogger.QueryLogParams.QueryEngine.SINGLE_STAGE, identity, serverStats);
  }
}
