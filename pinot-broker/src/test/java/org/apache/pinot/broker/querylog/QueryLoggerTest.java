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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.requesthandler.BaseBrokerRequestHandler;
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
  public void tearDown() throws Exception {
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
    Assert.assertEquals(_infoLog.get(0), "requestId=123,"
        + "table=table,"
        + "timeMs=456,"
        + "docs=1/2,"
        + "entries=3/4,"
        + "segments(queried/processed/matched/consumingQueried/consumingProcessed/consumingMatched/unavailable)"
        + ":5/6/7/8/9/10/24,"
        + "consumingFreshnessTimeMs=11,"
        + "servers=12/13,"
        + "groupLimitReached=false,"
        + "brokerReduceTimeMs=22,"
        + "exceptions=0,"
        + "serverStats=serverStats,"
        + "offlineThreadCpuTimeNs(total/thread/sysActivity/resSer):14/15/16/17,"
        + "realtimeThreadCpuTimeNs(total/thread/sysActivity/resSer):18/19/20/21,"
        + "clientIp=ip,"
        + "query=SELECT * FROM foo");
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
    Assert.assertFalse(
        _infoLog.get(0).contains("clientId"),
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
    // first and third request get dropped
    final CountDownLatch dropLogLatch = new CountDownLatch(1);
    final CountDownLatch logLatch = new CountDownLatch(1);
    Mockito.when(_logRateLimiter.tryAcquire())
        .thenReturn(false)
        .thenReturn(true)   // this one will block when it hits tryAcquire()
        .thenReturn(false)  // this one just increments the dropped logs
        .thenAnswer(invocation -> {
          // this one will unblock the tryAcquire, but only after
          // the first thread has reached _droppedRateLimiter#tryAcquire()
          logLatch.await();
          dropLogLatch.countDown();
          return true;
        });

    Mockito.when(_droppedRateLimiter.tryAcquire())
        .thenAnswer(invocation -> {
          logLatch.countDown();
          dropLogLatch.await();
          return true;
        }).thenReturn(true);

    QueryLogger.QueryLogParams params = generateParams(false, 0, 456);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, _logger, _droppedRateLimiter);

    ExecutorService executorService = Executors.newFixedThreadPool(4);

    // When:
    try {
      executorService.submit(() -> queryLogger.log(params)); // this one gets dropped
      executorService.submit(() -> queryLogger.log(params)); // this one succeeds, but blocks
      executorService.submit(() -> queryLogger.log(params)); // this one gets dropped
      executorService.submit(() -> queryLogger.log(params)); // this one succeeds, and unblocks (2)
    } finally {
      executorService.shutdown();
      Assert.assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS), "expected shutdown to complete");
    }

    // Then:
    Assert.assertEquals(_numDropped.size(), 1); // the second successful one never logs to warn
    Assert.assertEquals((long) _numDropped.get(0), 2L);
  }

  private QueryLogger.QueryLogParams generateParams(boolean isGroupLimitHit, int numExceptions, long timeUsed) {
    BrokerResponseNative response = new BrokerResponseNative();
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
    response.setNumGroupsLimitReached(isGroupLimitHit);
    response.setExceptions(
        IntStream.range(0, numExceptions)
            .mapToObj(i -> new ProcessingException()).collect(Collectors.toList()));
    response.setOfflineTotalCpuTimeNs(14);
    response.setOfflineThreadCpuTimeNs(15);
    response.setOfflineSystemActivitiesCpuTimeNs(16);
    response.setOfflineResponseSerializationCpuTimeNs(17);
    response.setRealtimeTotalCpuTimeNs(18);
    response.setRealtimeThreadCpuTimeNs(19);
    response.setRealtimeSystemActivitiesCpuTimeNs(20);
    response.setRealtimeResponseSerializationCpuTimeNs(21);

    RequestContext request = new DefaultRequestContext();
    request.setReduceTimeMillis(22);

    BaseBrokerRequestHandler.ServerStats serverStats = new BaseBrokerRequestHandler.ServerStats();
    serverStats.setServerStats("serverStats");
    RequesterIdentity identity = new RequesterIdentity() {
      @Override public String getClientIp() {
        return "ip";
      }
    };

    return new QueryLogger.QueryLogParams(
        123,
        "SELECT * FROM foo",
        request,
        "table",
        24,
        serverStats,
        response,
        timeUsed,
        identity
    );
  }
}
