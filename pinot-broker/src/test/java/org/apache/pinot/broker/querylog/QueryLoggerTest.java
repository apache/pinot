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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.broker.requesthandler.BaseSingleStageBrokerRequestHandler.ServerStats;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.trace.DefaultRequestContext;
import org.apache.pinot.spi.trace.QueryFingerprint;
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

    // Also mock the varargs version used by logQueryReceived
    Mockito.doAnswer(invocationOnMock -> {
      String format = invocationOnMock.getArgument(0);
      Object arg1 = invocationOnMock.getArgument(1);
      Object arg2 = invocationOnMock.getArgument(2);
      _infoLog.add(String.format(format.replace("{}", "%s"), arg1, arg2));
      return null;
    }).when(_logger).info(Mockito.anyString(), Mockito.anyLong(), Mockito.anyString());

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
    QueryLogger.QueryLogParams params = generateParams(false, false, 0, 456, null);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, true, _logger, _droppedRateLimiter);

    // When:
    queryLogger.logQueryCompleted(params, true);

    // Then:
    Assert.assertEquals(_infoLog.size(), 1);
    //@formatter:off
    Assert.assertEquals(_infoLog.get(0), "requestId=123,"
        + "table=table,"
        + "queryHash=,"
        + "timeMs=456,"
        + "docs=1/2,"
        + "entries=3/4,"
        + "segments(queried/processed/matched/consumingQueried/consumingProcessed/consumingMatched/unavailable)"
        + ":5/6/7/8/9/10/21,"
        + "consumingFreshnessTimeMs=11,"
        + "servers=12/13,"
        + "groupsTrimmed=false,"
        + "groupLimitReached=false,"
        + "groupWarningLimitReached=false,"
        + "brokerReduceTimeMs=20,"
        + "exceptions=0,"
        + "serverStats=serverStats,"
        + "offlineThreadCpuTimeNs(total/thread/sysActivity/resSer):45/14/15/16,"
        + "realtimeThreadCpuTimeNs(total/thread/sysActivity/resSer):54/17/18/19,"
        + "clientIp=ip,"
        + "queryEngine=singleStage,"
        + "offlineMemAllocatedBytes(total/thread/resSer):0/0/0,"
        + "realtimeMemAllocatedBytes(total/thread/resSer):0/0/0,"
        + "pools=[],"
        + "rlsFiltersApplied=true,"
        + "query=SELECT * FROM foo");
    //@formatter:on
  }

  @Test
  public void shouldOmitClientId() {
    // Given:
    QueryLogger.QueryLogParams params = generateParams(false, false, 0, 456, null);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, false, true, _logger, _droppedRateLimiter);

    // When:
    queryLogger.logQueryCompleted(params, true);

    // Then:
    Assert.assertEquals(_infoLog.size(), 1);
    Assert.assertFalse(_infoLog.get(0).contains("clientId"),
        "did not expect to see clientId Logs. Got: " + _infoLog.get(0));
  }

  @Test
  public void shouldNotLogCompletionWhenWasLoggedFalseAndNoForceLog() {
    // Given: wasLogged=false and no force-log conditions (no exceptions, not slow)
    QueryLogger.QueryLogParams params = generateParams(false, false, 0, 456, null);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, true, _logger, _droppedRateLimiter);

    // When:
    queryLogger.logQueryCompleted(params, false);

    // Then:
    Assert.assertEquals(_infoLog.size(), 0);
  }

  @Test
  public void shouldForceLogWhenNumGroupsLimitIsReached() {
    // Given: wasLogged=false but numGroupsLimitReached (force-log condition)
    QueryLogger.QueryLogParams params = generateParams(true, true, 0, 456, null);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, true, _logger, _droppedRateLimiter);

    // When:
    queryLogger.logQueryCompleted(params, false);

    // Then: should still log due to force-log condition
    Assert.assertEquals(_infoLog.size(), 1);
  }

  @Test
  public void shouldForceLogWhenExceptionsExist() {
    // Given: wasLogged=false but exceptions exist (force-log condition)
    QueryLogger.QueryLogParams params = generateParams(false, false, 1, 456, null);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, true, _logger, _droppedRateLimiter);

    // When:
    queryLogger.logQueryCompleted(params, false);

    // Then: should still log due to force-log condition
    Assert.assertEquals(_infoLog.size(), 1);
  }

  @Test
  public void shouldForceLogWhenTimeIsMoreThanOneSecond() {
    // Given: wasLogged=false but query took >1s (force-log condition)
    QueryLogger.QueryLogParams params = generateParams(false, false, 0, 1456, null);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, true, _logger, _droppedRateLimiter);

    // When:
    queryLogger.logQueryCompleted(params, false);

    // Then: should still log due to force-log condition
    Assert.assertEquals(_infoLog.size(), 1);
  }

  @Test
  public void shouldLogQueryReceivedWhenAllowed() {
    // Given: rate limiter allows
    Mockito.when(_logRateLimiter.tryAcquire()).thenReturn(true);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, true, _logger, _droppedRateLimiter);

    // When:
    boolean wasLogged = queryLogger.logQueryReceived(123L, "SELECT * FROM foo");

    // Then:
    Assert.assertTrue(wasLogged);
    Assert.assertEquals(_infoLog.size(), 1);
    Assert.assertTrue(_infoLog.get(0).contains("SQL query for request 123"));
  }

  @Test
  public void shouldNotLogQueryReceivedWhenRateLimited() {
    // Given: rate limiter denies
    Mockito.when(_logRateLimiter.tryAcquire()).thenReturn(false);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, true, _logger, _droppedRateLimiter);

    // When:
    boolean wasLogged = queryLogger.logQueryReceived(123L, "SELECT * FROM foo");

    // Then:
    Assert.assertFalse(wasLogged);
    Assert.assertEquals(_infoLog.size(), 0);
  }

  @Test
  public void shouldReturnTrueButNotLogWhenLogBeforeProcessingIsDisabled() {
    // Given: rate limiter allows, but logBeforeProcessing=false
    Mockito.when(_logRateLimiter.tryAcquire()).thenReturn(true);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, false, _logger, _droppedRateLimiter);

    // When:
    boolean wasLogged = queryLogger.logQueryReceived(123L, "SELECT * FROM foo");

    // Then: returns true because rate limiter allowed, but no log because logBeforeProcessing=false
    Assert.assertTrue(wasLogged);
    Assert.assertEquals(_infoLog.size(), 0);
  }

  @Test
  public void shouldLogCompletionWhenWasLoggedIsTrue() {
    // Given:
    QueryLogger.QueryLogParams params = generateParams(false, false, 0, 456);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, true, _logger, _droppedRateLimiter);

    // When:
    queryLogger.logQueryCompleted(params, true);

    // Then:
    Assert.assertEquals(_infoLog.size(), 1);
  }

  @Test(timeOut = 10_000L)
  public void shouldHandleRaceConditionsWithDroppedQueries()
      throws Exception {
    // Given:
    final CountDownLatch firstDroppedLogAttempted = new CountDownLatch(1);
    final CountDownLatch releaseFirstDroppedLogAttempt = new CountDownLatch(1);
    final AtomicInteger droppedRateLimiterCalls = new AtomicInteger();
    Mockito.when(_logRateLimiter.tryAcquire()).thenReturn(false)
        .thenReturn(true)
        .thenReturn(false)
        .thenReturn(true);

    Mockito.when(_droppedRateLimiter.tryAcquire()).thenAnswer(invocation -> {
      if (droppedRateLimiterCalls.getAndIncrement() == 0) {
        firstDroppedLogAttempted.countDown();
        Assert.assertTrue(releaseFirstDroppedLogAttempt.await(5, TimeUnit.SECONDS),
            "timed out waiting to release the first dropped-log attempt");
      }
      return true;
    });

    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, true, _logger, _droppedRateLimiter);

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    // When:
    try {
      Assert.assertFalse(queryLogger.logQueryReceived(123, "SELECT * FROM foo")); // 1 this one gets dropped

      // 2 this one succeeds, but blocks when it checks whether to log the dropped count
      Future<Boolean> blockedLogger =
          executorService.submit(() -> queryLogger.logQueryReceived(123, "SELECT * FROM foo"));
      Assert.assertTrue(firstDroppedLogAttempted.await(5, TimeUnit.SECONDS),
          "expected the first successful log to reach the dropped-log rate limiter");

      Assert.assertFalse(queryLogger.logQueryReceived(123, "SELECT * FROM foo")); // 3 this one gets dropped
      Assert.assertTrue(queryLogger.logQueryReceived(123, "SELECT * FROM foo")); // 4 this one drains the dropped count

      releaseFirstDroppedLogAttempt.countDown();
      Assert.assertTrue(blockedLogger.get(5, TimeUnit.SECONDS));
    } finally {
      releaseFirstDroppedLogAttempt.countDown();
      executorService.shutdown();
      Assert.assertTrue(executorService.awaitTermination(10, TimeUnit.SECONDS), "expected shutdown to complete");
    }

    // Then:
    Assert.assertEquals(_numDropped.size(), 1); // the second successful one never logs to warn
    Assert.assertEquals((long) _numDropped.get(0), 2L);
  }

  @Test
  public void shouldEmitQueryHashWhenSet() {
    // Given:
    QueryLogger.QueryLogParams params = generateParams(false, false, 0, 456,
      new QueryFingerprint("abc", "SELECT * FROM foo"));
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, true, _logger, _droppedRateLimiter);

    // When:
    queryLogger.logQueryCompleted(params, true);

    // Then:
    Assert.assertEquals(_infoLog.size(), 1);
    String logLine = _infoLog.get(0);
    Assert.assertTrue(logLine.contains("queryHash=abc"),
        "Expected queryHash in log. Got: " + logLine);
  }

  @Test
  public void shouldEmitEmptyQueryHashWhenNotSet() {
    // Given:
    QueryLogger.QueryLogParams params = generateParams(false, false, 0, 456, null);
    QueryLogger queryLogger = new QueryLogger(_logRateLimiter, 100, true, true, _logger, _droppedRateLimiter);

    // When:
    queryLogger.logQueryCompleted(params, true);

    // Then:
    Assert.assertEquals(_infoLog.size(), 1);
    String logLine = _infoLog.get(0);
    Assert.assertTrue(logLine.contains("queryHash=,"),
        "Expected empty queryHash field. Got: " + logLine);
  }

  private QueryLogger.QueryLogParams generateParams(boolean numGroupsLimitReached, boolean numGroupsWarningLimitReached,
      int numExceptions, long timeUsedMs) {
    return generateParams(numGroupsLimitReached, numGroupsWarningLimitReached, numExceptions, timeUsedMs, null);
  }

  private QueryLogger.QueryLogParams generateParams(boolean numGroupsLimitReached, boolean numGroupsWarningLimitReached,
      int numExceptions, long timeUsedMs, QueryFingerprint queryFingerprint) {
    RequestContext requestContext = new DefaultRequestContext();
    requestContext.setRequestId(123);
    requestContext.setQuery("SELECT * FROM foo");
    requestContext.setNumUnavailableSegments(21);

    if (queryFingerprint != null) {
      requestContext.setQueryFingerprint(queryFingerprint);
    }

    BrokerResponseNative response = new BrokerResponseNative();
    response.setNumGroupsLimitReached(numGroupsLimitReached);
    response.setNumGroupsWarningLimitReached(numGroupsWarningLimitReached);
    for (int i = 0; i < numExceptions; i++) {
      response.addException(new QueryProcessingException(QueryErrorCode.INTERNAL, "message" + i));
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
    response.setRLSFiltersApplied(true);

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
