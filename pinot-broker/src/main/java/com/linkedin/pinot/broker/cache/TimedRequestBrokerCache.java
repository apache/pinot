/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.cache;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.metrics.BrokerQueryPhase;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import com.linkedin.pinot.requestHandler.BrokerRequestHandler;

public class TimedRequestBrokerCache implements BrokerCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimedRequestBrokerCache.class);

  private static final Pql2Compiler REQUEST_COMPILER = new Pql2Compiler();

  public static String EXPIRE_TIME_VALUE = "expireTimeValue";
  public static String EXPIRE_TIME_UNIT = "expireTimeUnit";
  public static String BUFFER_SIZE = "bufferSize";

  private CacheLoader<String, BrokerResponse> loader;
  private LoadingCache<String, BrokerResponse> cache;
  private BrokerRequestHandler requestHandler;
  private BrokerMetrics brokerMetrics;
  private int bufferSize;
  private long expireTimeValue;
  private TimeUnit expireTimeUnit;
  private static ScheduledExecutorService scheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor();

  public TimedRequestBrokerCache() {
    LOGGER.info("Called constructor for TimedRequestBrokerCache!");
  }

  public void init(BrokerRequestHandler requestHandler, BrokerCacheConfigs brokerCacheConfigs,
      BrokerMetrics brokerMetrics) {
    this.requestHandler = requestHandler;
    this.brokerMetrics = brokerMetrics;
    this.loader = new ResultCacheLoader(this.requestHandler, this.brokerMetrics);
    Map<String, String> brokerCacheClassConfigs =
        brokerCacheConfigs.getConfigsFor(this.getClass().getSimpleName());
    LOGGER.info("Trying to init TimedRequestBrokerCache with configs:\n{}",
        Arrays.asList(brokerCacheConfigs));
    this.expireTimeValue = Long.valueOf(brokerCacheClassConfigs.get(EXPIRE_TIME_VALUE));
    this.expireTimeUnit = TimeUnit.valueOf(brokerCacheClassConfigs.get(EXPIRE_TIME_UNIT));
    this.bufferSize = Integer.valueOf(brokerCacheClassConfigs.get(BUFFER_SIZE));
    this.cache = CacheBuilder.newBuilder()
        .initialCapacity(bufferSize)
        .maximumSize(bufferSize)
        .expireAfterWrite(expireTimeValue, expireTimeUnit)
        .build(loader);
    scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        cache.cleanUp();
      }
    }, expireTimeValue, expireTimeValue, expireTimeUnit);
    LOGGER.info("Initialized TimedRequestBrokerCache with expire time {} {} and buffer size {}",
        this.expireTimeValue, this.expireTimeUnit, this.bufferSize);
  }

  public BrokerResponse handleRequest(JSONObject brokerRequestJson) throws Exception {
    long startTime = System.nanoTime();
    String brokerRequestString = brokerRequestJson.toString();
    BrokerResponse brokerResponse = this.cache.getIfPresent(brokerRequestString);
    if (brokerResponse == null) {
      brokerResponse = this.cache.get(brokerRequestString);
    } else {
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.CACHE_HIT, 1);
      long timeUsedNs = System.nanoTime() - startTime;
      long timeUsedMs = timeUsedNs / 1000000;
      brokerResponse.setTimeUsedMs(timeUsedMs);
      try {
        BrokerRequest brokerRequest =
            REQUEST_COMPILER.compileToBrokerRequest(brokerRequestJson.getString("pql"));
        brokerMetrics.addPhaseTiming(brokerRequest, BrokerQueryPhase.QUERY_EXECUTION, timeUsedNs);
        brokerMetrics.addMeteredQueryValue(brokerRequest, BrokerMeter.QUERIES, 1);
      } catch (Exception e) {
        // Swallow exceptions.
      }
      LOGGER.info("Request: {}, hitting broker cache, return in {} ms!", brokerRequestString,
          timeUsedMs);
    }
    return brokerResponse;
  }

  /**
   * Unless specifically specified by config.userClientExecute(), default behavior will leverage
   * internal caches to retrieve results.
   */
  private class ResultCacheLoader extends CacheLoader<String, BrokerResponse> {

    private final BrokerRequestHandler requestHandler;
    private final BrokerMetrics brokerMetrics;

    public ResultCacheLoader(BrokerRequestHandler requestHandler, BrokerMetrics brokerMetrics) {
      this.requestHandler = requestHandler;
      this.brokerMetrics = brokerMetrics;
    }

    @Override
    public BrokerResponse load(String brokerRequestString) throws Exception {
      brokerMetrics.addMeteredGlobalValue(BrokerMeter.CACHE_MISSED, 1);
      JSONObject brokerRequest = new JSONObject(brokerRequestString);
      return this.requestHandler.handleRequest(brokerRequest);
    }
  }
}
