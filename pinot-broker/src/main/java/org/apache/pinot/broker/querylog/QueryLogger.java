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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.broker.requesthandler.BaseBrokerRequestHandler;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Broker;


/**
 * {@code QueryLogger} is responsible for logging query responses in a configurable
 * fashion. Query logging can be useful to capture production traffic to assist with
 * debugging or regression testing.
 */
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@SuppressWarnings("UnstableApiUsage")
public class QueryLogger {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryLogger.class);

  private final RateLimiter _logRateLimiter;
  @Getter
  private final int _maxQueryLengthToLog;
  private final boolean _enableIpLogging;
  private final Logger _logger;
  private final RateLimiter _droppedLogRateLimiter;
  private final AtomicLong _numDroppedLogs = new AtomicLong(0L);

  public QueryLogger(PinotConfiguration config) {
    this(RateLimiter.create(config.getProperty(Broker.CONFIG_OF_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND,
            Broker.DEFAULT_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND)),
        config.getProperty(Broker.CONFIG_OF_BROKER_QUERY_LOG_LENGTH, Broker.DEFAULT_BROKER_QUERY_LOG_LENGTH),
        config.getProperty(Broker.CONFIG_OF_BROKER_REQUEST_CLIENT_IP_LOGGING,
            Broker.DEFAULT_BROKER_REQUEST_CLIENT_IP_LOGGING), LOGGER, RateLimiter.create(1)
        // log once a second for dropped log count
    );
  }

  public void log(QueryLogParams params) {
    _logger.debug("Broker Response: {}", params._response);

    if (!(_logRateLimiter.tryAcquire() || shouldForceLog(params))) {
      _numDroppedLogs.incrementAndGet();
      return;
    }

    final StringBuilder queryLogBuilder = new StringBuilder();
    for (QueryLogEntry value : QueryLogEntry.values()) {
      value.format(queryLogBuilder, this, params);
      queryLogBuilder.append(',');
    }

    // always log the query last - don't add this to the QueryLogEntry enum
    queryLogBuilder.append("query=").append(StringUtils.substring(params._query, 0, _maxQueryLengthToLog));
    _logger.info(queryLogBuilder.toString());

    if (_droppedLogRateLimiter.tryAcquire()) {
      // use getAndSet to 0 so that there will be no race condition between
      // loggers that increment this counter and this thread
      long numDroppedLogsSinceLastLog = _numDroppedLogs.getAndSet(0);
      if (numDroppedLogsSinceLastLog > 0) {
        _logger.warn("{} logs were dropped. (log max rate per second: {})", numDroppedLogsSinceLastLog,
            _droppedLogRateLimiter.getRate());
      }
    }
  }

  public double getLogRateLimit() {
    return _logRateLimiter.getRate();
  }

  private boolean shouldForceLog(QueryLogParams params) {
    return params._response.isNumGroupsLimitReached() || params._response.getExceptionsSize() > 0
        || params._timeUsedMs > TimeUnit.SECONDS.toMillis(1);
  }

  public static class QueryLogParams {
    final long _requestId;
    final String _query;
    final RequestContext _requestContext;
    final String _table;
    final int _numUnavailableSegments;
    @Nullable
    final BaseBrokerRequestHandler.ServerStats _serverStats;
    final BrokerResponse _response;
    final long _timeUsedMs;
    @Nullable
    final RequesterIdentity _requester;

    public QueryLogParams(long requestId, String query, RequestContext requestContext, String table,
        int numUnavailableSegments, @Nullable BaseBrokerRequestHandler.ServerStats serverStats, BrokerResponse response,
        long timeUsedMs, @Nullable RequesterIdentity requester) {
      _requestId = requestId;
      _query = query;
      _table = table;
      _timeUsedMs = timeUsedMs;
      _requestContext = requestContext;
      _requester = requester;
      _response = response;
      _serverStats = serverStats;
      _numUnavailableSegments = numUnavailableSegments;
    }
  }

  /**
   * NOTE: please maintain the order of this query log entry enum. If you want to add a new
   * entry, add it to the end of the existing list.
   */
  private enum QueryLogEntry {
    REQUEST_ID("requestId") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._requestId);
      }
    },
    TABLE("table") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._table);
      }
    },
    TIME_MS("timeMs") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._timeUsedMs);
      }
    },
    DOCS("docs") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getNumDocsScanned()).append('/').append(params._response.getTotalDocs());
      }
    },
    ENTRIES("entries") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getNumEntriesScannedInFilter()).append('/')
            .append(params._response.getNumEntriesScannedPostFilter());
      }
    },
    SEGMENT_INFO("segments(queried/processed/matched/consumingQueried/consumingProcessed/consumingMatched/unavailable)",
        ':') {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getNumSegmentsQueried()).append('/')
            .append(params._response.getNumSegmentsProcessed()).append('/')
            .append(params._response.getNumSegmentsMatched()).append('/')
            .append(params._response.getNumConsumingSegmentsQueried()).append('/')
            .append(params._response.getNumConsumingSegmentsProcessed()).append('/')
            .append(params._response.getNumConsumingSegmentsMatched()).append('/')
            .append(params._numUnavailableSegments);
      }
    },
    CONSUMING_FRESHNESS_MS("consumingFreshnessTimeMs") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getMinConsumingFreshnessTimeMs());
      }
    },
    SERVERS("servers") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getNumServersResponded()).append('/')
            .append(params._response.getNumServersQueried());
      }
    },
    GROUP_LIMIT_REACHED("groupLimitReached") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.isNumGroupsLimitReached());
      }
    },
    BROKER_REDUCE_TIME_MS("brokerReduceTimeMs") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._requestContext.getReduceTimeMillis());
      }
    },
    EXCEPTIONS("exceptions") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getExceptionsSize());
      }
    },
    SERVER_STATS("serverStats") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        if (params._serverStats != null) {
          builder.append(params._serverStats.getServerStats());
        } else {
          builder.append(CommonConstants.UNKNOWN);
        }
      }
    },
    OFFLINE_THREAD_CPU_TIME("offlineThreadCpuTimeNs(total/thread/sysActivity/resSer)", ':') {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getOfflineTotalCpuTimeNs()).append('/')
            .append(params._response.getOfflineThreadCpuTimeNs()).append('/')
            .append(params._response.getOfflineSystemActivitiesCpuTimeNs()).append('/')
            .append(params._response.getOfflineResponseSerializationCpuTimeNs());
      }
    },
    REALTIME_THREAD_CPU_TIME("realtimeThreadCpuTimeNs(total/thread/sysActivity/resSer)", ':') {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        builder.append(params._response.getRealtimeTotalCpuTimeNs()).append('/')
            .append(params._response.getRealtimeThreadCpuTimeNs()).append('/')
            .append(params._response.getRealtimeSystemActivitiesCpuTimeNs()).append('/')
            .append(params._response.getRealtimeResponseSerializationCpuTimeNs());
      }
    },
    CLIENT_IP("clientIp") {
      @Override
      void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
        if (logger._enableIpLogging && params._requester != null) {
          builder.append(params._requester.getClientIp());
        } else {
          builder.append(CommonConstants.UNKNOWN);
        }
      }
    };

    public final String _entryName;
    public final char _separator; // backwards compatibility for the entries that use ':' instead of '='

    QueryLogEntry(String entryName) {
      this(entryName, '=');
    }

    QueryLogEntry(String entryName, final char separator) {
      _entryName = entryName;
      _separator = separator;
    }

    abstract void doFormat(StringBuilder builder, QueryLogger logger, QueryLogParams params);

    void format(StringBuilder builder, QueryLogger logger, QueryLogParams params) {
      // use StringBuilder because the compiler will struggle to turn string complicated
      // (as part of a loop) string concatenation into StringBuilder, which is significantly
      // more efficient
      builder.append(_entryName).append(_separator);
      doFormat(builder, logger, params);
    }
  }
}
