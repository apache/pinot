/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.queryquota;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.linkedin.pinot.common.config.QuotaConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metrics.BrokerGauge;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableQueryQuotaManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableQueryQuotaManager.class);

  private BrokerMetrics _brokerMetrics;
  private final HelixManager _helixManager;
  private final Map<String, QueryQuotaConfig> _rateLimiterMap;
  private static int TIME_RANGE_IN_SECOND = 1;

  public TableQueryQuotaManager(HelixManager helixManager) {
    _helixManager = helixManager;
    _rateLimiterMap  = new ConcurrentHashMap<>();
  }

  /**
   * Initialize dynamic rate limiter with table query quota.
   * @param tableConfig table config.
   * @param brokerResource broker resource which stores all the broker states of each table.
   */
  public void initTableQueryQuota(TableConfig tableConfig, ExternalView brokerResource) {
    String tableName = tableConfig.getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    LOGGER.info("Initializing rate limiter for table {}", rawTableName);

    // Check whether qps quotas from both tables are the same.
    QuotaConfig offlineQuotaConfig;
    QuotaConfig realtimeQuotaConfig;
    CommonConstants.Helix.TableType tableType = tableConfig.getTableType();
    if (tableType == CommonConstants.Helix.TableType.OFFLINE) {
      offlineQuotaConfig = tableConfig.getQuotaConfig();
      realtimeQuotaConfig = getQuotaConfigFromPropertyStore(rawTableName, CommonConstants.Helix.TableType.REALTIME);
    } else {
      realtimeQuotaConfig = tableConfig.getQuotaConfig();
      offlineQuotaConfig = getQuotaConfigFromPropertyStore(rawTableName, CommonConstants.Helix.TableType.OFFLINE);
    }
    // Log a warning if MaxQueriesPerSecond are set different.
    if ((offlineQuotaConfig != null && !Strings.isNullOrEmpty(offlineQuotaConfig.getMaxQueriesPerSecond()))
        && (realtimeQuotaConfig != null && !Strings.isNullOrEmpty(realtimeQuotaConfig.getMaxQueriesPerSecond()))) {
      if (!offlineQuotaConfig.getMaxQueriesPerSecond().equals(realtimeQuotaConfig.getMaxQueriesPerSecond())) {
        LOGGER.warn("Attention! The values of MaxQueriesPerSecond for table {} are set different! Offline table qps quota: {}, Real-time table qps quota: {}",
            rawTableName, offlineQuotaConfig.getMaxQueriesPerSecond(), realtimeQuotaConfig.getMaxQueriesPerSecond());
      }
    }

    // Create rate limiter
    createRateLimiter(tableName, brokerResource, tableConfig.getQuotaConfig());
  }


  /**
   * Drop table query quota.
   * @param tableName table name with type.
   */
  public void dropTableQueryQuota(String tableName) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    LOGGER.info("Dropping rate limiter for table {}", rawTableName);
    removeRateLimiter(tableName);
  }

  /** Remove or update rate limiter if another table with the same raw table name but different type is still using the quota config.
   * @param tableName original table name
   */
  private void removeRateLimiter(String tableName) {
    _rateLimiterMap.remove(tableName);
  }

  /**
   * Get QuotaConfig from property store.
   * @param rawTableName table name without table type.
   * @param tableType table type: offline or real-time.
   * @return QuotaConfig, which could be null.
   */
  private QuotaConfig getQuotaConfigFromPropertyStore(String rawTableName, CommonConstants.Helix.TableType tableType) {
    ZkHelixPropertyStore<ZNRecord> propertyStore = _helixManager.getHelixPropertyStore();

    String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(rawTableName);
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(propertyStore, tableNameWithType);
    if (tableConfig == null) {
      return null;
    }
    return tableConfig.getQuotaConfig();
  }

  /**
   * Create a rate limiter for a table.
   * @param tableName table name with table type.
   * @param brokerResource broker resource which stores all the broker states of each table.
   * @param quotaConfig quota config of the table.
   */
  private void createRateLimiter(String tableName, ExternalView brokerResource, QuotaConfig quotaConfig) {
    if (quotaConfig == null || Strings.isNullOrEmpty(quotaConfig.getMaxQueriesPerSecond())) {
      LOGGER.info("No qps config specified for table: {}", tableName);
      return;
    }

    if (brokerResource == null) {
      LOGGER.warn("Failed to init qps quota for table {}. No broker resource connected!", tableName);
      return;
    }

    Map<String, String> stateMap = brokerResource.getStateMap(tableName);
    int otherOnlineBrokerCount = 0;

    // If stateMap is null, that means this broker is the first broker for this table.
    if (stateMap != null) {
      for (Map.Entry<String, String> state : stateMap.entrySet()) {
        if (!_helixManager.getInstanceName().equals(state.getKey())
            && state.getValue().equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)) {
          otherOnlineBrokerCount++;
        }
      }
    }
    LOGGER.info("The number of online brokers for table {} is {}", tableName, otherOnlineBrokerCount + 1);
    //int onlineCount = otherOnlineBrokerCount + 1;

    // FIXME We use fixed rate for the 1st version.
    int onlineCount = 1;

    // Get the dynamic rate
    double overallRate;
    if (quotaConfig.isMaxQueriesPerSecondValid()) {
      overallRate = Double.parseDouble(quotaConfig.getMaxQueriesPerSecond());
    } else {
      LOGGER.error("Failed to init qps quota: error when parsing qps quota: {} for table: {}",
          quotaConfig.getMaxQueriesPerSecond(), tableName);
      return;
    }

    double perBrokerRate =  overallRate / onlineCount;
    QueryQuotaConfig queryQuotaConfig = new QueryQuotaConfig(RateLimiter.create(perBrokerRate), new HitCounter(TIME_RANGE_IN_SECOND));
    _rateLimiterMap.put(tableName, queryQuotaConfig);
    LOGGER.info("Rate limiter for table: {} has been initialized. Overall rate: {}. Per-broker rate: {}. Number of online broker instances: {}",
        tableName, overallRate, perBrokerRate, onlineCount);
  }

  /**
   * Acquire a token from rate limiter based on the table name.
   * @param tableName original table name which could be raw.
   * @return true if there is no query quota specified for the table or a token can be acquired, otherwise return false.
   */
  public boolean acquire(String tableName) {
    LOGGER.debug("Trying to acquire token for table: {}", tableName);
    String offlineTableName = null;
    String realtimeTableName = null;
    QueryQuotaConfig offlineTableQueryQuotaConfig = null;
    QueryQuotaConfig realtimeTableQueryQuotaConfig = null;

    CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == CommonConstants.Helix.TableType.OFFLINE) {
      offlineTableName = tableName;
      offlineTableQueryQuotaConfig = _rateLimiterMap.get(tableName);
    } else if (tableType == CommonConstants.Helix.TableType.REALTIME) {
      realtimeTableName = tableName;
      realtimeTableQueryQuotaConfig = _rateLimiterMap.get(tableName);
    } else {
      offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      offlineTableQueryQuotaConfig = _rateLimiterMap.get(offlineTableName);
      realtimeTableQueryQuotaConfig = _rateLimiterMap.get(realtimeTableName);
    }

    boolean offlineQuotaOk = offlineTableQueryQuotaConfig == null || tryAcquireToken(offlineTableName, offlineTableQueryQuotaConfig);
    boolean realtimeQuotaOk = realtimeTableQueryQuotaConfig == null || tryAcquireToken(realtimeTableName, realtimeTableQueryQuotaConfig);

    return offlineQuotaOk && realtimeQuotaOk;
  }

  /**
   * Try to acquire token from rate limiter. Emit the utilization of the qps quota if broker metric isn't null.
   * @param tableNameWithType table name with type.
   * @param queryQuotaConfig query quota config for type-specific table.
   * @return true if there's no qps quota for that table, or a token is acquired successfully.
   */
  private boolean tryAcquireToken(String tableNameWithType, QueryQuotaConfig queryQuotaConfig) {
    // Use hit counter to count the number of hits.
    queryQuotaConfig.getHitCounter().hit();

    RateLimiter rateLimiter = queryQuotaConfig.getRateLimiter();
    double perBrokerRate = rateLimiter.getRate();

    // Emit the qps capacity utilization rate.
    int numHits = queryQuotaConfig.getHitCounter().getHitCount();
    if (_brokerMetrics != null) {
      int percentageOfCapacityUtilization = (int)(numHits * 100 / perBrokerRate);
      LOGGER.debug("The percentage of rate limit capacity utilization is {}", percentageOfCapacityUtilization);
      _brokerMetrics.setValueOfTableGauge(tableNameWithType, BrokerGauge.QUERY_QUOTA_CAPACITY_UTILIZATION_RATE, percentageOfCapacityUtilization);
    }

    if (!rateLimiter.tryAcquire()) {
      LOGGER.info("Quota is exceeded for table: {}. Per-broker rate: {}. Current qps: {}", tableNameWithType, perBrokerRate, numHits);
      return false;
    }
    // Token is successfully acquired.
    return true;
  }

  public void setBrokerMetrics(BrokerMetrics brokerMetrics) {
    _brokerMetrics = brokerMetrics;
  }

  @VisibleForTesting
  public int getRateLimiterMapSize() {
    return _rateLimiterMap.size();
  }

  @VisibleForTesting
  public void cleanUpRateLimiterMap() {
    _rateLimiterMap.clear();
  }

  public void processQueryQuotaChange() {
    // TODO: update rate
  }
}
