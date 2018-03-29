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
package com.linkedin.pinot.broker.queryquota;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.RateLimiter;
import com.linkedin.pinot.common.config.QuotaConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metrics.BrokerMeter;
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
   * */
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
   * @param tableConfig table config.
   * @param brokerResource broker resource which stores all the broker states of each table.
   * */
  public void dropTableQueryQuota(TableConfig tableConfig, ExternalView brokerResource) {
    String tableName = tableConfig.getTableName();
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    LOGGER.info("Dropping rate limiter for table {}", rawTableName);

    removeRateLimiter(brokerResource, tableName, tableConfig.getQuotaConfig());
  }

  /** Remove or update rate limiter if another table with the same raw table name but different type is still using the quota config.
   * @param brokerResource broker resource which stores all the broker states of each table.
   * @param tableName original table name
   * @param quotaConfig old quota config
   * */
  private void removeRateLimiter(ExternalView brokerResource, String tableName, QuotaConfig quotaConfig) {
    if (quotaConfig == null || Strings.isNullOrEmpty(quotaConfig.getMaxQueriesPerSecond()) || !_rateLimiterMap.containsKey(tableName)) {
      // Do nothing if the current table doesn't specify qps quota.
      LOGGER.info("No qps quota is specified for table: {}", tableName);
      return;
    }
    if (brokerResource == null) {
      LOGGER.warn("Failed to drop qps quota for table {}. No broker resource connected!", tableName);
      return;
    }
    _rateLimiterMap.remove(tableName);
  }

  /**
   * Get QuotaConfig from property store.
   * @param rawTableName table name without table type.
   * @param tableType table type: offline or real-time.
   * @return QuotaConfig, which could be null.
   * */
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
   * */
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
    if (stateMap == null) {
      LOGGER.warn("Failed to init qps quota: table {} is not in the broker resource!", tableName);
      return;
    }
    int onlineCount = 0;
    for (String state : stateMap.values()) {
      if (state.equals(CommonConstants.Helix.StateModel.SegmentOnlineOfflineStateModel.ONLINE)) {
        onlineCount++;
      }
    }
    LOGGER.info("The number of online brokers for table {} is {}", tableName, onlineCount);

    // assert onlineCount > 0;
    if (onlineCount == 0) {
      LOGGER.warn("Failed to init qps quota: there's no online broker services for table {}!", tableName);
//      return;
    }

    // FIXME We use fixed rate for the 1st version.
    onlineCount = 1;

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
   * */
  public boolean acquire(String tableName, String offlineTableName, String realtimeTableName) {
    LOGGER.info("Trying to acquire token for table: {}", tableName);

    QueryQuotaConfig offlineTableQueryQuotaConfig = null;
    QueryQuotaConfig realtimeTableQueryQuotaConfig = null;
    if (offlineTableName != null) {
      offlineTableQueryQuotaConfig = _rateLimiterMap.get(offlineTableName);
    }
    if (realtimeTableName != null) {
      realtimeTableQueryQuotaConfig = _rateLimiterMap.get(realtimeTableName);
    }

    if (offlineTableQueryQuotaConfig == null && realtimeTableQueryQuotaConfig == null) {
      LOGGER.info("No qps quota is specified for table: {}", tableName);
      return true;
    }

    // Try to acquire token for offline table if there's a qps quota.
    if (offlineTableQueryQuotaConfig != null && !tryAcquireToken(tableName, offlineTableQueryQuotaConfig)) {
      return false;
    }

    // Try to acquire token for offline table if there's a qps quota.
    return realtimeTableQueryQuotaConfig == null || tryAcquireToken(tableName, realtimeTableQueryQuotaConfig);
  }

  /**
   * Try to acquire token from rate limiter. Emit the utilization of the qps quota if broker metric isn't null.
   * @param tableName origin table name, which could be raw.
   * @param queryQuotaConfig query quota config for type-specific table.
   * @return true if there's no qps quota for that table, or a token is acquired successfully.
   * */
  private boolean tryAcquireToken(String tableName, QueryQuotaConfig queryQuotaConfig) {
    // Use hit counter to count the number of hits.
    queryQuotaConfig.getHitCounter().hit();

    RateLimiter rateLimiter = queryQuotaConfig.getRateLimiter();
    double perBrokerRate = rateLimiter.getRate();
    if (!rateLimiter.tryAcquire()) {
      LOGGER.error("Quota is exceeded for table: {}. Per-broker rate: {}", tableName, perBrokerRate);
      return false;
    }

    // Emit the qps capacity utilization rate.
    if (_brokerMetrics != null) {
      int numHits = queryQuotaConfig.getHitCounter().getHitCount();
      int percentageOfCapacityUtilization = (int)(numHits * 100 / perBrokerRate);
      LOGGER.info("The percentage of rate limit capacity utilization is {}", percentageOfCapacityUtilization);
      _brokerMetrics.addMeteredTableValue(tableName, BrokerMeter.QUERY_QUOTA_CAPACITY_UTILIZATION_RATE, percentageOfCapacityUtilization);
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
