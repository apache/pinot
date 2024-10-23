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
package org.apache.pinot.broker.queryquota;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.collections4.SetUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.pinot.broker.broker.helix.ClusterChangeHandler;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.BrokerGauge;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.spi.config.DatabaseConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is to support the qps quota feature.
 * It allows performing qps quota check at table level, database and application level.
 * For table level check it depends on the broker source change to update the dynamic rate limit,
 *  which means it gets updated when a new table added or a broker restarted.
 * For database level check it depends on the broker as well as cluster config and database config change
 * to update the dynamic rate limit, which means it gets updated when
 * - the default query quota at cluster config is updated
 * - the database config is updated
 * - new table is assigned to the broker (rate limiter is created if not present)
 * - broker added or removed from cluster
 * For application level check it depends on the broker as well as cluster config and application quota change
 * to update the dynamic rate limit, which means it gets updated when
 * - the default query quota at cluster config is updated
 * - the application quota is updated (e.g. via rest api)
 * - broker added or removed from cluster
 */
public class HelixExternalViewBasedQueryQuotaManager implements ClusterChangeHandler, QueryQuotaManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixExternalViewBasedQueryQuotaManager.class);
  private static final int ONE_SECOND_TIME_RANGE_IN_SECOND = 1;
  private static final int ONE_MINUTE_TIME_RANGE_IN_SECOND = 60;
  private static final Set<HelixConstants.ChangeType> CHANGE_TYPES_TO_PROCESS = SetUtils.hashSet(
      HelixConstants.ChangeType.EXTERNAL_VIEW, HelixConstants.ChangeType.INSTANCE_CONFIG,
      HelixConstants.ChangeType.CLUSTER_CONFIG);

  private final BrokerMetrics _brokerMetrics;
  private final String _instanceId;
  private final AtomicInteger _lastKnownBrokerResourceVersion = new AtomicInteger(-1);
  private final Map<String, QueryQuotaEntity> _rateLimiterMap = new ConcurrentHashMap<>();
  private final Map<String, QueryQuotaEntity> _databaseRateLimiterMap = new ConcurrentHashMap<>();
  private final Map<String, QueryQuotaEntity> _applicationRateLimiterMap = new ConcurrentHashMap<>();
  private double _defaultQpsQuotaForDatabase;
  private double _defaultQpsQuotaForApplication;

  private HelixManager _helixManager;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private volatile boolean _queryRateLimitDisabled;

  public HelixExternalViewBasedQueryQuotaManager(BrokerMetrics brokerMetrics, String instanceId) {
    _brokerMetrics = brokerMetrics;
    _instanceId = instanceId;
  }

  @Override
  public void init(HelixManager helixManager) {
    Preconditions.checkState(_helixManager == null, "HelixExternalViewBasedQueryQuotaManager is already initialized");
    _helixManager = helixManager;
    _propertyStore = _helixManager.getHelixPropertyStore();
    _defaultQpsQuotaForDatabase = getDefaultQueryQuotaForDatabase();
    _defaultQpsQuotaForApplication = getDefaultQueryQuotaForApplication();
    getQueryQuotaEnabledFlagFromInstanceConfig();

    initializeApplicationQpsQuotas();
  }

  // read all app quotas from ZK and create rate limiters
  private void initializeApplicationQpsQuotas() {
    Map<String, Double> quotas =
        ZKMetadataProvider.getApplicationQpsQuotas(_helixManager.getHelixPropertyStore());

    if (quotas == null || quotas.isEmpty()) {
      return;
    }

    ExternalView brokerResource = getBrokerResource();
    int numOnlineBrokers = getNumOnlineBrokers(brokerResource);

    for (Map.Entry<String, Double> entry : quotas.entrySet()) {
      if (entry.getKey() == null) {
        continue;
      }

      String appName = entry.getKey();
      double appQpsQuota =
          entry.getValue() != null && entry.getValue() != -1.0d ? entry.getValue() : _defaultQpsQuotaForApplication;

      if (appQpsQuota < 0) {
        buildEmptyOrResetApplicationRateLimiter(appName);
        continue;
      }

      double perBrokerQpsQuota = appQpsQuota / numOnlineBrokers;
      LOGGER.info("Adding new query rate limiter for application {} with rate {}.", appName, perBrokerQpsQuota);
      QueryQuotaEntity queryQuotaEntity =
          new QueryQuotaEntity(RateLimiter.create(perBrokerQpsQuota), new HitCounter(ONE_SECOND_TIME_RANGE_IN_SECOND),
              new MaxHitRateTracker(ONE_MINUTE_TIME_RANGE_IN_SECOND), numOnlineBrokers, appQpsQuota, -1);
      _applicationRateLimiterMap.put(appName, queryQuotaEntity);
    }

    return;
  }

  @Override
  public void processClusterChange(HelixConstants.ChangeType changeType) {
    Preconditions.checkState(CHANGE_TYPES_TO_PROCESS.contains(changeType), "Illegal change type: " + changeType);
    if (changeType == HelixConstants.ChangeType.EXTERNAL_VIEW) {
      ExternalView brokerResourceEV = getBrokerResource();
      processQueryRateLimitingExternalViewChange(brokerResourceEV);
    } else if (changeType == HelixConstants.ChangeType.INSTANCE_CONFIG) {
      processQueryRateLimitingInstanceConfigChange();
    } else {
      processQueryRateLimitingClusterConfigChange();
      processApplicationQueryRateLimitingClusterConfigChange();
    }
  }

  public void initOrUpdateTableQueryQuota(String tableNameWithType) {
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    ExternalView brokerResourceEV = getBrokerResource();
    initOrUpdateTableQueryQuota(tableConfig, brokerResourceEV);
  }

  /**
   * Initialize or update dynamic rate limiter with table query quota.
   * @param tableConfig table config.
   * @param brokerResourceEV broker resource which stores all the broker states of each table.
   */
  public void initOrUpdateTableQueryQuota(TableConfig tableConfig, ExternalView brokerResourceEV) {
    if (tableConfig == null) {
      LOGGER.info("No query quota to update since table config is null");
      return;
    }
    String tableNameWithType = tableConfig.getTableName();
    LOGGER.info("Initializing rate limiter for table {}", tableNameWithType);

    // Create rate limiter if query quota config is specified.
    createOrUpdateRateLimiter(tableNameWithType, brokerResourceEV, tableConfig.getQuotaConfig());
  }

  /**
   * Drop table query quota.
   * @param tableNameWithType table name with type.
   */
  public void dropTableQueryQuota(String tableNameWithType) {
    LOGGER.info("Dropping rate limiter for table {}", tableNameWithType);
    removeRateLimiter(tableNameWithType);
  }

  /** Remove or update rate limiter if another table with the same raw table name but different type is still using
   * the quota config.
   * @param tableNameWithType table name with type
   */
  private void removeRateLimiter(String tableNameWithType) {
    _rateLimiterMap.remove(tableNameWithType);
  }

  /**
   * Get QuotaConfig from property store.
   * @param tableNameWithType table name with table type.
   * @return QuotaConfig, which could be null.
   */
  private QuotaConfig getQuotaConfigFromPropertyStore(String tableNameWithType) {
    TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, tableNameWithType);
    if (tableConfig == null) {
      return null;
    }
    return tableConfig.getQuotaConfig();
  }

  /**
   * Create or update a rate limiter for a table.
   * @param tableNameWithType table name with table type.
   * @param brokerResource broker resource which stores all the broker states of each table.
   * @param quotaConfig quota config of the table.
   */
  private void createOrUpdateRateLimiter(String tableNameWithType, ExternalView brokerResource,
      QuotaConfig quotaConfig) {
    if (quotaConfig == null || quotaConfig.getMaxQueriesPerSecond() == null) {
      LOGGER.info("No qps config specified for table: {}", tableNameWithType);
      buildEmptyOrResetRateLimiterInQueryQuotaEntity(tableNameWithType);
      return;
    }

    if (brokerResource == null) {
      LOGGER.warn("Failed to init qps quota for table {}. No broker resource connected!", tableNameWithType);
      // It could be possible that brokerResourceEV is null due to ZK connection issue.
      // In this case, the rate limiter should not be reset. Simply exit the method would be sufficient.
      return;
    }

    Map<String, String> stateMap = brokerResource.getStateMap(tableNameWithType);
    int otherOnlineBrokerCount = 0;

    // If stateMap is null, that means this broker is the first broker for this table.
    if (stateMap != null) {
      for (Map.Entry<String, String> state : stateMap.entrySet()) {
        if (!_helixManager.getInstanceName().equals(state.getKey()) && state.getValue()
            .equals(CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE)) {
          otherOnlineBrokerCount++;
        }
      }
    }

    int onlineCount = otherOnlineBrokerCount + 1;
    LOGGER.info("The number of online brokers for table {} is {}", tableNameWithType, onlineCount);

    // Get the dynamic rate
    double overallRate = quotaConfig.getMaxQPS();

    // Get stat from property store
    String tableConfigPath = constructTableConfigPath(tableNameWithType);
    Stat stat = _propertyStore.getStat(tableConfigPath, AccessOption.PERSISTENT);
    double perBrokerRate = overallRate / onlineCount;

    QueryQuotaEntity queryQuotaEntity = _rateLimiterMap.get(tableNameWithType);
    if (queryQuotaEntity == null) {
      queryQuotaEntity =
          new QueryQuotaEntity(RateLimiter.create(perBrokerRate), new HitCounter(ONE_SECOND_TIME_RANGE_IN_SECOND),
              new MaxHitRateTracker(ONE_MINUTE_TIME_RANGE_IN_SECOND), onlineCount, overallRate, stat.getVersion());
      _rateLimiterMap.put(tableNameWithType, queryQuotaEntity);
      LOGGER.info(
          "Rate limiter for table: {} has been initialized. Overall rate: {}. Per-broker rate: {}. Number of online "
              + "broker instances: {}. Table config stat version: {}", tableNameWithType, overallRate, perBrokerRate,
          onlineCount, stat.getVersion());
    } else {
      RateLimiter rateLimiter = queryQuotaEntity.getRateLimiter();
      double previousRate = -1;
      if (rateLimiter == null) {
        // Query quota is just added to the table.
        rateLimiter = RateLimiter.create(perBrokerRate);
        queryQuotaEntity.setRateLimiter(rateLimiter);
      } else {
        // Query quota gets updated to a new value.
        previousRate = rateLimiter.getRate();
        rateLimiter.setRate(perBrokerRate);
      }
      queryQuotaEntity.setNumOnlineBrokers(onlineCount);
      queryQuotaEntity.setOverallRate(overallRate);
      queryQuotaEntity.setTableConfigStatVersion(stat.getVersion());
      LOGGER.info(
          "Rate limiter for table: {} has been updated. Overall rate: {}. Previous per-broker rate: {}. New "
              + "per-broker rate: {}. Number of online broker instances: {}. Table config stat version: {}",
          tableNameWithType, overallRate, previousRate, perBrokerRate, onlineCount, stat.getVersion());
    }
    addMaxBurstQPSCallbackTableGaugeIfNeeded(tableNameWithType, queryQuotaEntity);
    addQueryQuotaCapacityUtilizationRateTableGaugeIfNeeded(tableNameWithType, queryQuotaEntity);
    if (isQueryRateLimitDisabled()) {
      LOGGER.info("Query rate limiting is currently disabled for this broker. So it won't take effect immediately.");
    }
  }

  /**
   * Updates the database rate limiter if it already exists. Will not create a new database rate limiter.
   * @param databaseName database name for which rate limiter needs to be updated
   */
  public void updateDatabaseRateLimiter(String databaseName) {
    if (!_databaseRateLimiterMap.containsKey(databaseName)) {
      return;
    }
    createOrUpdateDatabaseRateLimiter(Collections.singletonList(databaseName));
  }

  /**
   * Updates the application rate limiter if it already exists. It won't  create a new rate limiter.
   *
   * @param applicationName application name for which rate limiter needs to be updated
   */
  public void updateApplicationRateLimiter(String applicationName) {
    if (!_applicationRateLimiterMap.containsKey(applicationName)) {
      return;
    }
    createOrUpdateApplicationRateLimiter(applicationName);
  }

  // Caller method need not worry about getting lock on _databaseRateLimiterMap
  // as this method will do idempotent updates to the database rate limiters
  private synchronized void createOrUpdateDatabaseRateLimiter(List<String> databaseNames) {
    ExternalView brokerResource = getBrokerResource();
    for (String databaseName : databaseNames) {
      double qpsQuota = getEffectiveQueryQuotaOnDatabase(databaseName);
      if (qpsQuota < 0) {
        buildEmptyOrResetDatabaseRateLimiter(databaseName);
        continue;
      }
      int numOnlineBrokers = getNumOnlineBrokers(databaseName, brokerResource);
      double perBrokerQpsQuota = qpsQuota / numOnlineBrokers;
      QueryQuotaEntity oldEntity = _databaseRateLimiterMap.get(databaseName);
      if (oldEntity == null) {
        LOGGER.info("Adding new query rate limiter for database {} with rate {}.", databaseName, perBrokerQpsQuota);
        QueryQuotaEntity queryQuotaEntity =
            new QueryQuotaEntity(RateLimiter.create(perBrokerQpsQuota),
                new HitCounter(ONE_SECOND_TIME_RANGE_IN_SECOND),
                new MaxHitRateTracker(ONE_MINUTE_TIME_RANGE_IN_SECOND),
                numOnlineBrokers, qpsQuota, -1);
        _databaseRateLimiterMap.put(databaseName, queryQuotaEntity);
        continue;
      }
      checkQueryQuotaChanged(databaseName, oldEntity, qpsQuota, "database", numOnlineBrokers, perBrokerQpsQuota);
    }
  }

  public synchronized void createOrUpdateApplicationRateLimiter(String applicationName) {
    createOrUpdateApplicationRateLimiter(Collections.singletonList(applicationName));
  }

  // Caller method need not worry about getting lock on _applicationRateLimiterMap
  // as this method will do idempotent updates to the application rate limiters
  private synchronized void createOrUpdateApplicationRateLimiter(List<String> applicationNames) {
    ExternalView brokerResource = getBrokerResource();
    for (String appName : applicationNames) {
      double qpsQuota = getEffectiveQueryQuotaOnApplication(appName);
      if (qpsQuota < 0) {
        buildEmptyOrResetApplicationRateLimiter(appName);
        continue;
      }
      int numOnlineBrokers = getNumOnlineBrokers(brokerResource);
      double perBrokerQpsQuota = qpsQuota / numOnlineBrokers;
      QueryQuotaEntity oldEntity = _applicationRateLimiterMap.get(appName);
      if (oldEntity == null) {
        LOGGER.info("Adding new query rate limiter for application {} with rate {}.", appName, perBrokerQpsQuota);
        QueryQuotaEntity queryQuotaEntity =
            new QueryQuotaEntity(RateLimiter.create(perBrokerQpsQuota), new HitCounter(ONE_SECOND_TIME_RANGE_IN_SECOND),
                                 new MaxHitRateTracker(ONE_MINUTE_TIME_RANGE_IN_SECOND), numOnlineBrokers, qpsQuota,
                                 -1);
        _applicationRateLimiterMap.put(appName, queryQuotaEntity);
        continue;
      }
      checkQueryQuotaChanged(appName, oldEntity, qpsQuota, "application", numOnlineBrokers, perBrokerQpsQuota);
    }
  }

  private void checkQueryQuotaChanged(String appName, QueryQuotaEntity oldEntity, double qpsQuota, String quotaType,
                                      int numOnlineBrokers, double perBrokerQpsQuota) {
    boolean isChange = false;
    double oldQuota = oldEntity.getRateLimiter() != null ? oldEntity.getRateLimiter().getRate() : -1;
    if (oldEntity.getOverallRate() != qpsQuota) {
      isChange = true;
      LOGGER.info("Overall quota changed for the {} {} from {} to {}", quotaType, appName, oldEntity.getOverallRate(),
                  qpsQuota);
      oldEntity.setOverallRate(qpsQuota);
    }
    if (oldEntity.getNumOnlineBrokers() != numOnlineBrokers) {
      isChange = true;
      LOGGER.info("Number of online brokers changed for the {} {} from {} to {}",
                  quotaType, appName, oldEntity.getNumOnlineBrokers(), numOnlineBrokers);
      oldEntity.setNumOnlineBrokers(numOnlineBrokers);
    }
    if (!isChange) {
      LOGGER.info("No change detected with the query rate limiter for {} {}", quotaType, appName);
      return;
    }
    LOGGER.info("Updating existing query rate limiter for {} {} from rate {} to {}", quotaType, appName, oldQuota,
                perBrokerQpsQuota);
    oldEntity.setRateLimiter(RateLimiter.create(perBrokerQpsQuota));
  }

  private ExternalView getBrokerResource() {
    return HelixHelper.getExternalViewForResource(_helixManager.getClusterManagmentTool(),
        _helixManager.getClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
  }

  // Pulling this logic to a separate placeholder method so that the quota split logic
  // can be enhanced further in isolation.
  private int getNumOnlineBrokers(String databaseName, ExternalView brokerResource) {
    // Tables in database can span across broker tags as we don't maintain a broker tag to database mapping as of now.
    // Hence, we consider all online brokers for the rate distribution.
    // TODO consider computing only the online brokers which serve the tables under the database
    return HelixHelper.getOnlineInstanceFromExternalView(brokerResource).size();
  }

  private int getNumOnlineBrokers(ExternalView brokerResource) {
    return HelixHelper.getOnlineInstanceFromExternalView(brokerResource).size();
  }

  /**
   * Utility to get the effective query quota being imposed on a database.
   * It is computed based on the default quota set at cluster config and override set at database config
   * @param databaseName database name to get the query quota on.
   * @return effective query quota limit being applied
   */
  private double getEffectiveQueryQuotaOnDatabase(String databaseName) {
    DatabaseConfig databaseConfig =
        ZKMetadataProvider.getDatabaseConfig(_helixManager.getHelixPropertyStore(), databaseName);
    if (databaseConfig != null && databaseConfig.getQuotaConfig() != null
        && databaseConfig.getQuotaConfig().getMaxQPS() != -1) {
      return databaseConfig.getQuotaConfig().getMaxQPS();
    }
    return _defaultQpsQuotaForDatabase;
  }

  /**
   * Utility to get the effective query quota being imposed on an application. It is computed based on the default quota
   * set at cluster config.
   *
   * @param applicationName application name to get the query quota on.
   * @return effective query quota limit being applied
   */
  private double getEffectiveQueryQuotaOnApplication(String applicationName) {
    Map<String, Double> quotas =
        ZKMetadataProvider.getApplicationQpsQuotas(_helixManager.getHelixPropertyStore());
    if (quotas != null && quotas.get(applicationName) != null && quotas.get(applicationName) != -1.0d) {
      return quotas.get(applicationName);
    }
    return _defaultQpsQuotaForApplication;
  }

  /**
   * Creates a new database rate limiter. Will not update the database rate limiter if it already exists.
   * @param databaseName database name for which rate limiter needs to be created
   */
  public void createDatabaseRateLimiter(String databaseName) {
    if (_databaseRateLimiterMap.containsKey(databaseName)) {
      return;
    }
    createOrUpdateDatabaseRateLimiter(Collections.singletonList(databaseName));
  }

  /**
   * Creates a new database rate limiter. Will not update the database rate limiter if it already exists.
   *
   * @param applicationName database name for which rate limiter needs to be created
   */
  public void createApplicationRateLimiter(String applicationName) {
    if (_applicationRateLimiterMap.containsKey(applicationName)) {
      return;
    }
    createOrUpdateApplicationRateLimiter(Collections.singletonList(applicationName));
  }

  /**
   * Build an empty rate limiter in the new query quota entity, or set the rate limiter to null in an existing query
   * quota entity.
   */
  private void buildEmptyOrResetDatabaseRateLimiter(String databaseName) {
    QueryQuotaEntity queryQuotaEntity = _databaseRateLimiterMap.get(databaseName);
    if (queryQuotaEntity == null) {
      // Create an QueryQuotaEntity object without setting a rate limiter.
      queryQuotaEntity = new QueryQuotaEntity(null, new HitCounter(ONE_SECOND_TIME_RANGE_IN_SECOND),
          new MaxHitRateTracker(ONE_MINUTE_TIME_RANGE_IN_SECOND), 0, 0, 0);
      _databaseRateLimiterMap.put(databaseName, queryQuotaEntity);
    } else {
      // Set rate limiter to null for an existing QueryQuotaEntity object.
      queryQuotaEntity.setRateLimiter(null);
    }
  }

  /**
   * Build an empty rate limiter in the new query quota entity, or set the rate limiter to null in an existing query
   * quota entity.
   */
  private void buildEmptyOrResetApplicationRateLimiter(String applicationName) {
    QueryQuotaEntity quotaEntity = _applicationRateLimiterMap.get(applicationName);
    if (quotaEntity == null) {
      // Create an QueryQuotaEntity object without setting a rate limiter.
      quotaEntity = new QueryQuotaEntity(null, new HitCounter(ONE_SECOND_TIME_RANGE_IN_SECOND),
          new MaxHitRateTracker(ONE_MINUTE_TIME_RANGE_IN_SECOND), 0, 0, 0);
      _applicationRateLimiterMap.put(applicationName, quotaEntity);
    } else {
      // Set rate limiter to null for an existing QueryQuotaEntity object.
      quotaEntity.setRateLimiter(null);
    }
  }

  /**
   * Build an empty rate limiter in the new query quota entity, or set the rate limiter to null in an existing query
   * quota entity.
   */
  private void buildEmptyOrResetRateLimiterInQueryQuotaEntity(String tableNameWithType) {
    QueryQuotaEntity queryQuotaEntity = _rateLimiterMap.get(tableNameWithType);
    if (queryQuotaEntity == null) {
      // Create an QueryQuotaEntity object without setting a rate limiter.
      queryQuotaEntity = new QueryQuotaEntity(null, new HitCounter(ONE_SECOND_TIME_RANGE_IN_SECOND),
          new MaxHitRateTracker(ONE_MINUTE_TIME_RANGE_IN_SECOND), 0, 0, 0);
      _rateLimiterMap.put(tableNameWithType, queryQuotaEntity);
    } else {
      // Set rate limiter to null for an existing QueryQuotaEntity object.
      queryQuotaEntity.setRateLimiter(null);
    }
    addMaxBurstQPSCallbackTableGaugeIfNeeded(tableNameWithType, queryQuotaEntity);
    addQueryQuotaCapacityUtilizationRateTableGaugeIfNeeded(tableNameWithType, queryQuotaEntity);
  }

  /**
   * Add the max burst QPS callback table gauge to the metric system if it doesn't exist.
   */
  private void addMaxBurstQPSCallbackTableGaugeIfNeeded(String tableNameWithType, QueryQuotaEntity queryQuotaEntity) {
    final QueryQuotaEntity finalQueryQuotaEntity = queryQuotaEntity;
    _brokerMetrics.addCallbackTableGaugeIfNeeded(tableNameWithType, BrokerGauge.MAX_BURST_QPS,
        () -> (long) finalQueryQuotaEntity.getMaxQpsTracker().getMaxCountPerBucket());
  }

  /**
   * Add the query quota capacity utilization rate table gauge to the metric system if the qps quota is specified.
   */
  private void addQueryQuotaCapacityUtilizationRateTableGaugeIfNeeded(String tableNameWithType,
      QueryQuotaEntity queryQuotaEntity) {
    if (queryQuotaEntity.getRateLimiter() != null) {
      final QueryQuotaEntity finalQueryQuotaEntity = queryQuotaEntity;
      _brokerMetrics.setOrUpdateTableGauge(tableNameWithType, BrokerGauge.QUERY_QUOTA_CAPACITY_UTILIZATION_RATE, () -> {
        double perBrokerRate = finalQueryQuotaEntity.getRateLimiter().getRate();
        int actualHitCountWithinTimeRange = finalQueryQuotaEntity.getMaxQpsTracker().getHitCount();
        long hitCountAllowedWithinTimeRage =
            (long) (perBrokerRate * finalQueryQuotaEntity.getMaxQpsTracker().getDefaultTimeRangeMs() / 1000L);
        // Since the MaxQpsTracker specifies 1-min window as valid time range, we can get the query quota capacity
        // utilization by using the actual hit count within 1 min divided by the expected hit count within 1 min.
        long percentageOfCapacityUtilization = actualHitCountWithinTimeRange * 100L / hitCountAllowedWithinTimeRage;
        LOGGER.debug("The percentage of rate limit capacity utilization is {}", percentageOfCapacityUtilization);
        return percentageOfCapacityUtilization;
      });
    }
  }

  @Override
  public boolean acquireDatabase(String databaseName) {
    // Return true if query quota is disabled in the current broker.
    if (isQueryRateLimitDisabled()) {
      return true;
    }
    QueryQuotaEntity queryQuota = _databaseRateLimiterMap.get(databaseName);
    if (queryQuota == null) {
      return true;
    }
    LOGGER.debug("Trying to acquire token for database: {}", databaseName);
    return tryAcquireToken(databaseName, queryQuota);
  }

  @Override
  public boolean acquireApplication(String applicationName) {
    if (isQueryRateLimitDisabled()) {
      return true;
    }
    QueryQuotaEntity queryQuota = _applicationRateLimiterMap.get(applicationName);
    if (queryQuota == null) {
      if (getDefaultQueryQuotaForApplication() < 0) {
        return true;
      } else {
        createOrUpdateApplicationRateLimiter(applicationName);
        queryQuota = _applicationRateLimiterMap.get(applicationName);
      }
    }

    LOGGER.debug("Trying to acquire token for application: {}", applicationName);
    return tryAcquireToken(applicationName, queryQuota);
  }

  @Override
  public double getTableQueryQuota(String tableNameWithType) {
    return getQueryQuota(_rateLimiterMap.get(tableNameWithType));
  }

  @Override
  public double getDatabaseQueryQuota(String databaseName) {
    return getQueryQuota(_databaseRateLimiterMap.get(databaseName));
  }

  @Override
  public double getApplicationQueryQuota(String applicationName) {
    return getQueryQuota(_applicationRateLimiterMap.get(applicationName));
  }

  private double getQueryQuota(QueryQuotaEntity quotaEntity) {
    return quotaEntity == null || quotaEntity.getRateLimiter() == null ? 0 : quotaEntity.getRateLimiter().getRate();
  }

  /**
   * {@inheritDoc}
   * <p>Acquires a token from rate limiter based on the table name.
   *
   * @return true if there is no query quota specified for the table or a token can be acquired, otherwise return false.
   */
  @Override
  public boolean acquire(String tableName) {
    // Return true if query quota is disabled in the current broker.
    if (isQueryRateLimitDisabled()) {
      return true;
    }
    String offlineTableName = null;
    String realtimeTableName = null;
    QueryQuotaEntity offlineTableQueryQuotaEntity = null;
    QueryQuotaEntity realtimeTableQueryQuotaEntity = null;

    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == TableType.OFFLINE) {
      offlineTableName = tableName;
      offlineTableQueryQuotaEntity = _rateLimiterMap.get(tableName);
    } else if (tableType == TableType.REALTIME) {
      realtimeTableName = tableName;
      realtimeTableQueryQuotaEntity = _rateLimiterMap.get(tableName);
    } else {
      offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      offlineTableQueryQuotaEntity = _rateLimiterMap.get(offlineTableName);
      realtimeTableQueryQuotaEntity = _rateLimiterMap.get(realtimeTableName);
    }

    boolean offlineQuotaOk = true;
    if (offlineTableQueryQuotaEntity != null) {
      LOGGER.debug("Trying to acquire token for table: {}", offlineTableName);
      offlineQuotaOk = tryAcquireToken(offlineTableName, offlineTableQueryQuotaEntity);
    }
    boolean realtimeQuotaOk = true;
    if (realtimeTableQueryQuotaEntity != null) {
      LOGGER.debug("Trying to acquire token for table: {}", realtimeTableName);
      realtimeQuotaOk = tryAcquireToken(realtimeTableName, realtimeTableQueryQuotaEntity);
    }

    return offlineQuotaOk && realtimeQuotaOk;
  }

  /**
   * Try to acquire token from rate limiter. Emit the utilization of the qps quota if broker metric isn't null.
   * @param resourceName resource name to acquire.
   * @param queryQuotaEntity query quota entity for type-specific table.
   * @return true if there's no qps quota for that table, or a token is acquired successfully.
   */
  private boolean tryAcquireToken(String resourceName, QueryQuotaEntity queryQuotaEntity) {
    // Use hit counter to count the number of hits.
    queryQuotaEntity.getQpsTracker().hit();
    queryQuotaEntity.getMaxQpsTracker().hit();

    RateLimiter rateLimiter = queryQuotaEntity.getRateLimiter();
    // Return true if no rate limiter is initialized.
    if (rateLimiter == null) {
      return true;
    }

    // Emit the qps capacity utilization rate.
    if (!rateLimiter.tryAcquire()) {
      int numHits = queryQuotaEntity.getQpsTracker().getHitCount();
      double perBrokerRate = rateLimiter.getRate();
      LOGGER.info("Quota is exceeded for table/database: {}. Per-broker rate: {}. Current qps: {}", resourceName,
          perBrokerRate, numHits);
      return false;
    }
    // Token is successfully acquired.
    return true;
  }

  @VisibleForTesting
  public int getRateLimiterMapSize() {
    return _rateLimiterMap.size();
  }

  @VisibleForTesting
  public Map<String, QueryQuotaEntity> getDatabaseRateLimiterMap() {
    return _databaseRateLimiterMap;
  }

  @VisibleForTesting
  public Map<String, QueryQuotaEntity> getApplicationRateLimiterMap() {
    return _applicationRateLimiterMap;
  }

  @VisibleForTesting
  public void cleanUpRateLimiterMap() {
    _rateLimiterMap.clear();
  }

  /**
   * Process query quota change when number of online brokers has changed.
   */
  public void processQueryRateLimitingExternalViewChange(ExternalView currentBrokerResourceEV) {
    LOGGER.info("Start processing qps quota change.");
    long startTime = System.currentTimeMillis();

    if (currentBrokerResourceEV == null) {
      LOGGER.warn("Finish processing qps quota change: external view for broker resource is null!");
      return;
    }
    int currentVersionNumber = currentBrokerResourceEV.getRecord().getVersion();
    if (currentVersionNumber == _lastKnownBrokerResourceVersion.get()) {
      LOGGER.info("No qps quota change: external view for broker resource remains the same.");
      return;
    }

    int numRebuilt = 0;
    for (Iterator<Map.Entry<String, QueryQuotaEntity>> it = _rateLimiterMap.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<String, QueryQuotaEntity> entry = it.next();
      String tableNameWithType = entry.getKey();
      QueryQuotaEntity queryQuotaEntity = entry.getValue();
      if (queryQuotaEntity.getRateLimiter() == null) {
        // No rate limiter set, skip this table.
        continue;
      }

      // Get number of online brokers.
      Map<String, String> stateMap = currentBrokerResourceEV.getStateMap(tableNameWithType);
      if (stateMap == null) {
        LOGGER.info("No broker resource for Table {}. Removing its rate limit.", tableNameWithType);
        it.remove();
        continue;
      }
      int otherOnlineBrokerCount = 0;
      for (Map.Entry<String, String> state : stateMap.entrySet()) {
        if (!_helixManager.getInstanceName().equals(state.getKey()) && state.getValue()
            .equals(CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE)) {
          otherOnlineBrokerCount++;
        }
      }
      int onlineBrokerCount = otherOnlineBrokerCount + 1;

      // Get stat from property store
      String tableConfigPath = constructTableConfigPath(tableNameWithType);
      Stat stat = _propertyStore.getStat(tableConfigPath, AccessOption.PERSISTENT);
      if (stat == null) {
        LOGGER.info("Table {} has been deleted from property store. Removing its rate limit.", tableNameWithType);
        it.remove();
        continue;
      }

      // If number of online brokers and table config don't change, there is no need to re-calculate the dynamic rate.
      if (onlineBrokerCount == queryQuotaEntity.getNumOnlineBrokers() && stat.getVersion() == queryQuotaEntity
          .getTableConfigStatVersion()) {
        continue;
      }

      double overallRate;
      // Get latest quota config only if stat don't match.
      if (stat.getVersion() != queryQuotaEntity.getTableConfigStatVersion()) {
        QuotaConfig quotaConfig = getQuotaConfigFromPropertyStore(tableNameWithType);
        if (quotaConfig == null || quotaConfig.getMaxQueriesPerSecond() == null) {
          LOGGER.info("No query quota config or the config is invalid for Table {}. Removing its rate limit.",
              tableNameWithType);
          it.remove();
          continue;
        }
        overallRate = quotaConfig.getMaxQPS();
      } else {
        overallRate = queryQuotaEntity.getOverallRate();
      }
      double latestRate = overallRate / onlineBrokerCount;
      double previousRate = queryQuotaEntity.getRateLimiter().getRate();
      if (Math.abs(latestRate - previousRate) > 0.001) {
        queryQuotaEntity.getRateLimiter().setRate(latestRate);
        queryQuotaEntity.setNumOnlineBrokers(onlineBrokerCount);
        queryQuotaEntity.setOverallRate(overallRate);
        queryQuotaEntity.setTableConfigStatVersion(stat.getVersion());
        LOGGER.info("Rate limiter for table: {} has been updated. Overall rate: {}. Previous per-broker rate: {}. New "
                + "per-broker rate: {}. Number of online broker instances: {}. Table config stat version: {}.",
            tableNameWithType, overallRate, previousRate, latestRate, onlineBrokerCount, stat.getVersion());
        numRebuilt++;
      }
    }

    // handle EV change for database query quotas
    int onlineBrokerCount = HelixHelper.getOnlineInstanceFromExternalView(currentBrokerResourceEV).size();
    for (Map.Entry<String, QueryQuotaEntity> it : _databaseRateLimiterMap.entrySet()) {
      QueryQuotaEntity quota = it.getValue();
      if (quota.getNumOnlineBrokers() != onlineBrokerCount) {
        quota.setNumOnlineBrokers(onlineBrokerCount);
      }
      if (quota.getOverallRate() > 0) {
        double qpsQuota = quota.getOverallRate() / onlineBrokerCount;
        quota.setRateLimiter(RateLimiter.create(qpsQuota));
      }
    }

    // handle EV change for application query quotas
    for (Map.Entry<String, QueryQuotaEntity> it : _applicationRateLimiterMap.entrySet()) {
      QueryQuotaEntity quota = it.getValue();
      if (quota.getNumOnlineBrokers() != onlineBrokerCount) {
        quota.setNumOnlineBrokers(onlineBrokerCount);
      }
      if (quota.getOverallRate() > 0) {
        double qpsQuota = quota.getOverallRate() / onlineBrokerCount;
        quota.setRateLimiter(RateLimiter.create(qpsQuota));
      }
    }

    if (isQueryRateLimitDisabled()) {
      LOGGER.info("Query rate limiting is currently disabled for this broker. So it won't take effect immediately.");
    }
    _lastKnownBrokerResourceVersion.set(currentVersionNumber);
    long endTime = System.currentTimeMillis();
    LOGGER
        .info("Processed query quota change in {}ms, {} out of {} query quota configs rebuilt.", (endTime - startTime),
            numRebuilt, _rateLimiterMap.size());
  }

  /**
   * Process query quota state change when cluster config gets changed
   */
  public void processQueryRateLimitingClusterConfigChange() {
    double oldDatabaseQpsQuota = _defaultQpsQuotaForDatabase;
    _defaultQpsQuotaForDatabase = getDefaultQueryQuotaForDatabase();
    if (oldDatabaseQpsQuota == _defaultQpsQuotaForDatabase) {
      return;
    }
    createOrUpdateDatabaseRateLimiter(new ArrayList<>(_databaseRateLimiterMap.keySet()));
  }

  public void processApplicationQueryRateLimitingClusterConfigChange() {
    double oldQpsQuota = _defaultQpsQuotaForApplication;
    _defaultQpsQuotaForApplication = getDefaultQueryQuotaForApplication();
    if (oldQpsQuota == _defaultQpsQuotaForApplication) {
      return;
    }
    createOrUpdateApplicationRateLimiter(new ArrayList<>(_applicationRateLimiterMap.keySet()));
  }

  private double getDefaultQueryQuotaForDatabase() {
    HelixAdmin helixAdmin = _helixManager.getClusterManagmentTool();
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
        .forCluster(_helixManager.getClusterName()).build();
    return Double.parseDouble(helixAdmin.getConfig(configScope,
            Collections.singletonList(CommonConstants.Helix.DATABASE_MAX_QUERIES_PER_SECOND))
            .getOrDefault(CommonConstants.Helix.DATABASE_MAX_QUERIES_PER_SECOND, "-1"));
  }

  private double getDefaultQueryQuotaForApplication() {
    HelixAdmin helixAdmin = _helixManager.getClusterManagmentTool();
    HelixConfigScope configScope = new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(
        _helixManager.getClusterName()).build();
    return Double.parseDouble(helixAdmin.getConfig(configScope,
            Collections.singletonList(CommonConstants.Helix.APPLICATION_MAX_QUERIES_PER_SECOND))
        .getOrDefault(CommonConstants.Helix.APPLICATION_MAX_QUERIES_PER_SECOND, "-1"));
  }

  /**
   * Process query quota state change when instance config gets changed
   */
  public void processQueryRateLimitingInstanceConfigChange() {
    getQueryQuotaEnabledFlagFromInstanceConfig();
  }

  private void getQueryQuotaEnabledFlagFromInstanceConfig() {
    try {
      Map<String, String> instanceConfigsMap = HelixHelper
          .getInstanceConfigsMapFor(_instanceId, _helixManager.getClusterName(),
              _helixManager.getClusterManagmentTool());
      String queryRateLimitDisabled =
          instanceConfigsMap.getOrDefault(CommonConstants.Helix.QUERY_RATE_LIMIT_DISABLED, "false");
      _queryRateLimitDisabled = Boolean.parseBoolean(queryRateLimitDisabled);
      LOGGER.info("Set query rate limiting to: {} for all {} tables in this broker.",
          _queryRateLimitDisabled ? "DISABLED" : "ENABLED", _rateLimiterMap.size());
    } catch (ZkNoNodeException e) {
      // It's a brand new broker. Skip checking instance config.
      _queryRateLimitDisabled = false;
    }
    _brokerMetrics.setValueOfGlobalGauge(BrokerGauge.QUERY_RATE_LIMIT_DISABLED, _queryRateLimitDisabled ? 1L : 0L);
  }

  public boolean isQueryRateLimitDisabled() {
    return _queryRateLimitDisabled;
  }

  /**
   * Construct table config path
   * @param tableNameWithType table name with table type
   */
  private String constructTableConfigPath(String tableNameWithType) {
    return "/CONFIGS/TABLE/" + tableNameWithType;
  }
}
