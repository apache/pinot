package com.linkedin.thirdeye.tools;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.LoadingCache;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.configs.DashboardConfig;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.bao.JobManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.bao.WebappConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;

public class MigrateTool {

  private static final Logger LOG = LoggerFactory.getLogger(MigrateTool.class);
  private static ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();

  private AnomalyFunctionManager anomalyFunctionDAO;
  private RawAnomalyResultManager anomalyResultDAO;
  private EmailConfigurationManager emailConfigurationDAO;
  private JobManager anomalyJobDAO;
  private TaskManager anomalyTaskDAO;
  private WebappConfigManager webappConfigDAO;
  private MergedAnomalyResultManager anomalyMergedResultDAO;
  private DatasetConfigManager datasetConfigDAO;
  private MetricConfigManager metricConfigDAO;
  private DashboardConfigManager dashboardConfigDAO;

  public MigrateTool() {

  }

  private void init(String configsRootDir) {

    File configFolder = new File(configsRootDir);
    if (!configFolder.exists()) {
      System.err.println("Configs folder not present");
      System.exit(1);
    }

    File persistence = new File(configFolder, "persistence.yml");
    if (!persistence.exists()) {
      System.err.println("Persistence file not present");
      System.exit(1);
    }

    initDAOs(persistence);

    ThirdEyeConfiguration config = new ThirdEyeAnomalyConfiguration();
    config.setRootDir(configsRootDir);
    config.setWhitelistCollections("kbmi_additive,login_mobile_additive");
    initCaches(config);
  }

  private void migrate() throws ExecutionException {
    List<String> collections = CACHE_REGISTRY_INSTANCE.getCollectionsCache().getCollections();
    LoadingCache<String, Schema> schemaCache = CACHE_REGISTRY_INSTANCE.getSchemaCache();
    LoadingCache<String, CollectionConfig> collectionConfigCache = CACHE_REGISTRY_INSTANCE.getCollectionConfigCache();
    LoadingCache<String, String> dashboardConfigCache = CACHE_REGISTRY_INSTANCE.getDashboardsCache();

    Schema schema = null;
    CollectionConfig collectionConfig = null;
    String dashboardConfig = null;

    Long datasetConfigId = null;

    for (String collection : collections) {
      try {
        schema = schemaCache.get(collection);
        collectionConfig = collectionConfigCache.get(collection);
        dashboardConfig = dashboardConfigCache.get(collection);
      } catch (Exception e) {
        LOG.info("Exception in loading from cache");
      }
      String dataset = collection;
      List<String> dimensions = schema.getDimensionNames();
      List<MetricFieldSpec> metrics = schema.getMetricFieldSpecs();
      TimeGranularitySpec timeSpec = schema.getTimeFieldSpec().getOutgoingGranularitySpec();;

      // Create DatasetConfig
      DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
      datasetConfigDTO.setDataset(dataset);
      datasetConfigDTO.setDimensions(dimensions);
      datasetConfigDTO.setTimeColumn(timeSpec.getName());
      datasetConfigDTO.setTimeDuration(timeSpec.getTimeUnitSize());
      datasetConfigDTO.setTimeUnit(timeSpec.getTimeType());
      datasetConfigDTO.setTimeFormat(timeSpec.getTimeFormat());
      if (collectionConfig != null) {
        if (StringUtils.isNotBlank(collectionConfig.getTimezone())) {
          datasetConfigDTO.setTimezone(collectionConfig.getTimezone());
        }
        if (collectionConfig.isMetricAsDimension()) {
          datasetConfigDTO.setMetricAsDimension(true);
          datasetConfigDTO.setMetricNamesColumn(collectionConfig.getMetricNamesColumn());
          datasetConfigDTO.setMetricValuesColumn(collectionConfig.getMetricValuesColumn());
        }
      }
      datasetConfigId = datasetConfigDAO.save(datasetConfigDTO);

      // Create MetricConfig
      for (MetricFieldSpec metricFieldSpec : metrics) {
        MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
        String metric = metricFieldSpec.getName();
        metricConfigDTO.setName(metric);
        metricConfigDTO.setDataset(dataset);
        metricConfigDTO.setDatatype(MetricType.valueOf(metricFieldSpec.getDataType().toString()));
        if (collectionConfig != null) {
          if (CollectionUtils.isNotEmpty(collectionConfig.getInvertColorMetrics()) && collectionConfig.getInvertColorMetrics().contains(metric)) {
            metricConfigDTO.setInverseMetric(true);
            metricConfigDTO.setCellSizeExpression(collectionConfig.getCellSizeExpression().get(metric).getExpression());
          }
          metricConfigDTO.setRollupThreshold(collectionConfig.getMetricThreshold());
        }
        metricConfigDAO.save(metricConfigDTO);
      }

      // Create derived metrics and DashboardConfig



    }
  }

  private void shutdown() throws Exception {
    CACHE_REGISTRY_INSTANCE.getQueryCache().getClient().close();
  }

  private void initDAOs(File persistenceFile) {

    LOG.info("Loading persistence config from [{}]", persistenceFile);
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    anomalyResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl.class);
    emailConfigurationDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.EmailConfigurationManagerImpl.class);
    anomalyJobDAO =
        DaoProviderUtil.getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.JobManagerImpl.class);
    anomalyTaskDAO =
        DaoProviderUtil.getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.TaskManagerImpl.class);
    webappConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.WebappConfigManagerImpl.class);
    anomalyMergedResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl.class);
  }

  private void initCaches(ThirdEyeConfiguration config) {
    ThirdEyeCacheRegistry.initializeCaches(config, webappConfigDAO);
  }


  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("USAGE MIgrateTool <configs_folder>");
      System.exit(1);
    }
    MigrateTool mt = new MigrateTool();
    mt.init(args[0]);
    mt.migrate();
    mt.shutdown();
  }
}
