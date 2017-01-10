package com.linkedin.thirdeye.tools;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jersey.repackaged.com.google.common.collect.Lists;

import org.apache.http.HttpHost;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.api.CollectionSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.configs.CollectionConfig;
import com.linkedin.thirdeye.dashboard.configs.DashboardConfig;
import com.linkedin.thirdeye.dashboard.configs.WebappConfigFactory.WebappConfigType;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.WebappConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.WebappConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DashboardConfigBean;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

/**
 * Tool to read all pinot datasets and populate the new metric centered dashboard tables.
 */
public class LoadMetricsFromPinotTool {

  private static final Logger LOG = LoggerFactory.getLogger(LoadMetricsFromPinotTool.class);
  private static final String TABLES_ENDPOINT = "tables/";
  private static final String SCHEMA_ENDPOINT_TEMPLATE = "tables/%s/schema";
  private static final String UTF_8 = "UTF-8";

  private AnomalyFunctionManager anomalyFunctionDAO;
  private WebappConfigManager webappConfigDAO;
  private DatasetConfigManager datasetConfigDAO;
  private MetricConfigManager metricConfigDAO;
  private DashboardConfigManager dashboardConfigDAO;

  private final CloseableHttpClient controllerClient;
  private final HttpHost controllerHost;

  private List<String> allCollections = new ArrayList<>();
  private Map<String, Schema> allSchemas = new HashMap<>();

  public LoadMetricsFromPinotTool(String configsRootDir) throws Exception {

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

    PinotThirdEyeClientConfig pinotThirdeyeClientConfig = PinotThirdEyeClientConfig.createThirdEyeClientConfig(config);
    this.controllerClient = HttpClients.createDefault();
    this.controllerHost = new HttpHost(pinotThirdeyeClientConfig.getControllerHost(),
        pinotThirdeyeClientConfig.getControllerPort());

    loadAllCollections();
    LOG.info("Collections {}", allCollections);
  }


  private void initDAOs(File persistenceFile) {

    LOG.info("Loading persistence config from [{}]", persistenceFile);
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    webappConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.WebappConfigManagerImpl.class);
    datasetConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl.class);
    metricConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MetricConfigManagerImpl.class);
    dashboardConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.DashboardConfigManagerImpl.class);
    DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
    DAO_REGISTRY.registerDAOs(anomalyFunctionDAO, null, null, null, null, null, datasetConfigDAO,
        metricConfigDAO, dashboardConfigDAO, null, null, null, null);
  }


  private void loadAllCollections() throws Exception {

    HttpGet tablesReq = new HttpGet(TABLES_ENDPOINT);
    LOG.info("Retrieving collections: {}", tablesReq);
    CloseableHttpResponse tablesRes = controllerClient.execute(controllerHost, tablesReq);
    JsonNode tables = null;
    try {
      if (tablesRes.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(tablesRes.getStatusLine().toString());
      }
      InputStream tablesContent = tablesRes.getEntity().getContent();
      tables = new ObjectMapper().readTree(tablesContent).get("tables");
    } catch (Exception e) {
      LOG.error("Exception in loading collections", e);
    } finally {
      if (tablesRes.getEntity() != null) {
        EntityUtils.consume(tablesRes.getEntity());
      }
      tablesRes.close();
    }

    for (JsonNode table : tables) {
    String collection = table.asText();

      CollectionSchema collectionSchema = null;
      List<WebappConfigDTO> webappConfigDTOs = webappConfigDAO.
          findByCollectionAndType(collection, WebappConfigType.COLLECTION_SCHEMA);
      if (CollectionUtils.isNotEmpty(webappConfigDTOs)) {
        String configJson = Utils.getJsonFromObject(webappConfigDTOs.get(0).getConfigMap());
        collectionSchema = CollectionSchema.fromJSON(configJson, CollectionSchema.class);
        Schema pinotSchema = ThirdEyeUtils.createSchema(collectionSchema);
        allCollections.add(collection);
        allSchemas.put(collection, pinotSchema);
        continue;
      }

      HttpGet schemaReq = new HttpGet(String.format(SCHEMA_ENDPOINT_TEMPLATE, URLEncoder.encode(collection, UTF_8)));
      LOG.info("Retrieving schema: {}", schemaReq);
      CloseableHttpResponse schemaRes = controllerClient.execute(controllerHost, schemaReq);
      try {
        if (schemaRes.getStatusLine().getStatusCode() != 200) {
          LOG.error("Schema {} not found, {}", collection, schemaRes.getStatusLine().toString());
        }
        InputStream schemaContent = schemaRes.getEntity().getContent();
        Schema schema = new org.codehaus.jackson.map.ObjectMapper().readValue(schemaContent, Schema.class);

        allCollections.add(collection);
        allSchemas.put(collection, schema);
      } catch (Exception e) {
        LOG.error("Exception in retrieving schema collections, skipping {}", collection);
      } finally {
        if (schemaRes.getEntity() != null) {
          EntityUtils.consume(schemaRes.getEntity());
        }
        schemaRes.close();
      }
    }
  }


  private void migrate() throws Exception {

    for (String collection : allCollections) {
      try {

        // Skip if dataset already created
        DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(collection);
        if (datasetConfig != null) {
          continue;
        }
        LOG.info("Migrating collection {}", collection);
        Schema schema = null;
        CollectionConfig collectionConfig = null;

        schema = allSchemas.get(collection);
        List<WebappConfigDTO> webappConfigDTOs = webappConfigDAO.
            findByCollectionAndType(collection, WebappConfigType.COLLECTION_CONFIG);
        if (CollectionUtils.isNotEmpty(webappConfigDTOs)) {
          String configJson = Utils.getJsonFromObject(webappConfigDTOs.get(0).getConfigMap());
          collectionConfig = CollectionConfig.fromJSON(configJson, CollectionConfig.class);
        }

        // TODO: this condition will go away once this tool becomes a pure pull from pinot instead of a migrate existing
        if (collectionConfig != null && collectionConfig.isMetricAsDimension()) {
          loadMetricAsDimensionConfigs(collection, schema, collectionConfig);
        } else {
          loadStandardConfigs(collection, schema, collectionConfig);
        }
      } catch (Exception e) {
        LOG.error("Exception in migrating collection {}", collection, e);
      }
    }
  }

  private void loadStandardConfigs(String collection, Schema schema, CollectionConfig collectionConfig) throws Exception {

    LOG.info("Loading collection {}", collection);
    String dataset = collection;
    List<String> dimensions = schema.getDimensionNames();
    List<MetricFieldSpec> metricSpecs = schema.getMetricFieldSpecs();
    List<String> metricNames = schema.getMetricNames();
    TimeGranularitySpec timeSpec = schema.getTimeFieldSpec().getOutgoingGranularitySpec();

    // Create DatasetConfig
    Long datasetConfigId = null;
    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset(dataset);
    datasetConfigDTO.setDimensions(dimensions);
    datasetConfigDTO.setTimeColumn(timeSpec.getName());
    datasetConfigDTO.setTimeDuration(timeSpec.getTimeUnitSize());
    datasetConfigDTO.setTimeUnit(timeSpec.getTimeType());
    datasetConfigDTO.setTimeFormat(timeSpec.getTimeFormat());
    // TODO: Change how to fetch this once we don't have CollectionConfig
    if (collectionConfig != null) {
      if (StringUtils.isNotBlank(collectionConfig.getTimezone())) {
        datasetConfigDTO.setTimezone(collectionConfig.getTimezone());
      }
      if (collectionConfig.isNonAdditive()) {
        datasetConfigDTO.setAdditive(false);
        datasetConfigDTO.setDimensionsHaveNoPreAggregation(collectionConfig.getDimensionsHaveNoPreAggregation());
        datasetConfigDTO.setPreAggregatedKeyword(collectionConfig.getPreAggregatedKeyword());
        datasetConfigDTO.setNonAdditiveBucketSize(collectionConfig.getNonAdditiveBucketSize());
        datasetConfigDTO.setNonAdditiveBucketUnit(collectionConfig.getNonAdditiveBucketUnit());
      }
    }
    LOG.info("Creating dataset for {}", dataset);
    datasetConfigId = datasetConfigDAO.save(datasetConfigDTO);

    DatasetConfigDTO createdDataset = datasetConfigDAO.findById(datasetConfigId);

    // Create MetricConfig
    for (MetricFieldSpec metricFieldSpec : metricSpecs) {
      MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
      String metric = metricFieldSpec.getName();
      metricConfigDTO.setName(metric);
      metricConfigDTO.setAlias(ThirdEyeUtils.constructMetricAlias(dataset, metric));
      metricConfigDTO.setDataset(dataset);
      metricConfigDTO.setDatatype(MetricType.valueOf(metricFieldSpec.getDataType().toString()));
      if (collectionConfig != null) {
        if (CollectionUtils.isNotEmpty(collectionConfig.getInvertColorMetrics())
            && collectionConfig.getInvertColorMetrics().contains(metric)) {
          metricConfigDTO.setInverseMetric(true);
        }
        if (collectionConfig.getCellSizeExpression() != null
            && collectionConfig.getCellSizeExpression().containsKey(metric)) {
          String cellSizeExpression = ThirdEyeUtils.substituteMetricIdsForMetrics(
              collectionConfig.getCellSizeExpression().get(metric).getExpression(), dataset);
          metricConfigDTO.setCellSizeExpression(cellSizeExpression);
        }
        metricConfigDTO.setRollupThreshold(collectionConfig.getMetricThreshold());
      }
      metricConfigDTO.setDatasetConfig(createdDataset);
      LOG.info("Creating metric {} for {}", metric, dataset);
      metricConfigDAO.save(metricConfigDTO);
    }
    loadCommonConfigs(dataset, collectionConfig, metricNames, createdDataset);

  }

  private void loadCommonConfigs(String dataset, CollectionConfig collectionConfig, List<String> metricNames,
      DatasetConfigDTO createdDataset) throws Exception {
    // Create derived metrics
    if (collectionConfig != null) {
      Map<String, String> derivedMetrics = collectionConfig.getDerivedMetrics();
      if (derivedMetrics != null) {
        for (Entry<String, String> entry : derivedMetrics.entrySet()) {
          MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
          String metric = entry.getKey();
          metricConfigDTO.setName(metric);
          metricConfigDTO.setAlias(ThirdEyeUtils.constructMetricAlias(dataset, metric));
          metricConfigDTO.setDataset(dataset);
          metricConfigDTO.setDatatype(MetricType.DOUBLE);
          if (CollectionUtils.isNotEmpty(collectionConfig.getInvertColorMetrics())
              && collectionConfig.getInvertColorMetrics().contains(metric)) {
            metricConfigDTO.setInverseMetric(true);
          }
          if (collectionConfig.getCellSizeExpression() != null
              && collectionConfig.getCellSizeExpression().containsKey(metric)) {
            String cellSizeExpression = ThirdEyeUtils.substituteMetricIdsForMetrics(
                collectionConfig.getCellSizeExpression().get(metric).getExpression(), dataset);
            metricConfigDTO.setCellSizeExpression(cellSizeExpression);
          }
          metricConfigDTO.setRollupThreshold(collectionConfig.getMetricThreshold());

          metricConfigDTO.setDerived(true);
          String derivedMetricExpression = entry.getValue();
          derivedMetricExpression = ThirdEyeUtils.substituteMetricIdsForMetrics(derivedMetricExpression, dataset);
          metricConfigDTO.setDerivedMetricExpression(derivedMetricExpression);

          metricConfigDTO.setDatasetConfig(createdDataset);

          LOG.info("Creating derived metric {} for dataset {}", metric, dataset);
          metricConfigDAO.save(metricConfigDTO);
        }
      }
    }

    // Create DashboardConfig
    List<WebappConfigDTO> webappConfigs = webappConfigDAO.
        findByCollectionAndType(dataset, WebappConfigType.DASHBOARD_CONFIG);
    for (WebappConfigDTO webappConfig : webappConfigs) {
      String configJson = Utils.getJsonFromObject(webappConfig.getConfigMap());
      DashboardConfig dashboardConfig = DashboardConfig.fromJSON(configJson, DashboardConfig.class);
      String dashboardName = dashboardConfig.getDashboardName();
      if (dashboardConfigDAO.findByName(dashboardName) != null) {
        dashboardName = dashboardName + "_" + dataset;
      }

      List<Long> metricIds = new ArrayList<>();
      List<MetricExpression> metricExpressions = dashboardConfig.getMetricExpressions();
      for (MetricExpression metricExpression : metricExpressions) {
        String metric = metricExpression.getExpressionName();
        MetricConfigDTO metricConfig = metricConfigDAO.findByMetricAndDataset(metric, dataset);
        metricIds.add(metricConfig.getId());
      }

      DashboardConfigDTO dashboardConfigDTO = new DashboardConfigDTO();
      dashboardConfigDTO.setName(dashboardName);
      dashboardConfigDTO.setDataset(dataset);
      dashboardConfigDTO.setDatasetConfig(createdDataset);
      dashboardConfigDTO.setFilterClause(dashboardConfig.getFilterClause());
      dashboardConfigDTO.setMetricIds(metricIds);

      LOG.info("Creating custom dashboard {} for dataset {}", dashboardName, dataset);
      dashboardConfigDAO.save(dashboardConfigDTO);
    }

    // Create Default DashboardConfig
    List<MetricConfigDTO> allMetricConfigs = metricConfigDAO.findByDataset(dataset);
    List<Long> customDashboardMetricIds = new ArrayList<>();
    for (MetricConfigDTO metricConfig : allMetricConfigs) {
      if (!metricConfig.isDerived()) {
        customDashboardMetricIds.add(metricConfig.getId());
      }
    }
    List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionDAO.findAllByCollection(dataset);
    List<Long> anomalyFunctionIds = new ArrayList<>();
    for (AnomalyFunctionDTO anomalyFunctionDTO : anomalyFunctions) {
      anomalyFunctionIds.add(anomalyFunctionDTO.getId());
    }
    DashboardConfigDTO customDashboardConfigDTO = new DashboardConfigDTO();
    String dashboardName = DashboardConfigBean.DEFAULT_DASHBOARD_PREFIX + dataset;
    customDashboardConfigDTO.setName(dashboardName);
    customDashboardConfigDTO.setDataset(dataset);
    customDashboardConfigDTO.setDatasetConfig(createdDataset);
    customDashboardConfigDTO.setMetricIds(customDashboardMetricIds);
    customDashboardConfigDTO.setAnomalyFunctionIds(anomalyFunctionIds);

    LOG.info("Creating default dashboard {} for dataset {}", dashboardName, dataset);
    dashboardConfigDAO.save(customDashboardConfigDTO);
  }

  private void loadMetricAsDimensionConfigs(String collection, Schema schema, CollectionConfig collectionConfig) throws Exception {
    LOG.info("Loading collection {}", collection);

    WebappConfigDTO collectionSchemaWebappConfig = webappConfigDAO.
        findByCollectionAndType(collection, WebappConfigType.COLLECTION_SCHEMA).get(0);
    String collectionSchemaJson = Utils.getJsonFromObject(collectionSchemaWebappConfig.getConfigMap());
    CollectionSchema collectionSchema = CollectionSchema.fromJSON(collectionSchemaJson, CollectionSchema.class);

    String dataset = collection;
    List<String> dimensions = collectionSchema.getDimensionNames();
    List<MetricSpec> metricSpecs = collectionSchema.getMetrics();
    List<String> metricNames = collectionSchema.getMetricNames();
    TimeGranularitySpec timeSpec = schema.getTimeFieldSpec().getOutgoingGranularitySpec();;

    // Create DatasetConfig
    Long datasetConfigId = null;
    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();
    datasetConfigDTO.setDataset(dataset);
    datasetConfigDTO.setDimensions(dimensions);
    datasetConfigDTO.setTimeColumn(timeSpec.getName());
    datasetConfigDTO.setTimeDuration(timeSpec.getTimeUnitSize());
    datasetConfigDTO.setTimeUnit(timeSpec.getTimeType());
//    datasetConfigDTO.setTimeFormat(timeSpec.getTimeFormat());
    if (StringUtils.isNotBlank(collectionConfig.getTimezone())) {
      datasetConfigDTO.setTimezone(collectionConfig.getTimezone());
    }
    datasetConfigDTO.setMetricAsDimension(true);
    datasetConfigDTO.setMetricNamesColumn(collectionConfig.getMetricNamesColumn());
    datasetConfigDTO.setMetricValuesColumn(collectionConfig.getMetricValuesColumn());

    LOG.info("Creating metricAsDimension dataset {}", dataset);
    datasetConfigId = datasetConfigDAO.save(datasetConfigDTO);

    DatasetConfigDTO createdDataset = datasetConfigDAO.findById(datasetConfigId);

    // Create MetricConfig
    for (MetricSpec metricFieldSpec : metricSpecs) {
      MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
      String metric = metricFieldSpec.getName();
      metricConfigDTO.setName(metric);
      metricConfigDTO.setAlias(ThirdEyeUtils.constructMetricAlias(dataset, metric));
      metricConfigDTO.setDataset(dataset);
      metricConfigDTO.setDatatype(metricFieldSpec.getType());
      if (CollectionUtils.isNotEmpty(collectionConfig.getInvertColorMetrics())
          && collectionConfig.getInvertColorMetrics().contains(metric)) {
        metricConfigDTO.setInverseMetric(true);
      }
      if (collectionConfig.getCellSizeExpression() != null
          && collectionConfig.getCellSizeExpression().containsKey(metric)) {
        String cellSizeExpression = ThirdEyeUtils.substituteMetricIdsForMetrics(
              collectionConfig.getCellSizeExpression().get(metric).getExpression(), dataset);
          metricConfigDTO.setCellSizeExpression(cellSizeExpression);
      }
      metricConfigDTO.setRollupThreshold(collectionConfig.getMetricThreshold());

      metricConfigDTO.setDatasetConfig(createdDataset);

      LOG.info("Creating metric {} for dataset {}", metric, dataset);
      metricConfigDAO.save(metricConfigDTO);
    }
    loadCommonConfigs(dataset, collectionConfig, metricNames, createdDataset);
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("USAGE LoadMetricsFromPinot <configs_folder>");
      System.exit(1);
    }
    LoadMetricsFromPinotTool mt = new LoadMetricsFromPinotTool(args[0]);
    mt.migrate();
  }
}
