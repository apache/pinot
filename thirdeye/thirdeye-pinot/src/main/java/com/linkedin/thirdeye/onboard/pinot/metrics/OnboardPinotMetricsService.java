package com.linkedin.thirdeye.onboard.pinot.metrics;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.dashboard.resources.CacheResource;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DashboardConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.bao.WebappConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;

public class OnboardPinotMetricsService implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(OnboardPinotMetricsService.class);

  private static final String PINOT_TABLES_ENDPOINT = "tables/";
  private static final String PINOT_SCHEMA_ENDPOINT = "schemas/";
  private static final String UTF_8 = "UTF-8";
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private AnomalyFunctionManager anomalyFunctionDAO;
  private WebappConfigManager webappConfigDAO;
  private DatasetConfigManager datasetConfigDAO;
  private MetricConfigManager metricConfigDAO;
  private DashboardConfigManager dashboardConfigDAO;

  private CloseableHttpClient pinotControllerClient;
  private HttpHost pinotControllerHost;
  private CacheResource cacheResource;

  private ScheduledExecutorService scheduledExecutorService;

  private List<String> allCollections = new ArrayList<>();
  private Map<String, Schema> allSchemas = new HashMap<>();

  public OnboardPinotMetricsService(ThirdEyeConfiguration config) {
    try {
      PinotThirdEyeClientConfig pinotThirdeyeClientConfig = PinotThirdEyeClientConfig.createThirdEyeClientConfig(config);
      this.pinotControllerClient = HttpClients.createDefault();
      this.pinotControllerHost = new HttpHost(pinotThirdeyeClientConfig.getControllerHost(),
          pinotThirdeyeClientConfig.getControllerPort());
    } catch (Exception e) {
     LOG.error("Exception in creating pinot controller http host", e);
    }
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    cacheResource = new CacheResource();
    anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    datasetConfigDAO = DAO_REGISTRY.getDatasetConfigDAO();
    metricConfigDAO = DAO_REGISTRY.getMetricConfigDAO();
    dashboardConfigDAO = DAO_REGISTRY.getDashboardConfigDAO();
  }

  public void start() {
    scheduledExecutorService.scheduleAtFixedRate(this, 0, 4, TimeUnit.HOURS);
  }

  public void shutdown() {
    scheduledExecutorService.shutdown();
  }


  public void run() {
    try {
      // fetch all datasets and schemas
      loadDatasets();

      // for each dataset,
      for (String dataset : allCollections) {
        DatasetConfigDTO oldDatasetConfig = datasetConfigDAO.findByDataset(dataset);

        // if new add dataset, metrics, default dashboard
        if (oldDatasetConfig == null) {
          addNewDataset(dataset);
        } else {
        // if old, check for changes in metrics or dimensions
          refreshOldDataset(dataset);
        }
      }
      // refresh thirdeye caches
      refreshCaches();

    } catch (IOException e) {
      LOG.error("Exception in loading datasets", e);
    }


  }

  private void addNewDataset(String dataset) {
    Schema schema = allSchemas.get(dataset);
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
    if (timeSpec.getTimeFormat().startsWith(TimeFormat.SIMPLE_DATE_FORMAT.toString())) {
      datasetConfigDTO.setTimezone("US/Pacific");
    }

    LOG.info("Creating dataset for {}", dataset);
    datasetConfigId = datasetConfigDAO.save(datasetConfigDTO);

    DatasetConfigDTO createdDataset = datasetConfigDAO.findById(datasetConfigId);

    // Create MetricConfig
    for (MetricFieldSpec metricFieldSpec : metricSpecs) {
      MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
      String metric = metricFieldSpec.getName();
      metricConfigDTO.setName(metric);
      metricConfigDTO.setAlias(metric);
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

  private void refreshOldDataset(String dataset) {

  }

  private void refreshCaches() {
    cacheResource.refreshAllCaches();
  }

  private void loadDatasets() throws IOException {

    // Fetch all datasets from pinot
    HttpGet tablesReq = new HttpGet(PINOT_TABLES_ENDPOINT);
    LOG.info("Retrieving datasets: {}", tablesReq);
    CloseableHttpResponse tablesRes = pinotControllerClient.execute(pinotControllerHost, tablesReq);
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

    // Fetch schemas for all datasets from pinot
    for (JsonNode table : tables) {
      String collection = table.asText();

        HttpGet schemaReq = new HttpGet(String.format(PINOT_SCHEMA_ENDPOINT, URLEncoder.encode(collection, UTF_8)));
        LOG.info("Retrieving schema: {}", schemaReq);
        CloseableHttpResponse schemaRes = pinotControllerClient.execute(pinotControllerHost, schemaReq);
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

}
