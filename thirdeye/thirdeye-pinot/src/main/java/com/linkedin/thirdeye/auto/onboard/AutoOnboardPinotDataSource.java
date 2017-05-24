package com.linkedin.thirdeye.auto.onboard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.DataSourceConfig;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.pinot.PinotQuery;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

/**
 * This is a service to onboard datasets automatically to thirdeye from pinot
 * The run method is invoked periodically by the AutoOnboardService, and it checks for new tables in pinot, to add to thirdeye
 * If the table is an ingraph table, it loads metrics from the ingraph table
 * It also looks for any changes in dimensions or metrics to the existing tables
 */
public class AutoOnboardPinotDataSource extends AutoOnboard {
  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardPinotDataSource.class);


  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private static final String DEFAULT_INGRAPH_METRIC_NAMES_COLUMN = "metricName";
  private static final String DEFAULT_INGRAPH_METRIC_VALUES_COLUMN = "value";

  private AutoOnboardPinotMetricsUtils autoLoadPinotMetricsUtils;

  private List<String> allDatasets = new ArrayList<>();
  private Map<String, Schema> allSchemas = new HashMap<>();

  public AutoOnboardPinotDataSource(DataSourceConfig dataSourceConfig) {
    super(dataSourceConfig);
    autoLoadPinotMetricsUtils = new AutoOnboardPinotMetricsUtils(dataSourceConfig);
  }

  public AutoOnboardPinotDataSource(DataSourceConfig dataSourceConfig, AutoOnboardPinotMetricsUtils utils) {
    super(dataSourceConfig);
    autoLoadPinotMetricsUtils = utils;
  }

  public void run() {
    try {
      loadDatasets();
      LOG.info("Checking all datasets");
      for (String dataset : allDatasets) {
        LOG.info("Checking dataset {}", dataset);

        Schema schema = allSchemas.get(dataset);
        if (!isIngraphDataset(schema)) {
          DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);
          addPinotDataset(dataset, schema, datasetConfig);
        }
      }
    } catch (Exception e) {
      LOG.error("Exception in loading datasets", e);
    }
  }



  /**
   * Adds a dataset to the thirdeye database
   * @param dataset
   * @param schema
   * @param datasetConfig
   */
  public void addPinotDataset(String dataset, Schema schema, DatasetConfigDTO datasetConfig) throws Exception {
    if (datasetConfig == null) {
      LOG.info("Dataset {} is new, adding it to thirdeye", dataset);
      addNewDataset(dataset, schema);
    } else {
      LOG.info("Dataset {} already exists, checking for updates", dataset);
      refreshOldDataset(dataset, datasetConfig, schema);
    }
  }

  /**
   * Adds a new dataset to the thirdeye database
   * @param dataset
   * @param schema
   */
  private void addNewDataset(String dataset, Schema schema) throws Exception {
    List<MetricFieldSpec> metricSpecs = schema.getMetricFieldSpecs();

    // Create DatasetConfig
    DatasetConfigDTO datasetConfigDTO = ConfigGenerator.generateDatasetConfig(dataset, schema);
    LOG.info("Creating dataset for {}", dataset);
    DAO_REGISTRY.getDatasetConfigDAO().save(datasetConfigDTO);

    // Create MetricConfig
    for (MetricFieldSpec metricFieldSpec : metricSpecs) {
      MetricConfigDTO metricConfigDTO = ConfigGenerator.generateMetricConfig(metricFieldSpec, dataset);
      LOG.info("Creating metric {} for {}", metricConfigDTO.getName(), dataset);
      DAO_REGISTRY.getMetricConfigDAO().save(metricConfigDTO);
    }

    // Create Default DashboardConfig
    List<Long> metricIds = ConfigGenerator.getMetricIdsFromMetricConfigs(DAO_REGISTRY.getMetricConfigDAO().findByDataset(dataset));
    DashboardConfigDTO dashboardConfigDTO = ConfigGenerator.generateDefaultDashboardConfig(dataset, metricIds);
    LOG.info("Creating default dashboard for dataset {}", dataset);
    DAO_REGISTRY.getDashboardConfigDAO().save(dashboardConfigDTO);
  }

  /**
   * Refreshes an existing dataset in the thirdeye database
   * with any dimension/metric changes from pinot schema
   * @param dataset
   * @param datasetConfig
   * @param schema
   */
  private void refreshOldDataset(String dataset, DatasetConfigDTO datasetConfig, Schema schema) throws Exception {

    if (datasetConfig.isMetricAsDimension()) {
      LOG.info("Checking refresh for metricAsDimension dataset {}", dataset);
      checkMetricAsDimensionDataset(datasetConfig, schema);
    } else {
      checkDimensionChanges(dataset, datasetConfig, schema);
      checkMetricChanges(dataset, datasetConfig, schema);
    }
  }



  private void checkDimensionChanges(String dataset, DatasetConfigDTO datasetConfig, Schema schema) {

    LOG.info("Checking for dimensions changes in {}", dataset);
    List<String> schemaDimensions = schema.getDimensionNames();
    List<String> datasetDimensions = datasetConfig.getDimensions();
    List<String> dimensionsToAdd = new ArrayList<>();
    List<String> dimensionsToRemove = new ArrayList<>();

    // dimensions which are new in the pinot schema
    for (String dimensionName : schemaDimensions) {
      if (!datasetDimensions.contains(dimensionName)) {
        dimensionsToAdd.add(dimensionName);
      }
    }
    // dimensions which are removed form pinot schema
    for (String dimensionName : datasetDimensions) {
      if (!schemaDimensions.contains(dimensionName)) {
        dimensionsToRemove.add(dimensionName);
      }
    }
    if (CollectionUtils.isNotEmpty(dimensionsToAdd) || CollectionUtils.isNotEmpty(dimensionsToRemove)) {
      datasetDimensions.addAll(dimensionsToAdd);
      datasetDimensions.removeAll(dimensionsToRemove);
      datasetConfig.setDimensions(datasetDimensions);

      if (!datasetConfig.isAdditive()
          && CollectionUtils.isNotEmpty(datasetConfig.getDimensionsHaveNoPreAggregation())) {
        List<String> dimensionsHaveNoPreAggregation = datasetConfig.getDimensionsHaveNoPreAggregation();
        dimensionsHaveNoPreAggregation.removeAll(dimensionsToRemove);
        datasetConfig.setDimensionsHaveNoPreAggregation(dimensionsHaveNoPreAggregation);
      }
      LOG.info("Added dimensions {}, removed {}", dimensionsToAdd, dimensionsToRemove);
      DAO_REGISTRY.getDatasetConfigDAO().update(datasetConfig);
    }
  }

  private void checkMetricChanges(String dataset, DatasetConfigDTO datasetConfig, Schema schema) {

    LOG.info("Checking for metric changes in {}", dataset);
    List<MetricFieldSpec> schemaMetricSpecs = schema.getMetricFieldSpecs();
    List<MetricConfigDTO> datasetMetricConfigs = DAO_REGISTRY.getMetricConfigDAO().findByDataset(dataset);
    List<String> datasetMetricNames = new ArrayList<>();
    for (MetricConfigDTO metricConfig : datasetMetricConfigs) {
      datasetMetricNames.add(metricConfig.getName());
    }
    List<Long> metricsToAdd = new ArrayList<>();

    for (MetricFieldSpec metricSpec : schemaMetricSpecs) {
      // metrics which are new in pinot schema, create them
      String metricName = metricSpec.getName();
      if (!datasetMetricNames.contains(metricName)) {
        MetricConfigDTO metricConfigDTO = ConfigGenerator.generateMetricConfig(metricSpec, dataset);
        LOG.info("Creating metric {} for {}", metricName, dataset);
        metricsToAdd.add(DAO_REGISTRY.getMetricConfigDAO().save(metricConfigDTO));
      }
    }

    // add new metricIds to default dashboard
    if (CollectionUtils.isNotEmpty(metricsToAdd)) {
      LOG.info("Metrics to add {}", metricsToAdd);
      String dashboardName = ThirdEyeUtils.getDefaultDashboardName(dataset);
      DashboardConfigDTO dashboardConfig = DAO_REGISTRY.getDashboardConfigDAO().findByName(dashboardName);
      List<Long> metricIds = dashboardConfig.getMetricIds();
      metricIds.addAll(metricsToAdd);
      DAO_REGISTRY.getDashboardConfigDAO().update(dashboardConfig);
    }

    // TODO: write a tool, which given a metric id, erases all traces of that metric from the database
    // This will include:
    // 1) delete the metric from metricConfigs
    // 2) remove any derived metrics which use the deleted metric
    // 3) remove the metric, and derived metrics from all dashboards
    // 4) remove any anomaly functions associated with the metric
    // 5) remove any alerts associated with these anomaly functions

  }

  /**
   * Reads all table names in pinot, and loads their schema
   * @throws IOException
   */
  private void loadDatasets() throws IOException {

    JsonNode tables = autoLoadPinotMetricsUtils.getAllTablesFromPinot();
    LOG.info("Getting all schemas");
    for (JsonNode table : tables) {
      String dataset = table.asText();
      Schema schema = autoLoadPinotMetricsUtils.getSchemaFromPinot(dataset);
      if (schema != null) {
        if (!autoLoadPinotMetricsUtils.verifySchemaCorrectness(schema)) {
          LOG.info("Skipping {} due to incorrect schema", dataset);
        } else {
          allDatasets.add(dataset);
          allSchemas.put(dataset, schema);
        }
      }
    }
  }

  // TODO: when all ingraph traces are cleaned, remove these checks for ingraphs
  private boolean isIngraphDataset(Schema schema) {
    boolean isIngraphDataset = false;
    if ((schema.getDimensionNames().contains(DEFAULT_INGRAPH_METRIC_NAMES_COLUMN)
        && schema.getMetricNames().contains(DEFAULT_INGRAPH_METRIC_VALUES_COLUMN))) {
      isIngraphDataset = true;
    }
    return isIngraphDataset;
  }

  private List<String> fetchMetricAsADimensionMetrics(String dataset, String metricNamesColumn) {
    List<String> distinctMetricNames = new ArrayList<>();
    ThirdEyeCacheRegistry CACHE_REGISTRY = ThirdEyeCacheRegistry.getInstance();
    String sql = String.format("select count(*) from %s group by %s top 10", dataset, metricNamesColumn);
    try {

      ResultSetGroup result = CACHE_REGISTRY.getResultSetGroupCache().get(new PinotQuery(sql, dataset));
      ResultSet resultSet = result.getResultSet(0);
      int rowCount = resultSet.getRowCount();
      for (int i = 0; i < rowCount; i++) {
        String dimensionName = resultSet.getGroupKeyString(i, 0);
        distinctMetricNames.add(dimensionName);
      }

      LOG.info("Distinct Metrics {}", distinctMetricNames);
    } catch (Exception e) {
      LOG.error("Exception in fetching metrics from pinot", e);
    }
    return distinctMetricNames;
  }

  private void checkMetricAsDimensionDataset(DatasetConfigDTO datasetConfigDTO, Schema schema) {
    String dataset = datasetConfigDTO.getDataset();
    String metricNamesColumn = datasetConfigDTO.getMetricNamesColumn();
    String metricValuesColumn = datasetConfigDTO.getMetricValuesColumn();
    FieldSpec metricValuesColumnFieldSpec = schema.getFieldSpecFor(metricValuesColumn);
    String dashboardName = ThirdEyeUtils.getDefaultDashboardName(dataset);

    // remove metricNamesColumn from dimensions if exists
    List<String> dimensions = datasetConfigDTO.getDimensions();
    if (dimensions.contains(metricNamesColumn)) {
      dimensions.removeAll(Lists.newArrayList(metricNamesColumn));
      datasetConfigDTO.setDimensions(dimensions);
      DAO_REGISTRY.getDatasetConfigDAO().update(datasetConfigDTO);
    }

    // remove metricValuesColumn from metrics if exists
    MetricConfigDTO metricConfigDTO = DAO_REGISTRY.getMetricConfigDAO().findByMetricAndDataset(metricValuesColumn, dataset);

    if (metricConfigDTO != null) {
      Long metricId = metricConfigDTO.getId();
      DAO_REGISTRY.getMetricConfigDAO().delete(metricConfigDTO);

      // remove metricValuesColumn id from default dashboard
      DashboardConfigDTO dashboardConfig = DAO_REGISTRY.getDashboardConfigDAO().findByName(dashboardName);
      List<Long> dashboardMetricIds = dashboardConfig.getMetricIds();
      dashboardMetricIds.removeAll(Lists.newArrayList(metricId));
      LOG.info("Updating dashboard config for {}", dashboardName);
      DAO_REGISTRY.getDashboardConfigDAO().update(dashboardConfig);
    }

    if (datasetConfigDTO.isAutoDiscoverMetrics()) {

      // query pinot to fetch distinct metricNamesColumn
      List<String> allDistinctMetricNames = fetchMetricAsADimensionMetrics(dataset, metricNamesColumn);

      // create metrics for these metric names, if they dont exist
      List<MetricConfigDTO> existingMetricConfigs = DAO_REGISTRY.getMetricConfigDAO().findByDataset(dataset);
      List<String> existingMetricNames = Lists.newArrayList();
      for (MetricConfigDTO existingMetricConfig : existingMetricConfigs) {
        existingMetricNames.add(existingMetricConfig.getName());
      }
      allDistinctMetricNames.removeAll(existingMetricNames);
      for (String metricName : allDistinctMetricNames) {
        LOG.info("Creating metric config for {}", metricName);
        MetricFieldSpec metricFieldSpec = new MetricFieldSpec(metricName, metricValuesColumnFieldSpec.getDataType());
        MetricConfigDTO metricConfig = ConfigGenerator.generateMetricConfig(metricFieldSpec, dataset);
        DAO_REGISTRY.getMetricConfigDAO().save(metricConfig);
      }

      // Add metrics to default dashboard
      List<Long> allMetricIds = ConfigGenerator.getMetricIdsFromMetricConfigs(DAO_REGISTRY.getMetricConfigDAO().findByDataset(dataset));
      DashboardConfigDTO dashboardConfig = DAO_REGISTRY.getDashboardConfigDAO().findByName(dashboardName);
      dashboardConfig.setMetricIds(allMetricIds);
      LOG.info("Creating dashboard config for {}", dashboardName);
      DAO_REGISTRY.getDashboardConfigDAO().update(dashboardConfig);
    }
  }

}
