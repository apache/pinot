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

package com.linkedin.thirdeye.auto.onboard;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean.DimensionAsMetricProperties;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.MetadataSourceConfig;
import com.linkedin.thirdeye.datasource.pinot.PinotThirdEyeDataSource;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a service to onboard datasets automatically to thirdeye from pinot
 * The run method is invoked periodically by the AutoOnboardService, and it checks for new tables in pinot, to add to thirdeye
 * It also looks for any changes in dimensions or metrics to the existing tables
 */
public class AutoOnboardPinotMetadataSource extends AutoOnboard {
  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardPinotMetadataSource.class);

  private static final Set<String> DIMENSION_SUFFIX_BLACKLIST = new HashSet<>(Arrays.asList("_topk", "_approximate", "_tDigest"));

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private final AlertConfigManager alertDAO;
  private final DatasetConfigManager datasetDAO;
  private final MetricConfigManager metricDAO;

  private AutoOnboardPinotMetricsUtils autoLoadPinotMetricsUtils;

  public AutoOnboardPinotMetadataSource(MetadataSourceConfig metadataSourceConfig)
      throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
    super(metadataSourceConfig);
    try {
      autoLoadPinotMetricsUtils = new AutoOnboardPinotMetricsUtils(metadataSourceConfig);
      LOG.info("Created {}", AutoOnboardPinotMetadataSource.class.getName());
    } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
      throw e;
    }
    this.datasetDAO = DAO_REGISTRY.getDatasetConfigDAO();
    this.metricDAO = DAO_REGISTRY.getMetricConfigDAO();
    this.alertDAO = DAO_REGISTRY.getAlertConfigDAO();
  }

  public AutoOnboardPinotMetadataSource(MetadataSourceConfig metadataSourceConfig, AutoOnboardPinotMetricsUtils utils) {
    super(metadataSourceConfig);
    autoLoadPinotMetricsUtils = utils;
    this.datasetDAO = DAO_REGISTRY.getDatasetConfigDAO();
    this.metricDAO = DAO_REGISTRY.getMetricConfigDAO();
    this.alertDAO = DAO_REGISTRY.getAlertConfigDAO();
  }

  public void run() {
    try {
      List<String> allDatasets = new ArrayList<>();
      Map<String, Schema> allSchemas = new HashMap<>();
      Map<String, Map<String, String>> allCustomConfigs = new HashMap<>();
      loadDatasets(allDatasets, allSchemas, allCustomConfigs);
      LOG.info("Checking all datasets");
      removeDeletedDataset(allDatasets);
      for (String dataset : allDatasets) {
        LOG.info("Checking dataset {}", dataset);

        Schema schema = allSchemas.get(dataset);
        Map<String, String> customConfigs = allCustomConfigs.get(dataset);
        DatasetConfigDTO datasetConfig = datasetDAO.findByDataset(dataset);
        addPinotDataset(dataset, schema, customConfigs, datasetConfig);
      }
    } catch (Exception e) {
      LOG.error("Exception in loading datasets", e);
    }
  }

  void removeDeletedDataset(List<String> allDatasets) {
    LOG.info("Removing deleted Pinot datasets");
    List<DatasetConfigDTO> allExistingDataset = this.datasetDAO.findAll();
    Set<String> datasets = new HashSet<>(allDatasets);

    Collection<DatasetConfigDTO> filtered = Collections2.filter(allExistingDataset, new com.google.common.base.Predicate<DatasetConfigDTO>() {
      @Override
      public boolean apply(@Nullable DatasetConfigDTO datasetConfigDTO) {
        return datasetConfigDTO.getDataSource().equals(PinotThirdEyeDataSource.DATA_SOURCE_NAME);
      }
    });

    for (DatasetConfigDTO datasetConfigDTO : filtered) {
      if (shouldRemoveDataset(datasetConfigDTO, datasets)) {
        LOG.info("Deleting pinot dataset '{}'", datasetConfigDTO.getDataset());
        datasetDAO.deleteByPredicate(Predicate.EQ("dataset", datasetConfigDTO.getDataset()));
        deleteAutoCreatedAlertGroup(datasetConfigDTO);
      }
    }
  }

  private void deleteAutoCreatedAlertGroup(DatasetConfigDTO datasetConfigDTO) {
    String alertGroupName = AutoOnboardUtility.getAutoAlertGroupName(datasetConfigDTO.getDataset());
    AlertConfigDTO alertGroupDTO = alertDAO.findWhereNameEquals(alertGroupName);
    if (alertGroupDTO != null) {
      alertDAO.deleteById(alertGroupDTO.getId());
      LOG.info("Deleting auto created alert group {} associated with pinot dataset '{}'", alertGroupDTO.getName(),
          datasetConfigDTO.getDataset());
    }
  }

  private boolean shouldRemoveDataset(DatasetConfigDTO datasetConfigDTO, Set<String> datasets) {
    if (!datasets.contains(datasetConfigDTO.getDataset())) {
      List<MetricConfigDTO> metrics = metricDAO.findByDataset(datasetConfigDTO.getDataset());
      int metricCount = metrics.size();
      for (MetricConfigDTO metric : metrics) {
        if (!metric.isDerived()) {
          metricDAO.delete(metric);
          metricCount--;
        }
      }
      return metricCount == 0;
    } else {
      return false;
    }
  }


  /**
   * Adds a dataset to the thirdeye database
   * @param dataset
   * @param schema
   * @param datasetConfig
   */
  public void addPinotDataset(String dataset, Schema schema, Map<String, String> customConfigs,
      DatasetConfigDTO datasetConfig) throws Exception {
    if (datasetConfig == null) {
      LOG.info("Dataset {} is new, adding it to thirdeye", dataset);
      addNewDataset(dataset, schema, customConfigs);
    } else {
      LOG.info("Dataset {} already exists, checking for updates", dataset);
      refreshOldDataset(dataset, schema, customConfigs, datasetConfig);
    }
  }

  /**
   * Adds a new dataset to the thirdeye database
   * @param dataset
   * @param schema
   */
  private void addNewDataset(String dataset, Schema schema, Map<String, String> customConfigs) throws Exception {
    List<MetricFieldSpec> metricSpecs = schema.getMetricFieldSpecs();

    // Create DatasetConfig
    DatasetConfigDTO datasetConfigDTO = ConfigGenerator.generateDatasetConfig(dataset, schema, customConfigs);
    LOG.info("Creating dataset for {}", dataset);
    this.datasetDAO.save(datasetConfigDTO);

    // Create MetricConfig
    for (MetricFieldSpec metricFieldSpec : metricSpecs) {
      MetricConfigDTO metricConfigDTO = ConfigGenerator.generateMetricConfig(metricFieldSpec, dataset);
      LOG.info("Creating metric {} for {}", metricConfigDTO.getName(), dataset);
      this.metricDAO.save(metricConfigDTO);
    }
  }

  /**
   * Refreshes an existing dataset in the thirdeye database
   * with any dimension/metric changes from pinot schema
   * @param dataset
   * @param schema
   * @param datasetConfig
   */
  private void refreshOldDataset(String dataset, Schema schema, Map<String, String> customConfigs,
      DatasetConfigDTO datasetConfig) throws Exception {
    checkDimensionChanges(dataset, datasetConfig, schema);
    checkMetricChanges(dataset, datasetConfig, schema);
    checkTimeFieldChanges(datasetConfig, schema);
    appendNewCustomConfigs(datasetConfig, customConfigs);
  }

  private void checkDimensionChanges(String dataset, DatasetConfigDTO datasetConfig, Schema schema) {
    LOG.info("Checking for dimensions changes in {}", dataset);
    List<String> schemaDimensions = schema.getDimensionNames();
    List<String> datasetDimensions = datasetConfig.getDimensions();

    // remove blacklisted dimensions
    Iterator<String> itDimension = schemaDimensions.iterator();
    while (itDimension.hasNext()) {
      String dimName = itDimension.next();
      for (String suffix : DIMENSION_SUFFIX_BLACKLIST) {
        if (dimName.endsWith(suffix)) {
          itDimension.remove();
          break;
        }
      }
    }

    // in dimensionAsMetric case, the dimension name will be used in the METRIC_NAMES_COLUMNS property of the metric
    List<String> dimensionsAsMetrics = new ArrayList<>();
    List<MetricConfigDTO> metricConfigs = DAO_REGISTRY.getMetricConfigDAO().findByDataset(dataset);
    for (MetricConfigDTO metricConfig : metricConfigs) {
      if (metricConfig.isDimensionAsMetric()) {
        Map<String, String> metricProperties = metricConfig.getMetricProperties();
        if (MapUtils.isNotEmpty(metricProperties)) {
          String metricNames = metricProperties.get(DimensionAsMetricProperties.METRIC_NAMES_COLUMNS.toString());
          if (StringUtils.isNotBlank(metricNames)) {
            dimensionsAsMetrics.addAll(Lists.newArrayList(metricNames.split(MetricConfigBean.METRIC_PROPERTIES_SEPARATOR)));
          }
        }
      }
    }

    // create diff
    List<String> dimensionsToAdd = new ArrayList<>();
    List<String> dimensionsToRemove = new ArrayList<>();

    // dimensions which are new in the pinot schema
    for (String dimensionName : schemaDimensions) {
      if (!datasetDimensions.contains(dimensionName) && !dimensionsAsMetrics.contains(dimensionName)) {
        dimensionsToAdd.add(dimensionName);
      }
    }

    // dimensions which are removed from pinot schema
    for (String dimensionName : datasetDimensions) {
      if (!schemaDimensions.contains(dimensionName)) {
        dimensionsToRemove.add(dimensionName);
      }
    }

    // apply diff
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

    // Fetch metrics from Thirdeye
    List<MetricConfigDTO> datasetMetricConfigs = DAO_REGISTRY.getMetricConfigDAO().findByDataset(dataset);

    // Fetch metrics from Pinot
    List<MetricFieldSpec> schemaMetricSpecs = schema.getMetricFieldSpecs();

    // Index metric names
    Set<String> datasetMetricNames = new HashSet<>();
    for (MetricConfigDTO metricConfig : datasetMetricConfigs) {
      datasetMetricNames.add(getColumnName(metricConfig));
    }

    Set<String> schemaMetricNames = new HashSet<>();
    for (MetricFieldSpec metricSpec : schemaMetricSpecs) {
      schemaMetricNames.add(metricSpec.getName());
    }

    // add new metrics to ThirdEye
    for (MetricFieldSpec metricSpec : schemaMetricSpecs) {
      if (!datasetMetricNames.contains(metricSpec.getName())) {
        MetricConfigDTO metricConfigDTO = ConfigGenerator.generateMetricConfig(metricSpec, dataset);
        LOG.info("Creating metric {} in {}", metricSpec.getName(), dataset);
        DAO_REGISTRY.getMetricConfigDAO().save(metricConfigDTO);
      }
    }

    // remove deleted metrics from ThirdEye
    for (MetricConfigDTO metricConfig : datasetMetricConfigs) {
      if (!metricConfig.isDerived()) {
        if (!schemaMetricNames.contains(getColumnName(metricConfig))) {
          LOG.info("Deleting metric {} in {}", metricConfig.getName(), dataset);
          DAO_REGISTRY.getMetricConfigDAO().delete(metricConfig);
        }
      }
    }

    // TODO: write a tool, which given a metric id, erases all traces of that metric from the database
    // This will include:
    // 1) delete the metric from metricConfigs
    // 2) remove any derived metrics which use the deleted metric
    // 3) remove the metric, and derived metrics from all dashboards
    // 4) remove any anomaly functions associated with the metric
    // 5) remove any alerts associated with these anomaly functions

  }

  private void checkTimeFieldChanges(DatasetConfigDTO datasetConfig, Schema schema) {
    TimeGranularitySpec timeSpec = schema.getTimeFieldSpec().getOutgoingGranularitySpec();
    if (!datasetConfig.getTimeColumn().equals(timeSpec.getName())
        || !datasetConfig.getTimeFormat().equals(timeSpec.getTimeFormat())
        || datasetConfig.bucketTimeGranularity().getUnit() != timeSpec.getTimeType()
        || datasetConfig.bucketTimeGranularity().getSize() != timeSpec.getTimeUnitSize()) {
      ConfigGenerator.setTimeSpecs(datasetConfig, timeSpec);
      DAO_REGISTRY.getDatasetConfigDAO().update(datasetConfig);
      LOG.info("Refreshed time field. name = {}, format = {}, type = {}, unit size = {}.",
          timeSpec.getName(), timeSpec.getTimeType(), timeSpec.getTimeType(), timeSpec.getTimeUnitSize());
    }
  }

  /**
   * This method ensures that the given custom configs exist in the dataset config and their value are the same.
   *
   * @param datasetConfig the current dataset config to be appended with new custom config.
   * @param customConfigs the custom config to be matched with that from dataset config.
   *
   * TODO: Remove out-of-date Pinot custom config from dataset config.
   */
  private void appendNewCustomConfigs(DatasetConfigDTO datasetConfig, Map<String, String> customConfigs) {
    if (MapUtils.isNotEmpty(customConfigs)) {
      Map<String, String> properties = datasetConfig.getProperties();
      boolean hasUpdate = false;
      if (MapUtils.isEmpty(properties)) {
        properties = customConfigs;
        hasUpdate = true;
      } else {
        for (Map.Entry<String, String> customConfig : customConfigs.entrySet()) {
          String configKey = customConfig.getKey();
          String configValue = customConfig.getValue();

          if (!properties.containsKey(configKey)) {
            properties.put(configKey, configValue);
            hasUpdate = true;
          }
        }
      }
      if (hasUpdate) {
        datasetConfig.setProperties(properties);
        DAO_REGISTRY.getDatasetConfigDAO().update(datasetConfig);
      }
    }
  }

  /**
   * Reads all table names in pinot, and loads their schema
   * @param allSchemas
   * @param allDatasets
   * @throws IOException
   */
  private void loadDatasets(List<String> allDatasets, Map<String, Schema> allSchemas,
      Map<String, Map<String, String>> allCustomConfigs) throws IOException {

    JsonNode tables = autoLoadPinotMetricsUtils.getAllTablesFromPinot();
    LOG.info("Getting all schemas");
    for (JsonNode table : tables) {
      String dataset = table.asText();
      Map<String, String> pinotCustomProperty = autoLoadPinotMetricsUtils.getCustomConfigsFromPinotEndpoint(dataset);
      Schema schema = autoLoadPinotMetricsUtils.getSchemaFromPinot(dataset);
      if (schema != null) {
        if (!autoLoadPinotMetricsUtils.verifySchemaCorrectness(schema)) {
          LOG.info("Skipping {} due to incorrect schema", dataset);
        } else {
          allDatasets.add(dataset);
          allSchemas.put(dataset, schema);
          allCustomConfigs.put(dataset, pinotCustomProperty);
        }
      }
    }
  }

  /**
   * Returns the metric column name
   *
   * @param metricConfig metric config
   * @return column name
   */
  private static String getColumnName(MetricConfigDTO metricConfig) {
    // In dimensionAsMetric case, the metric name will be used in the METRIC_VALUES_COLUMN property of the metric
    if (metricConfig.isDimensionAsMetric()) {
      Map<String, String> metricProperties = metricConfig.getMetricProperties();
      if (MapUtils.isNotEmpty(metricProperties)) {
        return metricProperties.get(DimensionAsMetricProperties.METRIC_VALUES_COLUMN.toString());
      }
    } else {
      return metricConfig.getName();
    }
    throw new IllegalArgumentException(String.format("Could not resolve column name for '%s'", metricConfig));
  }

  @Override
  public void runAdhoc() {
    LOG.info("Triggering adhoc run for AutoOnboard Pinot data source");
    run();
  }

}
