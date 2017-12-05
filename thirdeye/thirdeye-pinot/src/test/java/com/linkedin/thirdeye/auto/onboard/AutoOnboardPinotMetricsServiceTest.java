package com.linkedin.thirdeye.auto.onboard;

import com.google.common.collect.Sets;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AutoOnboardPinotMetricsServiceTest {

  private AutoOnboardPinotDataSource testAutoLoadPinotMetricsService;
  private String dataset = "test-collection";
  private Schema schema;

  private DAOTestBase testDAOProvider;
  private DatasetConfigManager datasetConfigDAO;
  private MetricConfigManager metricConfigDAO;

  @BeforeMethod
  void beforeMethod() throws Exception {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    datasetConfigDAO = daoRegistry.getDatasetConfigDAO();
    metricConfigDAO = daoRegistry.getMetricConfigDAO();
    testAutoLoadPinotMetricsService = new AutoOnboardPinotDataSource(null, null);
    schema = Schema.fromInputSteam(ClassLoader.getSystemResourceAsStream("sample-pinot-schema.json"));
    Map<String, String> pinotCustomConfigs = new HashMap<>();
    pinotCustomConfigs.put("configKey1", "configValue1");
    pinotCustomConfigs.put("configKey2", "configValue2");
    testAutoLoadPinotMetricsService.addPinotDataset(dataset, schema, pinotCustomConfigs, null);
  }

  @AfterMethod(alwaysRun = true)
  void afterMethod() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testAddNewDataset() throws Exception {
    Assert.assertEquals(datasetConfigDAO.findAll().size(), 1);
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(dataset);
    Assert.assertEquals(datasetConfig.getDataset(), dataset);
    Assert.assertEquals(datasetConfig.getDimensions(), schema.getDimensionNames());
    Assert.assertEquals(datasetConfig.getTimeColumn(), schema.getTimeColumnName());
    TimeGranularitySpec timeGranularitySpec = schema.getTimeFieldSpec().getOutgoingGranularitySpec();
    Assert.assertEquals(datasetConfig.bucketTimeGranularity().getUnit(), timeGranularitySpec.getTimeType());
    Assert.assertEquals(datasetConfig.bucketTimeGranularity().getSize(), timeGranularitySpec.getTimeUnitSize());
    Assert.assertEquals(datasetConfig.getTimeFormat(), timeGranularitySpec.getTimeFormat());
    Assert.assertEquals(datasetConfig.getTimezone(), "UTC");
    Assert.assertEquals(datasetConfig.getExpectedDelay().getUnit(), TimeUnit.HOURS);

    List<MetricConfigDTO> metricConfigs = metricConfigDAO.findByDataset(dataset);
    List<String> schemaMetricNames = schema.getMetricNames();
    List<Long> metricIds = new ArrayList<>();
    Assert.assertEquals(metricConfigs.size(), schemaMetricNames.size());
    for (MetricConfigDTO metricConfig : metricConfigs) {
      Assert.assertTrue(schemaMetricNames.contains(metricConfig.getName()));
      metricIds.add(metricConfig.getId());
    }
  }

  @Test
  public void testRefreshDataset() throws Exception {
    DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(dataset);
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("newDimension", DataType.STRING, true);
    schema.addField(dimensionFieldSpec);
    Map<String, String> pinotCustomConfigs = new HashMap<>();
    pinotCustomConfigs.put("configKey1", "configValue1");
    pinotCustomConfigs.put("configKey2", "configValue2");
    testAutoLoadPinotMetricsService.addPinotDataset(dataset, schema, new HashMap<>(pinotCustomConfigs), datasetConfig);
    Assert.assertEquals(datasetConfigDAO.findAll().size(), 1);
    DatasetConfigDTO newDatasetConfig1 = datasetConfigDAO.findByDataset(dataset);
    Assert.assertEquals(newDatasetConfig1.getDataset(), dataset);
    Assert.assertEquals(Sets.newHashSet(newDatasetConfig1.getDimensions()), Sets.newHashSet(schema.getDimensionNames()));
    Assert.assertEquals(newDatasetConfig1.getProperties(), pinotCustomConfigs);

    MetricFieldSpec metricFieldSpec = new MetricFieldSpec("newMetric", DataType.LONG);
    schema.addField(metricFieldSpec);
    pinotCustomConfigs.put("configKey3", "configValue3");
    pinotCustomConfigs.remove("configKey2");
    testAutoLoadPinotMetricsService.addPinotDataset(dataset, schema, new HashMap<>(pinotCustomConfigs), newDatasetConfig1);

    Assert.assertEquals(datasetConfigDAO.findAll().size(), 1);
    List<MetricConfigDTO> metricConfigs = metricConfigDAO.findByDataset(dataset);
    List<String> schemaMetricNames = schema.getMetricNames();
    List<Long> metricIds = new ArrayList<>();
    Assert.assertEquals(metricConfigs.size(), schemaMetricNames.size());
    for (MetricConfigDTO metricConfig : metricConfigs) {
      Assert.assertTrue(schemaMetricNames.contains(metricConfig.getName()));
      metricIds.add(metricConfig.getId());
    }

    // Get the updated dataset config and check custom configs
    datasetConfig = datasetConfigDAO.findByDataset(dataset);
    Map<String, String> datasetCustomConfigs = datasetConfig.getProperties();
    for (Map.Entry<String, String> pinotCustomCnofig : pinotCustomConfigs.entrySet()) {
      String configKey = pinotCustomCnofig.getKey();
      String configValue = pinotCustomCnofig.getValue();
      Assert.assertTrue(datasetCustomConfigs.containsKey(configKey));
      Assert.assertEquals(datasetCustomConfigs.get(configKey), configValue);
    }
  }
}
