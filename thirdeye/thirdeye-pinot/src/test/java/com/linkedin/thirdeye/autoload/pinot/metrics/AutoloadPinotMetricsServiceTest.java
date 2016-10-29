package com.linkedin.thirdeye.autoload.pinot.metrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.thirdeye.autoload.pinot.metrics.AutoLoadPinotMetricsService;
import com.linkedin.thirdeye.autoload.pinot.metrics.ConfigGenerator;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.AbstractManagerTestBase;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.IngraphMetricConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DashboardConfigBean;

public class AutoloadPinotMetricsServiceTest  extends AbstractManagerTestBase {

  private AutoLoadPinotMetricsService testAutoLoadPinotMetricsService;
  private String dataset = "test-collection";
  private String ingraphDataset = "thirdeye-ingraph-metric";
  private Schema schema;
  private Schema ingraphSchema;
  DatasetConfigDTO datasetConfig = null;
  DatasetConfigDTO ingraphDatasetConfig = null;


  private void setup() throws IOException {
    DAORegistry.registerDAOs(anomalyFunctionDAO, emailConfigurationDAO, rawResultDAO, mergedResultDAO,
        jobDAO, taskDAO, datasetConfigDAO, metricConfigDAO, dashboardConfigDAO, ingraphMetricConfigDAO);
    testAutoLoadPinotMetricsService = new AutoLoadPinotMetricsService();
    schema = Schema.fromInputSteam(ClassLoader.getSystemResourceAsStream("sample-pinot-schema.json"));
    ingraphSchema = Schema.fromInputSteam(ClassLoader.getSystemResourceAsStream("thirdeye_ingraph_metric.json"));
  }


  @Test
  public void testAddNewDataset() throws Exception {
    setup();

    testAutoLoadPinotMetricsService.addPinotDataset(dataset, schema, datasetConfig);

    Assert.assertEquals(datasetConfigDAO.findAll().size(), 1);
    datasetConfig = datasetConfigDAO.findByDataset(dataset);
    Assert.assertEquals(datasetConfig.getDataset(), dataset);
    Assert.assertEquals(datasetConfig.getDimensions(), schema.getDimensionNames());
    Assert.assertEquals(datasetConfig.getTimeColumn(), schema.getTimeColumnName());
    TimeGranularitySpec timeGranularitySpec = schema.getTimeFieldSpec().getOutgoingGranularitySpec();
    Assert.assertEquals(datasetConfig.getTimeUnit(), timeGranularitySpec.getTimeType());
    Assert.assertEquals(datasetConfig.getTimeDuration(), new Integer(timeGranularitySpec.getTimeUnitSize()));
    Assert.assertEquals(datasetConfig.getTimeFormat(), timeGranularitySpec.getTimeFormat());
    Assert.assertEquals(datasetConfig.getTimezone(), "UTC");

    List<MetricConfigDTO> metricConfigs = metricConfigDAO.findByDataset(dataset);
    List<String> schemaMetricNames = schema.getMetricNames();
    List<Long> metricIds = new ArrayList<>();
    Assert.assertEquals(metricConfigs.size(), schemaMetricNames.size());
    for (MetricConfigDTO metricConfig : metricConfigs) {
      Assert.assertTrue(schemaMetricNames.contains(metricConfig.getName()));
      metricIds.add(metricConfig.getId());
    }

    DashboardConfigDTO dashboardConfig = dashboardConfigDAO.
        findByName(DashboardConfigBean.DEFAULT_DASHBOARD_PREFIX + dataset);
    Assert.assertEquals(dashboardConfig.getMetricIds(), metricIds);

  }

  @Test(dependsOnMethods = {"testAddNewDataset"})
  public void testRefreshDataset() throws Exception {
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("newDimension", DataType.STRING, true);
    schema.addField(dimensionFieldSpec);
    testAutoLoadPinotMetricsService.addPinotDataset(dataset, schema, datasetConfig);
    Assert.assertEquals(datasetConfigDAO.findAll().size(), 1);
    DatasetConfigDTO newDatasetConfig1 = datasetConfigDAO.findByDataset(dataset);
    Assert.assertEquals(newDatasetConfig1.getDataset(), dataset);
    Assert.assertEquals(Sets.newHashSet(newDatasetConfig1.getDimensions()), Sets.newHashSet(schema.getDimensionNames()));

    MetricFieldSpec metricFieldSpec = new MetricFieldSpec("newMetric", DataType.LONG);
    schema.addField(metricFieldSpec);
    testAutoLoadPinotMetricsService.addPinotDataset(dataset, schema, newDatasetConfig1);

    Assert.assertEquals(datasetConfigDAO.findAll().size(), 1);
    List<MetricConfigDTO> metricConfigs = metricConfigDAO.findByDataset(dataset);
    List<String> schemaMetricNames = schema.getMetricNames();
    List<Long> metricIds = new ArrayList<>();
    Assert.assertEquals(metricConfigs.size(), schemaMetricNames.size());
    for (MetricConfigDTO metricConfig : metricConfigs) {
      Assert.assertTrue(schemaMetricNames.contains(metricConfig.getName()));
      metricIds.add(metricConfig.getId());
    }

    DashboardConfigDTO dashboardConfig = dashboardConfigDAO.
        findByName(DashboardConfigBean.DEFAULT_DASHBOARD_PREFIX + dataset);
    Assert.assertEquals(dashboardConfig.getMetricIds(), metricIds);

  }

  @Test(dependsOnMethods = {"testRefreshDataset"})
  public void  testAddNewIngraphDataset() throws Exception {
    // add dataset to ingraph table
    IngraphMetricConfigDTO ingraphMetricConfig1 = new IngraphMetricConfigDTO();
    ingraphMetricConfig1.setDataset(ingraphDataset);
    ingraphMetricConfig1.setMetric("m1");
    ingraphMetricConfig1.setMetricAlias("m1");
    ingraphMetricConfig1.setMetricDataType("FLOAT");
    ingraphMetricConfigDAO.save(ingraphMetricConfig1);

    IngraphMetricConfigDTO ingraphMetricConfig2 = new IngraphMetricConfigDTO();
    ingraphMetricConfig2.setDataset(ingraphDataset);
    ingraphMetricConfig2.setMetric("m2");
    ingraphMetricConfig2.setMetricAlias("m2");
    ingraphMetricConfig2.setMetricDataType("FLOAT");
    ingraphMetricConfigDAO.save(ingraphMetricConfig2);

    // test isIngraphDataset
    boolean isIngraphDataset = ConfigGenerator.isIngraphDataset(ingraphSchema);
    Assert.assertTrue(isIngraphDataset);

    // addIngraphDataset
    testAutoLoadPinotMetricsService.addIngraphDataset(ingraphDataset, ingraphSchema, ingraphDatasetConfig);

    // test dimensions, metrics, dashboard
    Assert.assertEquals(datasetConfigDAO.findAll().size(), 2);
    ingraphDatasetConfig = datasetConfigDAO.findByDataset(ingraphDataset);
    Assert.assertEquals(ingraphDatasetConfig.getDataset(), ingraphDataset);
    Assert.assertEquals(ingraphDatasetConfig.getDimensions(), ingraphSchema.getDimensionNames());
    Assert.assertEquals(ingraphDatasetConfig.getTimeColumn(), ingraphSchema.getTimeColumnName());
    TimeGranularitySpec timeGranularitySpec = ingraphSchema.getTimeFieldSpec().getOutgoingGranularitySpec();
    Assert.assertEquals(ingraphDatasetConfig.getTimeUnit(), timeGranularitySpec.getTimeType());
    Assert.assertEquals(ingraphDatasetConfig.getTimeDuration(), new Integer(timeGranularitySpec.getTimeUnitSize()));
    Assert.assertEquals(ingraphDatasetConfig.getTimeFormat(), timeGranularitySpec.getTimeFormat());
    Assert.assertEquals(ingraphDatasetConfig.getTimezone(), "UTC");

    List<MetricConfigDTO> ingraphMetricConfigs = metricConfigDAO.findByDataset(ingraphDataset);
    List<Long> metricIds = new ArrayList<>();
    Assert.assertEquals(ingraphMetricConfigs.size(), 2);
    for (MetricConfigDTO metricConfig : ingraphMetricConfigs) {
      Assert.assertTrue(metricConfig.getName().equals("m1") || metricConfig.getName().equals("m2"));
      metricIds.add(metricConfig.getId());
    }

    DashboardConfigDTO dashboardConfig = dashboardConfigDAO.
        findByName(DashboardConfigBean.DEFAULT_DASHBOARD_PREFIX + ingraphDataset);
    Assert.assertEquals(dashboardConfig.getMetricIds(), metricIds);

  }

}
