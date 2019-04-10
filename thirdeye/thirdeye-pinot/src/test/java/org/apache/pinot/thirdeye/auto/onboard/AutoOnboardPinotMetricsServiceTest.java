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

package org.apache.pinot.thirdeye.auto.onboard;

import com.google.common.collect.Sets;
import org.apache.pinot.common.data.DimensionFieldSpec;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.common.data.MetricFieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.data.TimeGranularitySpec;
import org.apache.pinot.thirdeye.common.metric.MetricType;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.MetricConfigBean;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AutoOnboardPinotMetricsServiceTest {

  private AutoOnboardPinotMetadataSource testAutoLoadPinotMetricsService;
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
    testAutoLoadPinotMetricsService = new AutoOnboardPinotMetadataSource(null, null);
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
    Assert.assertEquals(datasetConfig.getTimezone(), "US/Pacific");
    Assert.assertEquals(datasetConfig.getExpectedDelay().getUnit(), TimeUnit.HOURS);

    List<MetricConfigDTO> metricConfigs = metricConfigDAO.findByDataset(dataset);
    List<String> schemaMetricNames = schema.getMetricNames();
    List<Long> metricIds = new ArrayList<>();
    Assert.assertEquals(metricConfigs.size(), schemaMetricNames.size());
    for (MetricConfigDTO metricConfig : metricConfigs) {
      Assert.assertTrue(schemaMetricNames.contains(metricConfig.getName()));
      metricIds.add(metricConfig.getId());
      if (metricConfig.getName().equals("latency_tdigest")) {
        Assert.assertEquals(metricConfig.getDefaultAggFunction(), MetricConfigBean.DEFAULT_TDIGEST_AGG_FUNCTION);
        Assert.assertEquals(metricConfig.getDatatype(), MetricType.DOUBLE);
      } else {
        Assert.assertEquals(metricConfig.getDefaultAggFunction(), MetricConfigBean.DEFAULT_AGG_FUNCTION);
      }
    }
  }

  @Test (dependsOnMethods={"testAddNewDataset"})
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

    TimeFieldSpec timeFieldSpec = new TimeFieldSpec("timestampInEpoch", DataType.LONG, TimeUnit.MILLISECONDS);
    schema.removeField(schema.getTimeColumnName());
    schema.addField(timeFieldSpec);
    testAutoLoadPinotMetricsService.addPinotDataset(dataset, schema, new HashMap<>(pinotCustomConfigs), newDatasetConfig1);
    Assert.assertEquals(datasetConfigDAO.findAll().size(), 1);
    datasetConfig = datasetConfigDAO.findByDataset(dataset);
    TimeGranularitySpec timeGranularitySpec = schema.getTimeFieldSpec().getOutgoingGranularitySpec();
    Assert.assertEquals(datasetConfig.bucketTimeGranularity().getUnit(), TimeUnit.MINUTES);
    Assert.assertEquals(datasetConfig.bucketTimeGranularity().getSize(), 5);
    Assert.assertEquals(datasetConfig.getTimeUnit(), timeGranularitySpec.getTimeType());
    Assert.assertEquals(datasetConfig.getTimeDuration().intValue(), timeGranularitySpec.getTimeUnitSize());
    Assert.assertEquals(datasetConfig.getTimeFormat(), timeGranularitySpec.getTimeFormat());
    Assert.assertEquals(datasetConfig.getTimezone(), "US/Pacific");
    Assert.assertEquals(datasetConfig.getExpectedDelay().getUnit(), TimeUnit.HOURS);
  }

  @Test (dependsOnMethods={"testRefreshDataset"})
  public void testRemoveDataset() throws Exception {
    Assert.assertEquals(datasetConfigDAO.findAll().size(), 1);
    testAutoLoadPinotMetricsService.removeDeletedDataset(Collections.<String>emptyList());
    Assert.assertEquals(datasetConfigDAO.findAll().size(), 0);
  }
}
