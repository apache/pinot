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

package org.apache.pinot.thirdeye.detection.finetune;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.thirdeye.anomalydetection.context.AnomalyFeedback;
import org.apache.pinot.thirdeye.constant.AnomalyFeedbackType;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeDataSource;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.datasource.csv.CSVThirdEyeDataSource;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class GridSearchTuningAlgorithmTest {
  private DAOTestBase testDAOProvider;

  private TuningAlgorithm gridSearch;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @BeforeTest
  public void beforeMethod() throws JsonProcessingException {
    testDAOProvider = DAOTestBase.getInstance();

    // metric set up
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    MetricConfigDTO metric = new MetricConfigDTO();
    metric.setName("test");
    metric.setDataset("testDataSet");
    metric.setAlias("alias");
    metricDAO.save(metric);

    // data set set up
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    DatasetConfigDTO datasetDTO = new DatasetConfigDTO();
    datasetDTO.setDataset("testDataSet");
    datasetDTO.setDataSource("myDataSource");
    datasetDTO.setNonAdditiveBucketSize(1);
    datasetDTO.setTimeUnit(TimeUnit.MINUTES);
    datasetDAO.save(datasetDTO);

    // datasource set up
    DataFrame data = new DataFrame();
    data.addSeries("timestamp", 1526414678000L, 1527019478000L);
    data.addSeries("value", 100, 200);
    Map<String, DataFrame> datasets = new HashMap<>();
    datasets.put("testDataSet", data);

    Map<Long, String> id2name = new HashMap<>();
    id2name.put(1L, "value");

    Map<String, ThirdEyeDataSource> dataSourceMap = new HashMap<>();

    dataSourceMap.put("myDataSource", CSVThirdEyeDataSource.fromDataFrame(datasets, id2name));
    QueryCache cache = new QueryCache(dataSourceMap, Executors.newSingleThreadExecutor());
    ThirdEyeCacheRegistry.getInstance().registerQueryCache(cache);
    ThirdEyeCacheRegistry.initMetaDataCaches();

    // existing anomaly set up
    MergedAnomalyResultManager anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setStartTime(1525241940000L);
    anomaly.setEndTime(1525241940001L);
    AnomalyFeedback feedback = new AnomalyFeedbackDTO();
    feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY);
    anomaly.setFeedback(feedback);
    anomalyDAO.save(anomaly);

    // properties
    LinkedHashMap<String, Object> properties = new LinkedHashMap<>();
    properties.put("metricUrn", "thirdeye:metric:1");
    properties.put("className", "org.apache.pinot.thirdeye.detection.algorithm.BaselineAlgorithm");
    properties.put("change", 0.1);

    // parameters
    LinkedHashMap<String, List<Number>> parameters = new LinkedHashMap<>();
    parameters.put("$.change", Arrays.<Number>asList(0.05, 0.1));

    gridSearch = new GridSearchTuningAlgorithm(OBJECT_MAPPER.writeValueAsString(properties), parameters);
  }

  @AfterSuite(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testGridSearch() throws Exception {
    AnomalySlice slice = new AnomalySlice().withStart(1525211842000L).withEnd(1527890242000L);
    gridSearch.fit(slice, -1);
    DetectionConfigDTO config = gridSearch.bestDetectionConfig();
    Assert.assertEquals(MapUtils.getDouble(config.getProperties(), "change"), 0.05);
  }

  // TODO test dimension separation
}
