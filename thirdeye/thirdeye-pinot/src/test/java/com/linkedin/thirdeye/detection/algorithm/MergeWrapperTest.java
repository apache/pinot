package com.linkedin.thirdeye.detection.algorithm;

import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineResult;
import com.linkedin.thirdeye.detection.MockDataProvider;
import com.linkedin.thirdeye.detection.MockDetectionPipeline;
import com.linkedin.thirdeye.detection.MockDetectionPipelineLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MergeWrapperTest {

  DetectionConfigDTO config;
  MergeWrapper wrapper;
  Map<String, Object> properties;
  DataProvider mockDataProvider;
  List<MockDetectionPipeline> runs;


  private static final Long PROP_ID_VALUE = 1000L;
  private static final String PROP_NAME_VALUE = "myName";

  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_CLASS_NAMES = "classNames";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_CLASS_NAME_VALUE = "MyClassName";
  private static final String PROP_PROPERTIES = "properties";
  private static final String PROP_TARGET = "target";
  private static final String PROP_TARGET_VALUE = "myMetricUrn";
  private static final String PROP_PROPERTIES_MAP = "propertiesMap";

  @BeforeMethod
  public void beforeMethod() {
    this.runs = new ArrayList<>();

    this.properties = new HashMap<>();
    this.properties.put(PROP_METRIC_URN, "thirdeye:metric:1");
    this.properties.put(PROP_CLASS_NAME, PROP_CLASS_NAME_VALUE);
    this.properties.put(PROP_TARGET, PROP_TARGET_VALUE);
    this.properties.put(PROP_PROPERTIES, Collections.singletonMap("key", "value"));
    this.properties.put(PROP_CLASS_NAMES, Collections.singletonList(PROP_CLASS_NAME_VALUE));

    Map<String, Map<String, Object>> nestedPropertiesMap = new HashMap<>();
    Map<String, Object> nestedProperties = new HashMap<>();
    nestedProperties.put(PROP_TARGET, PROP_TARGET_VALUE);
    nestedProperties.put(PROP_METRIC_URN, "thirdeye:metric:1");
    nestedPropertiesMap.put(PROP_CLASS_NAME_VALUE, nestedProperties);
    this.properties.put(PROP_PROPERTIES_MAP, nestedPropertiesMap);

    this.config = new DetectionConfigDTO();
    this.config.setId(PROP_ID_VALUE);
    this.config.setName(PROP_NAME_VALUE);
    this.config.setProperties(this.properties);

    MergedAnomalyResultDTO existingAnomalyResultDTO = new MergedAnomalyResultDTO();
    existingAnomalyResultDTO.setStartTime(1L);
    existingAnomalyResultDTO.setEndTime(5L);
    existingAnomalyResultDTO.setDetectionConfigId(PROP_ID_VALUE);
    existingAnomalyResultDTO.setParentCount(0);

    MockDetectionPipelineLoader pipelineLoader = new MockDetectionPipelineLoader(this.runs);

    MergedAnomalyResultDTO anomalyResultOne = new MergedAnomalyResultDTO();
    anomalyResultOne.setStartTime(2L);
    anomalyResultOne.setEndTime(3L);
    anomalyResultOne.setDetectionConfigId(PROP_ID_VALUE);
    anomalyResultOne.setParentCount(0);

    MergedAnomalyResultDTO anomalyResultTwo = new MergedAnomalyResultDTO();
    anomalyResultTwo.setStartTime(4L);
    anomalyResultTwo.setEndTime(6L);
    anomalyResultTwo.setDetectionConfigId(PROP_ID_VALUE);
    anomalyResultTwo.setParentCount(0);

    MergedAnomalyResultDTO anomalyResultThree = new MergedAnomalyResultDTO();
    anomalyResultThree.setStartTime(7L);
    anomalyResultThree.setEndTime(10L);
    anomalyResultThree.setDetectionConfigId(PROP_ID_VALUE);
    anomalyResultThree.setParentCount(0);


    pipelineLoader.setMockMergedAnomalies(Arrays.asList(anomalyResultOne, anomalyResultTwo, anomalyResultThree));
    pipelineLoader.setMockLastTimeStamp(10);

    this.mockDataProvider = new MockDataProvider().setAnomalies(Collections.singletonList(existingAnomalyResultDTO)).setLoader(pipelineLoader);

  }

  @Test
  public void testMergerRun() throws Exception {
    this.wrapper = new MergeWrapper(this.mockDataProvider, this.config, 1, 100);
    DetectionPipelineResult result = this.wrapper.run();
    Assert.assertEquals(result.getLastTimestamp(), 10);
    Assert.assertEquals(result.getAnomalies().size(), 4);
    Assert.assertEquals(result.getAnomalies().get(0).getStartTime(), 1);
    Assert.assertEquals(result.getAnomalies().get(0).getEndTime(), 6);
    Assert.assertEquals(result.getAnomalies().get(0).getParentCount().intValue(), 0);
    Assert.assertEquals(result.getAnomalies().get(1).getStartTime(), 2);
    Assert.assertEquals(result.getAnomalies().get(1).getEndTime(), 3);
    Assert.assertEquals(result.getAnomalies().get(1).getParentCount().intValue(), 1);
    Assert.assertEquals(result.getAnomalies().get(2).getStartTime(), 4);
    Assert.assertEquals(result.getAnomalies().get(2).getEndTime(), 6);
    Assert.assertEquals(result.getAnomalies().get(2).getParentCount().intValue(), 1);
    Assert.assertEquals(result.getAnomalies().get(3).getStartTime(), 7);
    Assert.assertEquals(result.getAnomalies().get(3).getEndTime(), 10);
    Assert.assertEquals(result.getAnomalies().get(3).getParentCount().intValue(), 0);
  }
}
