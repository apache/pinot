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

package org.apache.pinot.thirdeye.datalayer.bao;

import java.util.Set;
import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.pinot.thirdeye.constant.AnomalyFeedbackType;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

public class TestMergedAnomalyResultManager{
  private MergedAnomalyResultDTO mergedResult = null;

  private DAOTestBase testDAOProvider;
  private DetectionConfigManager detectionConfigDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;

  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    detectionConfigDAO = daoRegistry.getDetectionConfigManager();
    mergedAnomalyResultDAO = daoRegistry.getMergedAnomalyResultDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test(dependsOnMethods = {"testSaveChildren"})
  public void testFeedback() {
    MergedAnomalyResultDTO anomalyMergedResult = mergedAnomalyResultDAO.findById(mergedResult.getId());
    AnomalyFeedbackDTO feedback = new AnomalyFeedbackDTO();
    feedback.setComment("this is a good find");
    feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY);
    anomalyMergedResult.setFeedback(feedback);
    // now we need to make explicit call to anomaly update in order to update the feedback
    mergedAnomalyResultDAO.updateAnomalyFeedback(anomalyMergedResult);

    //verify feedback
    MergedAnomalyResultDTO mergedResult1 = mergedAnomalyResultDAO.findById(mergedResult.getId());
    Assert.assertEquals(mergedResult1.getFeedback().getFeedbackType(), AnomalyFeedbackType.ANOMALY);
  }

  @Test(dependsOnMethods = {"testSaveChildren"})
  public void testFeedbackPropagate() {
    MergedAnomalyResultDTO anomalyMergedResult = mergedAnomalyResultDAO.findById(mergedResult.getId());
    AnomalyFeedbackDTO feedback = new AnomalyFeedbackDTO();
    feedback.setComment("this is a good find");
    feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY);
    anomalyMergedResult.setFeedback(feedback);
    // now we need to make explicit call to anomaly update in order to update the feedback
    mergedAnomalyResultDAO.updateAnomalyFeedback(anomalyMergedResult, true);

    //verify child feedback
    MergedAnomalyResultDTO mergedResult1 = mergedAnomalyResultDAO.findById(mergedResult.getId());
    mergedResult1.getChildren().forEach(child -> {
        Assert.assertEquals(mergedResult1.getFeedback().getFeedbackType(), child.getFeedback().getFeedbackType());
        Assert.assertEquals(mergedResult1.getFeedback().getComment(), child.getFeedback().getComment());
      });
  }

  @Test
  public void testSaveChildren() {
    mergedResult = new MergedAnomalyResultDTO();
    mergedResult.setStartTime(1000);
    mergedResult.setEndTime(2000);

    MergedAnomalyResultDTO child1 = new MergedAnomalyResultDTO();
    child1.setStartTime(1000);
    child1.setEndTime(1500);

    MergedAnomalyResultDTO child2 = new MergedAnomalyResultDTO();
    child2.setStartTime(1500);
    child2.setEndTime(2000);

    mergedResult.setChildren(new HashSet<>(Arrays.asList(child1, child2)));

    this.mergedAnomalyResultDAO.save(mergedResult);

    Assert.assertNotNull(mergedResult.getId());
    Assert.assertNotNull(child1.getId());
    Assert.assertNotNull(child2.getId());
  }

  @Test
  public void testFindByStartEndTimeInRangeAndDetectionConfigId() {
    long detectionConfigId = detectionConfigDAO.save(mockDetectionConfig());
    List<MergedAnomalyResultDTO> anomalies = mockAnomalies(detectionConfigId);
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      this.mergedAnomalyResultDAO.save(anomaly);
    }
    List<MergedAnomalyResultDTO> fetchedAnomalies = mergedAnomalyResultDAO
        .findByStartEndTimeInRangeAndDetectionConfigId(
            new DateTime(2019, 1, 1, 0, 0).getMillis(),
            new DateTime(2019, 1, 3, 0, 0).getMillis(),
            detectionConfigId);
    Assert.assertEquals(fetchedAnomalies.size(), anomalies.size());
    for (int i = 0; i < anomalies.size(); i ++) {
      MergedAnomalyResultDTO actual = fetchedAnomalies.get(i);
      MergedAnomalyResultDTO expected = anomalies.get(i);
      Assert.assertNotNull(actual.getId());
      Assert.assertEquals(actual.getDetectionConfigId(), expected.getDetectionConfigId());
      Assert.assertEquals(actual.getDimensions(), expected.getDimensions());
    }
    // Clean up
    for (int i = 0; i < anomalies.size(); i++) {
      this.mergedAnomalyResultDAO.delete(fetchedAnomalies.get(i));
    }
    this.detectionConfigDAO.deleteById(detectionConfigId);
  }


  @Test
  public void testFindByStartTimeInRangeAndDetectionConfigId() {
    long detectionConfigId = detectionConfigDAO.save(mockDetectionConfig());
    List<MergedAnomalyResultDTO> anomalies = mockAnomalies(detectionConfigId);
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      this.mergedAnomalyResultDAO.save(anomaly);
    }
    List<MergedAnomalyResultDTO> fetchedAnomalies = mergedAnomalyResultDAO
        .findByStartEndTimeInRangeAndDetectionConfigId(
            new DateTime(2019, 1, 1, 0, 0).getMillis(),
            new DateTime(2019, 1, 3, 0, 0).getMillis(),
            detectionConfigId);
    Assert.assertEquals(fetchedAnomalies.size(), anomalies.size());
    for (int i = 0; i < anomalies.size(); i ++) {
      MergedAnomalyResultDTO actual = fetchedAnomalies.get(i);
      MergedAnomalyResultDTO expected = anomalies.get(i);
      Assert.assertNotNull(actual.getId());
      Assert.assertEquals(actual.getDetectionConfigId(), expected.getDetectionConfigId());
      Assert.assertEquals(actual.getDimensions(), expected.getDimensions());
    }
    // Clean up
    for (int i = 0; i < anomalies.size(); i++) {
      this.mergedAnomalyResultDAO.delete(fetchedAnomalies.get(i));
    }
    this.detectionConfigDAO.deleteById(detectionConfigId);
  }

  @Test
  public void testSaveChildrenIndependently() {
    MergedAnomalyResultDTO parent = new MergedAnomalyResultDTO();
    parent.setStartTime(1000);
    parent.setEndTime(2000);

    MergedAnomalyResultDTO child1 = new MergedAnomalyResultDTO();
    child1.setStartTime(1000);
    child1.setEndTime(1500);

    MergedAnomalyResultDTO child2 = new MergedAnomalyResultDTO();
    child2.setStartTime(1500);
    child2.setEndTime(2000);

    parent.setChildren(new HashSet<>(Arrays.asList(child1, child2)));

    long id = this.mergedAnomalyResultDAO.save(child1);
    this.mergedAnomalyResultDAO.save(parent);

    Assert.assertNotNull(parent.getId());
    Assert.assertEquals(child1.getId().longValue(), id);
    Assert.assertNotNull(child2.getId());
  }

  @Test
  public void testSaveAndLoadHierarchicalAnomalies() {
    MergedAnomalyResultDTO parent = new MergedAnomalyResultDTO();
    parent.setStartTime(1000);
    parent.setEndTime(2000);

    MergedAnomalyResultDTO child1 = new MergedAnomalyResultDTO();
    child1.setStartTime(1000);
    child1.setEndTime(1500);

    MergedAnomalyResultDTO child2 = new MergedAnomalyResultDTO();
    child2.setStartTime(1500);
    child2.setEndTime(2000);

    MergedAnomalyResultDTO child3 = new MergedAnomalyResultDTO();
    child3.setStartTime(1600);
    child3.setEndTime(1800);

    child2.setChildren(new HashSet<>(Arrays.asList(child3)));
    parent.setChildren(new HashSet<>(Arrays.asList(child1, child2)));

    long parentId = this.mergedAnomalyResultDAO.save(parent);

    MergedAnomalyResultDTO read = this.mergedAnomalyResultDAO.findById(parentId);

    Assert.assertNotSame(read, parent);
    Assert.assertEquals(read.getStartTime(), 1000);
    Assert.assertEquals(read.getEndTime(), 2000);
    Assert.assertFalse(read.isChild());
    Assert.assertFalse(read.getChildren().isEmpty());

    List<MergedAnomalyResultDTO> readChildren = new ArrayList<>(read.getChildren());
    Collections.sort(readChildren, new Comparator<MergedAnomalyResultDTO>() {
      @Override
      public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
        return Long.compare(o1.getStartTime(), o2.getStartTime());
      }
    });

    Assert.assertNotSame(readChildren.get(0), child1);
    Assert.assertTrue(readChildren.get(0).isChild());
    Assert.assertEquals(readChildren.get(0).getStartTime(), 1000);
    Assert.assertEquals(readChildren.get(0).getEndTime(), 1500);

    Assert.assertNotSame(readChildren.get(1), child2);
    Assert.assertTrue(readChildren.get(1).isChild());
    Assert.assertEquals(readChildren.get(1).getStartTime(), 1500);
    Assert.assertEquals(readChildren.get(1).getEndTime(), 2000);
    Assert.assertEquals(readChildren.get(1).getChildren().size(), 1);
    Assert.assertEquals(readChildren.get(1).getChildren().iterator().next().getStartTime(), 1600);
    Assert.assertEquals(readChildren.get(1).getChildren().iterator().next().getEndTime(), 1800);
  }

  @Test
  public void testUpdateToAnomalyHierarchy() {
    MergedAnomalyResultDTO parent = new MergedAnomalyResultDTO();
    parent.setStartTime(1000);
    parent.setEndTime(2000);

    MergedAnomalyResultDTO child1 = new MergedAnomalyResultDTO();
    child1.setStartTime(1000);
    child1.setEndTime(1500);

    MergedAnomalyResultDTO child2 = new MergedAnomalyResultDTO();
    child2.setStartTime(1500);
    child2.setEndTime(2000);

    MergedAnomalyResultDTO child3 = new MergedAnomalyResultDTO();
    child3.setStartTime(1600);
    child3.setEndTime(1800);

    child1.setChildren(new HashSet<>(Arrays.asList(child2)));
    parent.setChildren(new HashSet<>(Arrays.asList(child1)));

    this.mergedAnomalyResultDAO.save(parent);

    child2.setChildren(new HashSet<>(Arrays.asList(child3)));

    this.mergedAnomalyResultDAO.save(parent);

    MergedAnomalyResultDTO read = this.mergedAnomalyResultDAO.findById(parent.getId());
    Assert.assertFalse(read.getChildren().isEmpty());
    Assert.assertEquals(read.getChildren().iterator().next().getStartTime(), 1000);
    Assert.assertFalse(read.getChildren().iterator().next().getChildren().isEmpty());
    Assert.assertEquals(read.getChildren().iterator().next().getChildren().iterator().next().getStartTime(), 1500);
    Assert.assertFalse(read.getChildren().iterator().next().getChildren().iterator().next().getChildren().isEmpty());
    Assert.assertEquals(read.getChildren().iterator().next().getChildren().iterator().next().getChildren().iterator().next().getStartTime(), 1600);
  }

  @Test
  public void testFindParent() {
    MergedAnomalyResultDTO top = new MergedAnomalyResultDTO();
    top.setDetectionConfigId(1L);
    top.setStartTime(1000);
    top.setEndTime(3000);

    MergedAnomalyResultDTO child1 = new MergedAnomalyResultDTO();
    child1.setDetectionConfigId(1L);
    child1.setStartTime(1000);
    child1.setEndTime(2000);

    MergedAnomalyResultDTO child2 = new MergedAnomalyResultDTO();
    child2.setDetectionConfigId(1L);
    child2.setStartTime(1200);
    child2.setEndTime(1800);

    MergedAnomalyResultDTO child3 = new MergedAnomalyResultDTO();
    child3.setDetectionConfigId(1L);
    child3.setStartTime(1500);
    child3.setEndTime(3000);

    child1.setChildren(new HashSet<>(Collections.singletonList(child2)));
    top.setChildren(new HashSet<>(Arrays.asList(child1, child3)));

    long topId = this.mergedAnomalyResultDAO.save(top);
    MergedAnomalyResultDTO topNode = this.mergedAnomalyResultDAO.findById(topId);
    MergedAnomalyResultDTO parent = null;
    MergedAnomalyResultDTO leafNode = null;
    for (MergedAnomalyResultDTO intermediate : topNode.getChildren()) {
      if (!intermediate.getChildren().isEmpty()) {
        parent = intermediate;
        leafNode = intermediate.getChildren().iterator().next();
      }
    }
    Assert.assertNotNull(parent);
    Assert.assertEquals(parent, this.mergedAnomalyResultDAO.findParent(leafNode));
  }

  public static DetectionConfigDTO mockDetectionConfig() {
    DetectionConfigDTO detectionConfig = new DetectionConfigDTO();
    detectionConfig.setName("Only For Test");
    return detectionConfig;
  }

  public static List<MergedAnomalyResultDTO> mockAnomalies(long detectionConfigId) {
    MergedAnomalyResultDTO anomaly1 = new MergedAnomalyResultDTO();
    anomaly1.setMetric("metric");
    anomaly1.setDetectionConfigId(detectionConfigId);
    anomaly1.setStartTime(new DateTime(2019, 1, 1, 0, 0).getMillis());
    anomaly1.setEndTime(new DateTime(2019, 1, 1, 12, 0).getMillis());
    DimensionMap dimension1 = new DimensionMap();
    dimension1.put("what", "a");
    dimension1.put("where", "b");
    dimension1.put("when", "c");
    dimension1.put("how", "d");
    anomaly1.setDimensions(dimension1);
    MergedAnomalyResultDTO anomaly2 = new MergedAnomalyResultDTO();
    anomaly2.setMetric("metric");
    anomaly2.setDetectionConfigId(detectionConfigId);
    anomaly2.setStartTime(new DateTime(2019, 1, 2, 10, 0).getMillis());
    anomaly2.setEndTime(new DateTime(2019, 1, 2, 20, 0).getMillis());
    DimensionMap dimension2 = new DimensionMap();
    dimension2.put("what", "e");
    dimension2.put("where", "f");
    dimension2.put("when", "g");
    dimension2.put("how", "h");
    anomaly2.setDimensions(dimension2);

    return Arrays.asList(anomaly1, anomaly2);
  }
}
