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

package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeConfig;
import com.linkedin.thirdeye.anomaly.merge.AnomalyTimeBasedSummarizer;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

public class TestMergedAnomalyResultManager{
  private MergedAnomalyResultDTO mergedResult = null;
  private AnomalyFunctionDTO function = DaoTestUtils.getTestFunctionSpec("metric", "dataset");

  private DAOTestBase testDAOProvider;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;

  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    mergedAnomalyResultDAO = daoRegistry.getMergedAnomalyResultDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testMergedResultCRUD() {
    long functionId = anomalyFunctionDAO.save(function);
    Assert.assertNotNull(function.getId());

    // create anomaly result
    AnomalyResult rawAnomaly = DaoTestUtils.getAnomalyResult();

    // Let's create merged result
    List<AnomalyResult> rawResults = new ArrayList<>();
    rawResults.add(rawAnomaly);

    AnomalyMergeConfig mergeConfig = new AnomalyMergeConfig();

    List<MergedAnomalyResultDTO> mergedResults = AnomalyTimeBasedSummarizer.mergeAnomalies(rawResults, mergeConfig);
    Assert.assertEquals(mergedResults.get(0).getStartTime(), rawAnomaly.getStartTime());
    Assert.assertEquals(mergedResults.get(0).getEndTime(), rawAnomaly.getEndTime());

    // Let's persist the merged result
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    mergedResults.get(0).setFunction(anomalyFunction);
    mergedResults.get(0).setCollection(anomalyFunction.getCollection());
    mergedResults.get(0).setMetric(anomalyFunction.getTopicMetric());

    mergedAnomalyResultDAO.save(mergedResults.get(0));
    mergedResult = mergedResults.get(0);
    Assert.assertNotNull(mergedResult.getId());

    // verify the merged result
    MergedAnomalyResultDTO mergedResultById = mergedAnomalyResultDAO.findById(mergedResult.getId());
    Assert.assertEquals(mergedResultById.getDimensions(), rawAnomaly.getDimensions());

    List<MergedAnomalyResultDTO> mergedResultsByMetricDimensionsTime = mergedAnomalyResultDAO
        .findByCollectionMetricDimensionsTime(mergedResult.getCollection(), mergedResult.getMetric(),
            mergedResult.getDimensions().toString(), 0, System.currentTimeMillis());

    Assert.assertEquals(mergedResultsByMetricDimensionsTime.get(0), mergedResult);
  }

  @Test(dependsOnMethods = {"testMergedResultCRUD"})
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

  @Test
  public void testSaveChildren() {
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

    this.mergedAnomalyResultDAO.save(parent);

    Assert.assertNotNull(parent.getId());
    Assert.assertNotNull(child1.getId());
    Assert.assertNotNull(child2.getId());
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
  public void testLoadChildren() {
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
  }
}
