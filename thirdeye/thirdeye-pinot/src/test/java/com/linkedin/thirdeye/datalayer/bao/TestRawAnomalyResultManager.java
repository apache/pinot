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

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datasource.DAORegistry;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

public class TestRawAnomalyResultManager {

  RawAnomalyResultDTO anomalyResult;
  AnomalyFunctionDTO spec = DaoTestUtils.getTestFunctionSpec("metric", "dataset");

  private DAOTestBase testDAOProvider;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private RawAnomalyResultManager rawAnomalyResultDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
    rawAnomalyResultDAO = daoRegistry.getRawAnomalyResultDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testAnomalyResultCRUD() {
    anomalyFunctionDAO.save(spec);
    Assert.assertNotNull(spec);

    // create anomaly result
    anomalyResult = getRawoAnomalyDTO();

    anomalyResult.setFunction(spec);
    rawAnomalyResultDAO.save(anomalyResult);

    RawAnomalyResultDTO resultRet = rawAnomalyResultDAO.findById(anomalyResult.getId());
    Assert.assertEquals(resultRet.getFunction(), spec);
  }

  @Test(dependsOnMethods = {"testAnomalyResultCRUD"})
  public void testResultFeedback() {
    RawAnomalyResultDTO result = rawAnomalyResultDAO.findById(anomalyResult.getId());
    Assert.assertNotNull(result);
    Assert.assertNull(result.getFeedback());

    AnomalyFeedbackDTO feedback = new AnomalyFeedbackDTO();
    feedback.setComment("this is a good find");
    feedback.setFeedbackType(AnomalyFeedbackType.ANOMALY);
    result.setFeedback(feedback);
    rawAnomalyResultDAO.save(result);

    RawAnomalyResultDTO resultRet = rawAnomalyResultDAO.findById(anomalyResult.getId());
    Assert.assertEquals(resultRet.getId(), result.getId());
    Assert.assertNotNull(resultRet.getFeedback());

    AnomalyFunctionDTO functionSpec = result.getFunction();

    rawAnomalyResultDAO.deleteById(anomalyResult.getId());
    anomalyFunctionDAO.deleteById(functionSpec.getId());
  }

  public RawAnomalyResultDTO getRawoAnomalyDTO() {
    RawAnomalyResultDTO anomalyResult = new RawAnomalyResultDTO();
    anomalyResult.setScore(1.1);
    anomalyResult.setStartTime(System.currentTimeMillis());
    anomalyResult.setEndTime(System.currentTimeMillis());
    anomalyResult.setWeight(10.1);
    DimensionMap dimensionMap = new DimensionMap();
    dimensionMap.put("dimensionName", "dimensionValue");
    anomalyResult.setDimensions(dimensionMap);
    return anomalyResult;
  }

}
