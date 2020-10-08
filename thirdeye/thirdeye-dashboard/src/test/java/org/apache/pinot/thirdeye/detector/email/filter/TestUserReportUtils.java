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

package org.apache.pinot.thirdeye.detector.email.filter;

import org.apache.pinot.thirdeye.common.dimension.DimensionMap;
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datalayer.bao.AnomalyFunctionManager;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.constant.AnomalyFeedbackType.*;


public class TestUserReportUtils {
  private DAOTestBase testDAOProvider;
  private MergedAnomalyResultManager mergedAnomalyDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private static final String TEST = "test";

  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    mergedAnomalyDAO = daoRegistry.getMergedAnomalyResultDAO();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test(dataProvider = "provideAnomaliesWithUserReport")
  public void testIsUserReportAnomalyIsQualified(MergedAnomalyResultDTO userReportAnomalyEmptyDimension,
      MergedAnomalyResultDTO userReportAnomalyWithDimension,
      MergedAnomalyResultDTO userReportAnomalyFailRecovered) throws Exception {
    AlertFilter alertFilter = new DummyAlertFilter();
    Assert.assertFalse(UserReportUtils.isUserReportAnomalyIsQualified(alertFilter, userReportAnomalyEmptyDimension));
    Assert.assertTrue(UserReportUtils.isUserReportAnomalyIsQualified(alertFilter, userReportAnomalyWithDimension));
    Assert.assertFalse(UserReportUtils.isUserReportAnomalyIsQualified(alertFilter, userReportAnomalyFailRecovered));
  }

  @DataProvider(name = "provideAnomaliesWithUserReport")
  public Object[][] getMockMergedAnomalies() {
    AnomalyFunctionDTO anomalyFunction = DaoTestUtils.getTestFunctionSpec(TEST, TEST);
    long functionId = anomalyFunctionDAO.save(anomalyFunction);
    List<MergedAnomalyResultDTO> anomalyResultDTOS = new ArrayList<>();
    int totalAnomalies = 7;
    AnomalyFeedbackDTO positiveFeedback = new AnomalyFeedbackDTO();
    AnomalyFeedbackDTO negativeFeedback = new AnomalyFeedbackDTO();
    positiveFeedback.setFeedbackType(ANOMALY);
    negativeFeedback.setFeedbackType(NOT_ANOMALY);
    DimensionMap dimensionMap = new DimensionMap("{country=US,pageKey=linkedin}");
    for (int i = 0; i < totalAnomalies; i++) {
      MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
      anomaly.setFunction(anomalyFunction);
      anomaly.setFeedback(null);
      anomaly.setNotified(true);
      anomaly.setStartTime(i);
      anomaly.setEndTime(i+1);
      anomaly.setDimensions(dimensionMap);
      anomalyResultDTOS.add(anomaly);
      mergedAnomalyDAO.save(anomaly);
    }
    MergedAnomalyResultDTO userReportAnomalyEmptyDimension = new MergedAnomalyResultDTO();
    userReportAnomalyEmptyDimension.setNotified(false);
    userReportAnomalyEmptyDimension.setFunction(anomalyFunction);
    userReportAnomalyEmptyDimension.setFeedback(positiveFeedback);
    userReportAnomalyEmptyDimension.setAnomalyResultSource(AnomalyResultSource.USER_LABELED_ANOMALY);
    userReportAnomalyEmptyDimension.setStartTime(0L);
    userReportAnomalyEmptyDimension.setEndTime(10L);
    mergedAnomalyDAO.save(userReportAnomalyEmptyDimension);

    MergedAnomalyResultDTO userReportAnomalyWithDimension = new MergedAnomalyResultDTO();
    userReportAnomalyWithDimension.setNotified(false);
    userReportAnomalyWithDimension.setFunction(anomalyFunction);
    userReportAnomalyWithDimension.setFeedback(positiveFeedback);
    userReportAnomalyWithDimension.setAnomalyResultSource(AnomalyResultSource.USER_LABELED_ANOMALY);
    userReportAnomalyWithDimension.setStartTime(1L);
    userReportAnomalyWithDimension.setEndTime(10L);
    userReportAnomalyWithDimension.setDimensions(dimensionMap);
    mergedAnomalyDAO.save(userReportAnomalyWithDimension);

    MergedAnomalyResultDTO userReportAnomalyFailRecovered = new MergedAnomalyResultDTO();
    userReportAnomalyFailRecovered.setNotified(false);
    userReportAnomalyFailRecovered.setFunction(anomalyFunction);
    userReportAnomalyFailRecovered.setFeedback(positiveFeedback);
    userReportAnomalyFailRecovered.setAnomalyResultSource(AnomalyResultSource.USER_LABELED_ANOMALY);
    userReportAnomalyFailRecovered.setStartTime(12L);
    userReportAnomalyFailRecovered.setEndTime(20L);
    userReportAnomalyFailRecovered.setDimensions(dimensionMap);
    mergedAnomalyDAO.save(userReportAnomalyFailRecovered);


    return new Object[][]{{userReportAnomalyEmptyDimension, userReportAnomalyWithDimension, userReportAnomalyFailRecovered}};
  }
}