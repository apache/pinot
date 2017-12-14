package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.constant.AnomalyResultSource;
import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.DAOTestBase;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.thirdeye.constant.AnomalyFeedbackType.*;


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