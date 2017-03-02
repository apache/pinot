package com.linkedin.thirdeye.client;

import com.linkedin.thirdeye.datalayer.bao.jdbc.AlertConfigManagerImpl;
import org.testng.annotations.Test;

public class TestDaoRegistry {

  final DAORegistry registry = DAORegistry.getInstance();

  // NOTE: DAORegistry is reset by test listener before test start

  // TODO cover all getters and setters
  //    registry.getAnomalyFunctionDAO();
  //    registry.getDashboardConfigDAO();
  //    registry.getDataCompletenessConfigDAO();
  //    registry.getDatasetConfigDAO();
  //    registry.getEmailConfigurationDAO();
  //    registry.getIngraphDashboardConfigDAO();
  //    registry.getIngraphMetricConfigDAO();
  //    registry.getJobDAO();
  //    registry.getMergedAnomalyResultDAO();
  //    registry.getMetricConfigDAO();
  //    registry.getOverrideConfigDAO();
  //    registry.getRawAnomalyResultDAO();
  //    registry.getTaskDAO();

  @Test
  void testSingletonReset() {
    registry.setAlertConfigDAO(new AlertConfigManagerImpl());
    registry.getAlertConfigDAO();
    DAORegistry.resetForTesting();

    registry.setAlertConfigDAO(new AlertConfigManagerImpl());
    registry.getAlertConfigDAO();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  void testGetNullFail() {
    registry.getAlertConfigDAO();
  }

  @Test
  void testSetNullPass() {
    registry.setAlertConfigDAO(new AlertConfigManagerImpl());
  }

  @Test
  void testGetNotNullPass() {
    registry.setAlertConfigDAO(new AlertConfigManagerImpl());
    registry.getAlertConfigDAO();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  void testSetNotNullFail() {
    registry.setAlertConfigDAO(new AlertConfigManagerImpl());
    registry.setAlertConfigDAO(new AlertConfigManagerImpl());
  }

}
