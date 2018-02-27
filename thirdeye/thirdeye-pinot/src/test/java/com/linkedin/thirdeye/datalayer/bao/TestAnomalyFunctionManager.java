package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;

public class TestAnomalyFunctionManager {

  private Long anomalyFunctionId;
  private static String collection = "my dataset";
  private static String metricName = "__counts";

  private DAOTestBase testDAOProvider;
  private AnomalyFunctionManager anomalyFunctionDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    anomalyFunctionDAO = daoRegistry.getAnomalyFunctionDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreate() {
    anomalyFunctionId = anomalyFunctionDAO.save(DaoTestUtils.getTestFunctionSpec(metricName, collection));
    Assert.assertNotNull(anomalyFunctionId);

    // test fetch all
    List<AnomalyFunctionDTO> functions = anomalyFunctionDAO.findAll();
    Assert.assertEquals(functions.size(), 1);

    functions = anomalyFunctionDAO.findAllActiveFunctions();
    Assert.assertEquals(functions.size(), 1);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindNameEquals(){
    AnomalyFunctionDTO anomalyFunctionSpec = DaoTestUtils.getTestFunctionSpec(metricName, collection);
    Assert.assertNotNull(anomalyFunctionDAO.findWhereNameEquals(anomalyFunctionSpec.getFunctionName()));
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindAllByCollection() {
    List<AnomalyFunctionDTO> functions = anomalyFunctionDAO.findAllByCollection(collection);
    Assert.assertEquals(functions.size(), 1);
  }

  @Test(dependsOnMethods = {"testFindAllByCollection"})
  public void testDistinctMetricsByCollection() {
    List<String> metrics = anomalyFunctionDAO.findDistinctTopicMetricsByCollection(collection);
    Assert.assertEquals(metrics.get(0), metricName);
  }

  @Test(dependsOnMethods = { "testDistinctMetricsByCollection" })
  public void testUpdate() {
    AnomalyFunctionDTO spec = anomalyFunctionDAO.findById(anomalyFunctionId);
    Assert.assertNotNull(spec);
    Assert.assertEquals(spec.getMetricFunction(), MetricAggFunction.SUM);
    spec.setMetricFunction(MetricAggFunction.COUNT);
    anomalyFunctionDAO.save(spec);
    AnomalyFunctionDTO specReturned = anomalyFunctionDAO.findById(anomalyFunctionId);
    Assert.assertEquals(specReturned.getMetricFunction(), MetricAggFunction.COUNT);
  }

  @Test(dependsOnMethods = { "testUpdate" })
  public void testDelete() {
    anomalyFunctionDAO.deleteById(anomalyFunctionId);
    AnomalyFunctionDTO spec = anomalyFunctionDAO.findById(anomalyFunctionId);
    Assert.assertNull(spec);
  }
}
