package com.linkedin.thirdeye.db.dao;

import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;

import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAnomalyFunctionDAO extends AbstractDbTestBase {

  private Long anomalyFunctionId;
  private static String collection = "my dataset";
  private static String metricName = "__counts";

  @Test
  public void testCreate() {
    anomalyFunctionId = anomalyFunctionDAO.save(getTestFunctionSpec(metricName, collection));
    Assert.assertNotNull(anomalyFunctionId);

    // test fetch all
    List<AnomalyFunctionDTO> functions = anomalyFunctionDAO.findAll();
    Assert.assertEquals(functions.size(), 1);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindAllByCollection() {
    List<AnomalyFunctionDTO> functions = anomalyFunctionDAO.findAllByCollection(collection);
    Assert.assertEquals(functions.size(), 1);
  }

  @Test(dependsOnMethods = {"testFindAllByCollection"})
  public void testDistinctMetricsByCollection() {
    List<String> metrics = anomalyFunctionDAO.findDistinctMetricsByCollection(collection);
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
