package com.linkedin.thirdeye.db;

import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAnomalyResultDAO extends AbstractDbTestBase {

  @Test
  public void testAnomalyResultCRUD() {
    AnomalyFunctionSpec spec = TestAnomalyFunctionDAO.getTestFunctionSpec();
    anomalyFunctionDAO.save(spec);
    Assert.assertNotNull(spec);

    AnomalyResult result = getAnomalyResult(spec.getId());
    anomalyResultDAO.save(result);

    AnomalyFunctionSpec specRet = anomalyFunctionDAO.findById(spec.getId());
    Assert.assertEquals(specRet.getAnomalies().size(), 1);

    anomalyResultDAO.deleteById(result.getId());
    anomalyFunctionDAO.delete(specRet);
  }

  static AnomalyResult getAnomalyResult(Long functionId) {
    AnomalyResult anomalyResult = new AnomalyResult();
    anomalyResult.setFunctionId(functionId);
    anomalyResult.setFunctionType("USER_RULE");
    anomalyResult.setScore(1.1);
    anomalyResult.setCollection("my dataset");
    anomalyResult.setStartTimeUtc(System.currentTimeMillis());
    anomalyResult.setEndTimeUtc(System.currentTimeMillis());
    anomalyResult.setDimensions("xyz dimension");
    anomalyResult.setMetric("x");
    anomalyResult.setWeight(10.1);
    anomalyResult.setCreationTimeUtc(System.currentTimeMillis());
    anomalyResult.setFunctionProperties("foo=bar");
    return anomalyResult;
  }
}
