package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.FunctionAutoTuneConfigDTO;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestFunctionAutoTuneConfigManager extends AbstractManagerTestBase {
  private long functionAutoTuneId;
  AnomalyFunctionDTO function = getTestFunctionSpec("metric", "dataset");
  private long functionId = 1l;
  private static long start = 1l;
  private static long end = 2l;

  @BeforeClass
  void beforeClass() {
    super.init();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    super.cleanup();
  }
  @Test
  public void testCreate() {
    functionId = anomalyFunctionDAO.save(function);
    Assert.assertNotNull(function.getId());

    functionAutoTuneId = functionAutoTuneConfigDAO.save(
        getTestFunctionAutoTuneConfig(functionId, start, end)
    );
    Assert.assertNotNull(functionAutoTuneId);

    // test fetch all
    List<FunctionAutoTuneConfigDTO> functions = functionAutoTuneConfigDAO.findAll();
    Assert.assertEquals(functions.size(), 1);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindAllByFunctionId() {
    List<FunctionAutoTuneConfigDTO> functions = functionAutoTuneConfigDAO.findAllByFunctionId(functionId);
    Assert.assertEquals(functions.size(), 1);
  }

  @Test(dependsOnMethods = { "testFindAllByFunctionId" })
  public void testUpdate() {
    FunctionAutoTuneConfigDTO spec = functionAutoTuneConfigDAO.findById(functionAutoTuneId);
    Assert.assertNotNull(spec);
    spec.setAutotuneMethod(AutotuneMethodType.EXHAUSTIVE);
    functionAutoTuneConfigDAO.save(spec);
    FunctionAutoTuneConfigDTO specReturned = functionAutoTuneConfigDAO.findById(functionAutoTuneId);
    Assert.assertEquals(specReturned.getAutotuneMethod(), AutotuneMethodType.EXHAUSTIVE);
  }

  @Test(dependsOnMethods = { "testUpdate" })
  public void testDelete() {
    functionAutoTuneConfigDAO.deleteById(functionAutoTuneId);
    FunctionAutoTuneConfigDTO spec = functionAutoTuneConfigDAO.findById(functionAutoTuneId);
    Assert.assertNull(spec);
  }
}
