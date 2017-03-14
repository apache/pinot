package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestAutotuneConfigManager extends AbstractManagerTestBase {
  private long autotuneId;
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

    autotuneId = autotuneConfigDAO.save(
        getTestAutotuneConfig(functionId, start, end)
    );
    Assert.assertNotNull(autotuneId);

    // test fetch all
    List<AutotuneConfigDTO> functions = autotuneConfigDAO.findAll();
    Assert.assertEquals(functions.size(), 1);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindAllByFunctionId() {
    List<AutotuneConfigDTO> functions = autotuneConfigDAO.findAllByFunctionId(functionId);
    Assert.assertEquals(functions.size(), 1);
  }

  @Test(dependsOnMethods = { "testFindAllByFunctionId" })
  public void testUpdate() {
    AutotuneConfigDTO spec = autotuneConfigDAO.findById(autotuneId);
    Assert.assertNotNull(spec);
    spec.setAutotuneMethod(AutotuneMethodType.EXHAUSTIVE);
    autotuneConfigDAO.save(spec);
    AutotuneConfigDTO specReturned = autotuneConfigDAO.findById(autotuneId);
    Assert.assertEquals(specReturned.getAutotuneMethod(), AutotuneMethodType.EXHAUSTIVE);
  }

  @Test(dependsOnMethods = { "testUpdate" })
  public void testDelete() {
    autotuneConfigDAO.deleteById(autotuneId);
    AutotuneConfigDTO spec = autotuneConfigDAO.findById(autotuneId);
    Assert.assertNull(spec);
  }
}
