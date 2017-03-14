package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestFunctionAutotuneConfigManager extends AbstractManagerTestBase {
  private long functionAutotuneId;
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

    functionAutotuneId = functionAutotuneConfigDAO.save(
        getTestFunctionAutotuneConfig(functionId, start, end)
    );
    Assert.assertNotNull(functionAutotuneId);

    // test fetch all
    List<AutotuneConfigDTO> functions = functionAutotuneConfigDAO.findAll();
    Assert.assertEquals(functions.size(), 1);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindAllByFunctionId() {
    List<AutotuneConfigDTO> functions = functionAutotuneConfigDAO.findAllByFunctionId(functionId);
    Assert.assertEquals(functions.size(), 1);
  }

  @Test(dependsOnMethods = { "testFindAllByFunctionId" })
  public void testUpdate() {
    AutotuneConfigDTO spec = functionAutotuneConfigDAO.findById(functionAutotuneId);
    Assert.assertNotNull(spec);
    spec.setAutotuneMethod(AutotuneMethodType.EXHAUSTIVE);
    functionAutotuneConfigDAO.save(spec);
    AutotuneConfigDTO specReturned = functionAutotuneConfigDAO.findById(functionAutotuneId);
    Assert.assertEquals(specReturned.getAutotuneMethod(), AutotuneMethodType.EXHAUSTIVE);
  }

  @Test(dependsOnMethods = { "testUpdate" })
  public void testDelete() {
    functionAutotuneConfigDAO.deleteById(functionAutotuneId);
    AutotuneConfigDTO spec = functionAutotuneConfigDAO.findById(functionAutotuneId);
    Assert.assertNull(spec);
  }
}
