package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestAnomalyFunctionExManager extends AbstractManagerTestBase {

  long anomalyFunctionId;

  static final String CLASS_NAME = "classname";
  static final String NAME = "name";

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
    anomalyFunctionId = anomalyFunctionExDAO.save(getTestFunctionExSpec(NAME, CLASS_NAME));
    Assert.assertNotNull(anomalyFunctionId);

    // test fetch all
    List<AnomalyFunctionExDTO> functions = anomalyFunctionExDAO.findAll();
    Assert.assertEquals(functions.size(), 1);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindByActive() {
    List<AnomalyFunctionExDTO> active = anomalyFunctionExDAO.findByActive(true);
    Assert.assertEquals(active.size(), 1);

    List<AnomalyFunctionExDTO> inactive = anomalyFunctionExDAO.findByActive(false);
    Assert.assertEquals(inactive.size(), 0);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindByName() {
    List<AnomalyFunctionExDTO> exist = anomalyFunctionExDAO.findByName(NAME);
    Assert.assertEquals(exist.size(), 1);

    List<AnomalyFunctionExDTO> doesnotexist = anomalyFunctionExDAO.findByName("DOES_NOT_EXIST");
    Assert.assertEquals(doesnotexist.size(), 0);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFindByClassName() {
    List<AnomalyFunctionExDTO> exist = anomalyFunctionExDAO.findByClassName(CLASS_NAME);
    Assert.assertEquals(exist.size(), 1);

    List<AnomalyFunctionExDTO> doesnotexist = anomalyFunctionExDAO.findByClassName("DOES_NOT_EXIST");
    Assert.assertEquals(doesnotexist.size(), 0);
  }

  @Test(dependsOnMethods = { "testFindByClassName" })
  public void testUpdate() {
    AnomalyFunctionExDTO spec = anomalyFunctionExDAO.findById(anomalyFunctionId);
    Assert.assertNotNull(spec);
    Assert.assertEquals(spec.getClassName(), CLASS_NAME);
    spec.setClassName("NEW");
    anomalyFunctionExDAO.save(spec);
    AnomalyFunctionExDTO specReturned = anomalyFunctionExDAO.findById(anomalyFunctionId);
    Assert.assertEquals(specReturned.getClassName(), "NEW");
  }

  @Test(dependsOnMethods = { "testUpdate" })
  public void testDelete() {
    anomalyFunctionExDAO.deleteById(anomalyFunctionId);
    AnomalyFunctionExDTO spec = anomalyFunctionExDAO.findById(anomalyFunctionId);
    Assert.assertNull(spec);
  }
}
