
package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.dto.ClassificationConfigDTO;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestClassificationJobConfigManager extends AbstractManagerTestBase {
  private static final long MAIN_FUNCTION_ID = 101L;

  private static final String NAME1 = "test classification name1";
  private static final long FUNCTION_ID1 = MAIN_FUNCTION_ID;
  private static List<Long> FUNCTION_IDS1 = new ArrayList<Long>() {{
    add(MAIN_FUNCTION_ID);
    add(102L);
    add(103L);
  }};

  private static final String NAME2 = "test classification name2";
  private static final long FUNCTION_ID2 = MAIN_FUNCTION_ID;
  private static List<Long> FUNCTION_IDS2 = new ArrayList<Long>() {{
    add(MAIN_FUNCTION_ID);
    add(103L);
    add(204L);
  }};

  private Long configId1;
  private Long configId2;

  @BeforeClass
  void beforeClass() {
    super.init();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    super.cleanup();
  }

  @Test
  public void testCreateConfig() {
    ClassificationConfigDTO classificationConfig1 = this.getTestClassificationConfig(NAME1, FUNCTION_ID1, FUNCTION_IDS1);
    configId1 = classificationConfigDAO.save(classificationConfig1);
    Assert.assertTrue(configId1 > 0);

    ClassificationConfigDTO classificationConfig2 = this.getTestClassificationConfig(NAME2, FUNCTION_ID2, FUNCTION_IDS2);
    classificationConfig2.setActive(false);
    configId2 = classificationConfigDAO.save(classificationConfig2);
    Assert.assertTrue(configId2 > 0);
  }

  @Test (dependsOnMethods = {"testCreateConfig"})
  public void testFetchConfig() {
    ClassificationConfigDTO response = classificationConfigDAO.findById(configId1);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getId(), configId1);
    Assert.assertEquals(response.getName(), NAME1);

    response = classificationConfigDAO.findByName(NAME1);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getId(), configId1);
    Assert.assertEquals(response.getName(), NAME1);
    Assert.assertEquals(response.isActive(), true);
    Assert.assertEquals(response.getFunctionIdList(), FUNCTION_IDS1);

    response = classificationConfigDAO.findByName(NAME2);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getId(), configId2);
    Assert.assertEquals(response.getName(), NAME2);
    Assert.assertEquals(response.isActive(), false);
    Assert.assertEquals(response.getFunctionIdList(), FUNCTION_IDS2);

    List<ClassificationConfigDTO> responses = classificationConfigDAO.findActiveByFunctionId(MAIN_FUNCTION_ID);
    Assert.assertEquals(responses.size(), 1);
    for (ClassificationConfigDTO classificationConfig : responses) {
      Assert.assertEquals(classificationConfig.getMainFunctionId(), MAIN_FUNCTION_ID);
      Assert.assertEquals(classificationConfig.isActive(), true);
    }
  }

  @Test (dependsOnMethods = {"testFetchConfig"})
  public void testDeleteConfig() {
    List<ClassificationConfigDTO> responses = classificationConfigDAO.findActiveByFunctionId(MAIN_FUNCTION_ID);
    for (ClassificationConfigDTO classificationConfig : responses) {
      classificationConfigDAO.deleteById(classificationConfig.getId());
    }
    List<ClassificationConfigDTO> allDTO =  classificationConfigDAO.findAll();
    Assert.assertEquals(allDTO.size(), 1);
  }
}
