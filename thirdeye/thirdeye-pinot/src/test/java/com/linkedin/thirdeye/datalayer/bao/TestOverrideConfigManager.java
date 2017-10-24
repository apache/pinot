package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.anomaly.override.OverrideConfigHelper;
import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datalayer.dto.OverrideConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestOverrideConfigManager {
  private Long overrideConfigId1 = null;
  private DateTime now = new DateTime();

  private DAOTestBase testDAOProvider;
  private OverrideConfigManager overrideConfigDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    overrideConfigDAO = daoRegistry.getOverrideConfigDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreate() {
    OverrideConfigDTO overrideConfigDTO1 = DaoTestUtils.getTestOverrideConfigForTimeSeries(now);
    overrideConfigId1 = overrideConfigDAO.save(overrideConfigDTO1);
    Assert.assertNotNull(overrideConfigId1);
    Assert.assertNotNull(overrideConfigDAO.findById(overrideConfigId1));
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFind() {
    List<OverrideConfigDTO> overrideConfigDTOs = overrideConfigDAO.findAll();
    Assert.assertEquals(overrideConfigDTOs.size(), 1);

    overrideConfigDTOs = overrideConfigDAO
        .findAllConflictByTargetType(OverrideConfigHelper.ENTITY_TIME_SERIES,
            now.minusHours(5).getMillis(), now.minusHours(2).getMillis());
    Assert.assertEquals(overrideConfigDTOs.size(), 1);

    overrideConfigDTOs = overrideConfigDAO
        .findAllConflictByTargetType(OverrideConfigHelper.ENTITY_ALERT_FILTER,
            now.minusHours(5).getMillis(), now.minusHours(2).getMillis());
    Assert.assertEquals(overrideConfigDTOs.size(), 0);

    overrideConfigDTOs = overrideConfigDAO
        .findAllConflictByTargetType(OverrideConfigHelper.ENTITY_TIME_SERIES,
            now.minusDays(100).getMillis(), now.minusDays(50).getMillis());
    Assert.assertEquals(overrideConfigDTOs.size(), 0);
  }

  @Test(dependsOnMethods = {"testFind"})
  public void testUpdate() {
    // Test update of override property
    OverrideConfigDTO overrideConfigDTO = overrideConfigDAO.findById(overrideConfigId1);
    Assert.assertNotNull(overrideConfigDTO);
    Assert.assertNotNull(overrideConfigDTO.getOverrideProperties());
    Assert.assertNotNull(overrideConfigDTO.getOverrideProperties().get(ScalingFactor.SCALING_FACTOR));
    Assert.assertEquals(overrideConfigDTO.getOverrideProperties().get(ScalingFactor.SCALING_FACTOR), "1.2");
    Map<String, String> newOverrideProperties = new HashMap<>();
    newOverrideProperties.put(ScalingFactor.SCALING_FACTOR, "0.8");
    overrideConfigDTO.setOverrideProperties(newOverrideProperties);
    overrideConfigDAO.update(overrideConfigDTO);

    overrideConfigDTO = overrideConfigDAO.findById(overrideConfigId1);
    Assert.assertNotNull(overrideConfigDTO);
    Assert.assertNotNull(overrideConfigDTO.getOverrideProperties());
    Assert.assertNotNull(overrideConfigDTO.getOverrideProperties().get(ScalingFactor.SCALING_FACTOR));
    Assert.assertEquals(overrideConfigDTO.getOverrideProperties().get(ScalingFactor.SCALING_FACTOR), "0.8");

    // Test update of target level
    overrideConfigDTO = overrideConfigDAO.findById(overrideConfigId1);
    Assert.assertNotNull(overrideConfigDTO);
    Assert.assertNotNull(overrideConfigDTO.getTargetLevel());
    List<String> targetLevel = overrideConfigDTO.getTargetLevel().get(OverrideConfigHelper
        .TARGET_COLLECTION);
    Assert.assertNotNull(targetLevel);
    Assert.assertEquals(targetLevel, Arrays.asList("collection1", "collection2"));
    List<String> newTargetLevel = Arrays.asList("collection1", "collection4");
    overrideConfigDTO.getTargetLevel().put(OverrideConfigHelper.TARGET_COLLECTION, newTargetLevel);
    overrideConfigDAO.update(overrideConfigDTO);

    overrideConfigDTO = overrideConfigDAO.findById(overrideConfigId1);
    Assert.assertNotNull(overrideConfigDTO);
    Assert.assertNotNull(overrideConfigDTO.getTargetLevel());
    targetLevel = overrideConfigDTO.getTargetLevel().get(OverrideConfigHelper.TARGET_COLLECTION);
    Assert.assertNotNull(targetLevel);
    Assert.assertEquals(targetLevel, Arrays.asList("collection1", "collection4"));
  }

  @Test(dependsOnMethods = { "testUpdate" })
  public void testDelete() {
    overrideConfigDAO.deleteById(overrideConfigId1);
    OverrideConfigDTO overrideConfigDTO = overrideConfigDAO.findById(overrideConfigId1);
    Assert.assertNull(overrideConfigDTO);
  }
}
