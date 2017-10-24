package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;

public class TestDataCompletenessConfigManager {

  private Long dataCompletenessConfigId1;
  private Long dataCompletenessConfigId2;
  private static String collection1 = "my dataset1";
  private DateTime now = new DateTime();
  private DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmm");

  private DAOTestBase testDAOProvider;
  private DataCompletenessConfigManager dataCompletenessConfigDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    dataCompletenessConfigDAO = daoRegistry.getDataCompletenessConfigDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreate() {

    dataCompletenessConfigId1 = dataCompletenessConfigDAO.
        save(DaoTestUtils.getTestDataCompletenessConfig(collection1, now.getMillis(), dateTimeFormatter.print(now.getMillis()), true));

    dataCompletenessConfigId2 = dataCompletenessConfigDAO.
        save(DaoTestUtils.getTestDataCompletenessConfig(collection1, now.minusHours(1).getMillis(), dateTimeFormatter.print(now.minusHours(1).getMillis()), false));

    Assert.assertNotNull(dataCompletenessConfigId1);
    Assert.assertNotNull(dataCompletenessConfigId2);

    List<DataCompletenessConfigDTO> dataCompletenessConfigDTOs = dataCompletenessConfigDAO.findAll();
    Assert.assertEquals(dataCompletenessConfigDTOs.size(), 2);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFind() {
    List<DataCompletenessConfigDTO> dataCompletenessConfigDTOs =
        dataCompletenessConfigDAO.findAllByDataset(collection1);
    Assert.assertEquals(dataCompletenessConfigDTOs.get(0).getDataset(), collection1);

    dataCompletenessConfigDTOs = dataCompletenessConfigDAO.findAllInTimeRange(now.minusMinutes(30).getMillis(),
        new DateTime().getMillis());
    Assert.assertEquals(dataCompletenessConfigDTOs.size(), 1);

    dataCompletenessConfigDTOs = dataCompletenessConfigDAO.findAllByTimeOlderThan(new DateTime().getMillis());
    Assert.assertEquals(dataCompletenessConfigDTOs.size(), 2);

    dataCompletenessConfigDTOs =
        dataCompletenessConfigDAO.findAllByTimeOlderThanAndStatus(new DateTime().getMillis(), true);
    Assert.assertEquals(dataCompletenessConfigDTOs.size(), 1);

    DataCompletenessConfigDTO config =
        dataCompletenessConfigDAO.findByDatasetAndDateSDF(collection1, dateTimeFormatter.print(now.getMillis()));
    Assert.assertNotNull(config);
    Assert.assertEquals(config.getId(), dataCompletenessConfigId1);

    config = dataCompletenessConfigDAO.findByDatasetAndDateMS(collection1, now.minusHours(1).getMillis());
    Assert.assertNotNull(config);
    Assert.assertEquals(config.getId(), dataCompletenessConfigId2);


  }

  @Test(dependsOnMethods = { "testFind" })
  public void testUpdate() {
    DataCompletenessConfigDTO dataCompletenessConfigDTO = dataCompletenessConfigDAO.findById(dataCompletenessConfigId2);
    Assert.assertNotNull(dataCompletenessConfigDTO);
    Assert.assertFalse(dataCompletenessConfigDTO.isTimedOut());
    dataCompletenessConfigDTO.setTimedOut(true);
    dataCompletenessConfigDAO.update(dataCompletenessConfigDTO);
    dataCompletenessConfigDTO = dataCompletenessConfigDAO.findById(dataCompletenessConfigId2);
    Assert.assertNotNull(dataCompletenessConfigDTO);
    Assert.assertTrue(dataCompletenessConfigDTO.isTimedOut());
  }

  @Test(dependsOnMethods = { "testUpdate" })
  public void testDelete() {
    dataCompletenessConfigDAO.deleteById(dataCompletenessConfigId2);
    DataCompletenessConfigDTO dataCompletenessConfigDTO = dataCompletenessConfigDAO.findById(dataCompletenessConfigId2);
    Assert.assertNull(dataCompletenessConfigDTO);
  }
}
