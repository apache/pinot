package com.linkedin.thirdeye.datalayer.bao;

import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.datalayer.dto.DetectionStatusDTO;

public class TestDetectionStatusManager extends AbstractManagerTestBase {

  private Long detectionStatusId1;
  private Long detectionStatusId2;
  private static String collection1 = "my dataset1";
  private DateTime now = new DateTime();
  private DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmm");

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

    String dateString = dateTimeFormatter.print(now.getMillis());
    long dateMillis = dateTimeFormatter.parseMillis(dateString);
    detectionStatusId1 = detectionStatusDAO.save(getTestDetectionStatus(collection1, dateMillis, dateString, false, 1));

    dateMillis = new DateTime(dateMillis).minusHours(1).getMillis();
    dateString = dateTimeFormatter.print(dateMillis);
    detectionStatusId2 = detectionStatusDAO.
        save(getTestDetectionStatus(collection1, dateMillis, dateString, true, 1));

    Assert.assertNotNull(detectionStatusId1);
    Assert.assertNotNull(detectionStatusId2);

    List<DetectionStatusDTO> detectionStatusDTOs = detectionStatusDAO.findAll();
    Assert.assertEquals(detectionStatusDTOs.size(), 2);
  }

  /*@Test(dependsOnMethods = {"testCreate"})
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
  }*/
}
