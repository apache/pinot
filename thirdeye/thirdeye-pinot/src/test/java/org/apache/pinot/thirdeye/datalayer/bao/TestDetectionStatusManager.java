/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.datalayer.bao;

import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.pinot.thirdeye.datalayer.dto.DetectionStatusDTO;

public class TestDetectionStatusManager {

  private Long detectionStatusId1;
  private Long detectionStatusId2;
  private static String collection1 = "my dataset1";
  private DateTime now = new DateTime();
  private DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHH");

  private DAOTestBase testDAOProvider;
  private DetectionStatusManager detectionStatusDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    detectionStatusDAO = daoRegistry.getDetectionStatusDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreate() {

    String dateString = dateTimeFormatter.print(now.getMillis());
    long dateMillis = dateTimeFormatter.parseMillis(dateString);
    detectionStatusId1 = detectionStatusDAO.save(DaoTestUtils.getTestDetectionStatus(collection1, dateMillis, dateString, false, 1));
    detectionStatusDAO.save(DaoTestUtils.getTestDetectionStatus(collection1, dateMillis, dateString, true, 2));

    dateMillis = new DateTime(dateMillis).minusHours(1).getMillis();
    dateString = dateTimeFormatter.print(dateMillis);
    detectionStatusId2 = detectionStatusDAO.
        save(DaoTestUtils.getTestDetectionStatus(collection1, dateMillis, dateString, true, 1));
    detectionStatusDAO.save(DaoTestUtils.getTestDetectionStatus(collection1, dateMillis, dateString, true, 2));

    dateMillis = new DateTime(dateMillis).minusHours(1).getMillis();
    dateString = dateTimeFormatter.print(dateMillis);
    detectionStatusDAO.save(DaoTestUtils.getTestDetectionStatus(collection1, dateMillis, dateString, true, 2));

    Assert.assertNotNull(detectionStatusId1);
    Assert.assertNotNull(detectionStatusId2);

    List<DetectionStatusDTO> detectionStatusDTOs = detectionStatusDAO.findAll();
    Assert.assertEquals(detectionStatusDTOs.size(), 5);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testFind() {
    DetectionStatusDTO detectionStatusDTO = detectionStatusDAO.findLatestEntryForFunctionId(1);
    String dateString = dateTimeFormatter.print(now.getMillis());
    Assert.assertEquals(detectionStatusDTO.getFunctionId(), 1);
    Assert.assertEquals(detectionStatusDTO.getDateToCheckInSDF(), dateString);
    Assert.assertEquals(detectionStatusDTO.isDetectionRun(), false);

    long dateMillis = dateTimeFormatter.parseMillis(dateString);
    dateMillis = new DateTime(dateMillis).minusHours(1).getMillis();

    List<DetectionStatusDTO> detectionStatusDTOs = detectionStatusDAO.
        findAllInTimeRangeForFunctionAndDetectionRun(dateMillis, now.getMillis(), 2, true);
    Assert.assertEquals(detectionStatusDTOs.size(), 2);
    detectionStatusDTOs = detectionStatusDAO.
        findAllInTimeRangeForFunctionAndDetectionRun(dateMillis, now.getMillis(), 2, false);
    Assert.assertEquals(detectionStatusDTOs.size(), 0);

  }

}
