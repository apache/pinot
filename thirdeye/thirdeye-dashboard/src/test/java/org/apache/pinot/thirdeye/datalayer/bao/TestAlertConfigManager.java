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

import com.google.common.base.Preconditions;
import org.apache.pinot.thirdeye.alert.commons.AnomalyFeedConfig;
import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datalayer.dto.AlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAlertConfigManager {

  Long alertConfigid;
  private DAOTestBase testDAOProvider;
  private AlertConfigManager alertConfigDAO;
  private List<Long> batchAlertConfigIdList = new ArrayList<>();

  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    alertConfigDAO = daoRegistry.getAlertConfigDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreateAlertConfig() {
    AlertConfigDTO request = new AlertConfigDTO();
    request.setActive(true);
    request.setName("my alert config");
    request.setApplication("test");
    alertConfigid = alertConfigDAO.save(request);
    Assert.assertTrue(alertConfigid > 0);
  }

  @Test
  public void testFindNameEquals() {
    AlertConfigDTO sample = alertConfigDAO.findAll().get(0);
    Assert.assertNotNull(alertConfigDAO.findWhereNameEquals(sample.getName()));
  }

  @Test (dependsOnMethods = {"testDeleteAlertConfig"})
  public void testCreateAlertConfigWithAnomalyFeedConfig() {
    AlertConfigDTO dto = new AlertConfigDTO();
    dto.setActive(true);
    dto.setName("my alert config");
    dto.setApplication("test");
    dto.setAnomalyFeedConfig(DaoTestUtils.getTestAnomalyFeedConfig());
    long dtoId = alertConfigDAO.save(dto);
    AlertConfigDTO newDto = alertConfigDAO.findById(dtoId);
    AnomalyFeedConfig feedConfig = dto.getAnomalyFeedConfig();
    AnomalyFeedConfig newFeedConfig = newDto.getAnomalyFeedConfig();

    Assert.assertEquals(newFeedConfig.getAnomalyFetcherConfigs().size(), feedConfig.getAnomalyFetcherConfigs().size());
    Assert.assertEquals(newFeedConfig.getAlertFilterConfigs(), feedConfig.getAlertFilterConfigs());
    Assert.assertEquals(newFeedConfig.getAnomalySource(), feedConfig.getAnomalySource());
  }

  @Test (dependsOnMethods = {"testCreateAlertConfig"})
  public void testFetchAlertConfig() {
    // find by id
    AlertConfigDTO response = alertConfigDAO.findById(alertConfigid);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getId(), alertConfigid);
    Assert.assertEquals(alertConfigDAO.findAll().size(), 1);
  }

  @Test (dependsOnMethods = {"testFetchAlertConfig"})
  public void testDeleteAlertConfig() {
    alertConfigDAO.deleteById(alertConfigid);
    Assert.assertEquals(alertConfigDAO.findAll().size(), 0);
  }

  /*
   * The Following section is the tests for AbstractManagerImpl and GenericPojoDao, which doesn't have a concrete class
   * to be tested upon. These test cannot be executed concurrently with above tests.
   */
  private List<AlertConfigDTO> createBatchAlertConfigs() {
    List<AlertConfigDTO> alertConfigList = new ArrayList<>();
    alertConfigList.add(createAlertConfig("1"));
    alertConfigList.add(createAlertConfig("2"));
    alertConfigList.add(createAlertConfig("3"));
    return alertConfigList;
  }

  private AlertConfigDTO createAlertConfig(String name) {
    AlertConfigDTO alertConfigDTO = new AlertConfigDTO();
    alertConfigDTO.setName(name);
    alertConfigDTO.setApplication("test");
    return alertConfigDTO;
  }

  @Test (expectedExceptions = IllegalArgumentException.class)
  public void testNullUpdate() {
    AlertConfigDTO alertConfig = createAlertConfig("nullID");
    alertConfigDAO.update(alertConfig);
  }

  @Test (dependsOnMethods = "testDeleteAlertConfig")
  public void testInterruptedSingleUpdate() {
    AlertConfigDTO alertConfig1 = createAlertConfig("100");
    Long id1 =alertConfigDAO.save(alertConfig1);

    AlertConfigDTO alertConfig2 = createAlertConfig("200");
    Long id2 = alertConfigDAO.save(alertConfig2);
    alertConfig2 = alertConfigDAO.findById(id2);

    // Trigger error due to duplicate names
    alertConfig2.setName("100");
    int update = alertConfigDAO.update(alertConfig2);// this should fail
    Assert.assertEquals(update, 0);
    AlertConfigDTO actualAlertConfig2 = alertConfigDAO.findById(id2);
    // Make sure the original data is preserved
    Assert.assertEquals(actualAlertConfig2.getName(), "200");

    // Clean up
    alertConfigDAO.deleteByIds(Arrays.asList(id1, id2));
  }

  @Test (dependsOnMethods = "testInterruptedSingleUpdate")
  public void testBatchUpdate() {
    List<AlertConfigDTO> initAlertConfigs = createBatchAlertConfigs();
    // Add multiple alert config to DB
    for (AlertConfigDTO alertConfig : initAlertConfigs) {
      alertConfig.setActive(false); // set the flag to false so we can update it later
      Long id = alertConfigDAO.save(alertConfig);
      batchAlertConfigIdList.add(id);
    }

    // Update previously saved alert configs
    List<AlertConfigDTO> readBackAlertConfigs = new ArrayList<>();
    for (Long alertId : batchAlertConfigIdList) {
      AlertConfigDTO readBackAlert = alertConfigDAO.findById(alertId);
      readBackAlert.setActive(true); // set to true to test batch update
      readBackAlertConfigs.add(readBackAlert);
    }
    int affectedRows = alertConfigDAO.update(readBackAlertConfigs);
    Assert.assertEquals(affectedRows, 3);

    // Check if batch update works
    for (Long alertId : batchAlertConfigIdList) {
      AlertConfigDTO updatedAlert = alertConfigDAO.findById(alertId);
      Assert.assertTrue(updatedAlert.isActive());
    }
  }

  @Test (dependsOnMethods = {"testBatchUpdate"})
  public void testBatchDeletion() {
    // Ensure that there are multiple alert configs in the DB; otherwise, this test is meaningless.
    Preconditions.checkState(batchAlertConfigIdList.size() > 1);
    for (Long alertId : batchAlertConfigIdList) {
      Preconditions.checkState(alertConfigDAO.findById(alertId) != null);
    }
    int expectedDeleteCount = batchAlertConfigIdList.size();

    // Add an non-exist key, which should not increase delete count
    Random random = new Random();
    while (batchAlertConfigIdList.size() == expectedDeleteCount) {
      long randomId = random.nextLong();
      if (alertConfigDAO.findById(randomId) == null) {
        batchAlertConfigIdList.add(randomId);
      }
    }

    // Delete by batch
    int deletedCount = alertConfigDAO.deleteByIds(batchAlertConfigIdList);
    Assert.assertEquals(deletedCount, expectedDeleteCount);
    for (Long alertId : batchAlertConfigIdList) {
      Assert.assertNull(alertConfigDAO.findById(alertId));
    }
  }

  @Test (dependsOnMethods = "testBatchDeletion")
  public void testBatchUpdateWithDuplicateName() {
    List<AlertConfigDTO> initAlertConfigs = createBatchAlertConfigs();
    List<Long> localBatchAlertConfigIdList = new ArrayList<>();
    // Add multiple alert config to DB
    for (AlertConfigDTO alertConfig : initAlertConfigs) {
      Long id = alertConfigDAO.save(alertConfig);
      localBatchAlertConfigIdList.add(id);
    }

    // Update previously saved alert configs
    List<AlertConfigDTO> readBackAlertConfigs = new ArrayList<>();
    for (Long alertId : localBatchAlertConfigIdList) {
      AlertConfigDTO readBackAlert = alertConfigDAO.findById(alertId);
      // Everyone update to the same name in order to trigger DB error
      readBackAlert.setName("4");
      readBackAlertConfigs.add(readBackAlert);
    }
    // Due to duplicate alert name, only the first alert config will be updated and
    // the other two will be ignored.
    int affectedRows = alertConfigDAO.update(readBackAlertConfigs);
    Assert.assertEquals(affectedRows, 1);

    // Check if batch update works; index table is also checked using findByPredicate() method.
    AlertConfigDTO firstAlert = alertConfigDAO.findById(localBatchAlertConfigIdList.get(0));
    Assert.assertEquals(firstAlert.getName(), "4");
    Assert.assertEquals(alertConfigDAO.findByPredicate(Predicate.EQ("name", "1")).size(), 0);
    Assert.assertEquals(alertConfigDAO.findByPredicate(Predicate.EQ("name", "4")).get(0), firstAlert);

    AlertConfigDTO secondAlert = alertConfigDAO.findById(localBatchAlertConfigIdList.get(1));
    Assert.assertEquals(secondAlert.getName(), "2");
    Assert.assertEquals(alertConfigDAO.findByPredicate(Predicate.EQ("name", "2")).get(0), secondAlert);

    AlertConfigDTO thirdAlert = alertConfigDAO.findById(localBatchAlertConfigIdList.get(2));
    Assert.assertEquals(thirdAlert.getName(), "3");
    Assert.assertEquals(alertConfigDAO.findByPredicate(Predicate.EQ("name", "3")).get(0), thirdAlert);

    // Clean up
    alertConfigDAO.deleteByIds(localBatchAlertConfigIdList);
  }

  @Test (dependsOnMethods = "testBatchUpdateWithDuplicateName")
  public void testBatchUpdateWithNullID() {
    List<AlertConfigDTO> initAlertConfigs = createBatchAlertConfigs();
    List<Long> localBatchAlertConfigIdList = new ArrayList<>();
    // Add multiple alert config to DB
    for (AlertConfigDTO alertConfig : initAlertConfigs) {
      Long id = alertConfigDAO.save(alertConfig);
      localBatchAlertConfigIdList.add(id);
    }

    // Update previously saved alert configs
    List<AlertConfigDTO> readBackAlertConfigs = new ArrayList<>();
    for (Long alertId : localBatchAlertConfigIdList) {
      AlertConfigDTO readBackAlert = alertConfigDAO.findById(alertId);
      // Activate everyone for future check
      readBackAlert.setActive(true);
      readBackAlertConfigs.add(readBackAlert);
    }
    AlertConfigDTO secondAlert = readBackAlertConfigs.get(1);
    secondAlert.setId(null);
    // Due to null alert name, only the first and third alert config will be updated
    int affectedRows = alertConfigDAO.update(readBackAlertConfigs);
    Assert.assertEquals(affectedRows, 2);

    // Check if batch update works; index table is also checked using findByPredicate() method.
    AlertConfigDTO firstAlert = alertConfigDAO.findById(localBatchAlertConfigIdList.get(0));
    Assert.assertTrue(firstAlert.isActive());

    secondAlert = alertConfigDAO.findById(localBatchAlertConfigIdList.get(1));
    Assert.assertFalse(secondAlert.isActive());

    AlertConfigDTO thirdAlert = alertConfigDAO.findById(localBatchAlertConfigIdList.get(2));
    Assert.assertTrue(thirdAlert.isActive());

    // Clean up
    alertConfigDAO.deleteByIds(localBatchAlertConfigIdList);
  }
  /*
   * End of the section for the tests of AbstractManagerImpl and GenericPojoDao.
   */
}
