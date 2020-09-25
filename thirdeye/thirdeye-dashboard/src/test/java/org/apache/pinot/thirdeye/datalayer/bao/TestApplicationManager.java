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

import org.apache.pinot.thirdeye.datalayer.dto.ApplicationDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestApplicationManager {

  Long applicationId;

  private DAOTestBase testDAOProvider;
  private ApplicationManager applicationDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    applicationDAO = daoRegistry.getApplicationDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreateApplication() {
    ApplicationDTO request = new ApplicationDTO();
    request.setApplication("MY_APP");
    request.setRecipients("abc@abc.in");
    applicationId = applicationDAO.save(request);
    Assert.assertTrue(applicationId > 0);
  }

  @Test(dependsOnMethods = { "testCreateApplication" })
  public void testFetchApplication() {
    // find by id
    ApplicationDTO response = applicationDAO.findById(applicationId);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getId(), applicationId);
    Assert.assertEquals(applicationDAO.findAll().size(), 1);
  }

  @Test(dependsOnMethods = { "testFetchApplication" })
  public void testDeleteApplication() {
    applicationDAO.deleteById(applicationId);
    Assert.assertEquals(applicationDAO.findAll().size(), 0);
  }
}
