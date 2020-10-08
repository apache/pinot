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

import static org.assertj.core.api.Assertions.assertThat;
import org.apache.pinot.thirdeye.datalayer.TestDatabase;
import org.apache.pinot.thirdeye.datalayer.dto.ApplicationDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestApplicationManager {

  public static final String APPLICATION_NAME = "MY_APP";
  public static final String APPLICATION_EMAIL = "abc@abc.in";
  private Long applicationId;

  private TestDatabase db;
  private ApplicationManager applicationManager;
  @BeforeClass
  void beforeClass() {
    db = new TestDatabase();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    applicationManager = daoRegistry.getApplicationDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    db.cleanup();
  }

  @Test
  public void testCreateApplication() {
    ApplicationDTO request = new ApplicationDTO();
    request.setApplication(APPLICATION_NAME);
    request.setRecipients(APPLICATION_EMAIL);
    applicationId = applicationManager.save(request);
    assertThat(applicationId).isGreaterThan(0);
  }

  @Test(dependsOnMethods = { "testCreateApplication" })
  public void testFetchApplication() {
    // find by id
    ApplicationDTO response = applicationManager.findById(applicationId);

    assertThat(response).isNotNull();
    assertThat(response.getId()).isEqualTo(applicationId);
    assertThat(response.getApplication()).isEqualTo(APPLICATION_NAME);
    assertThat(response.getRecipients()).isEqualTo(APPLICATION_EMAIL);

    assertThat(applicationManager.findAll().size()).isEqualTo(1);
  }

  @Test(dependsOnMethods = { "testFetchApplication" })
  public void testDeleteApplication() {
    assertThat(applicationManager.findAll().size()).isEqualTo(1);
    applicationManager.deleteById(applicationId);
    Assert.assertEquals(applicationManager.findAll().size(), 0);
  }
}
