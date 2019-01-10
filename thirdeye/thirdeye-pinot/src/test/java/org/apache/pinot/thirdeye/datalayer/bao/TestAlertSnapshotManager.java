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

import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.alert.commons.AnomalyNotifiedStatus;
import org.apache.pinot.thirdeye.datalayer.DaoTestUtils;
import org.apache.pinot.thirdeye.datalayer.dto.AlertSnapshotDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestAlertSnapshotManager {
  private DAOTestBase testDAOProvider;
  private AlertSnapshotManager alerSnapshotDAO;

  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    alerSnapshotDAO = daoRegistry.getAlertSnapshotDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testAlertSnapshotManagerCRUD(){
    // test create
    AlertSnapshotDTO originalDto = DaoTestUtils.getTestAlertSnapshot();
    Assert.assertEquals(originalDto.getSnapshot().size(), 3);
    long id = alerSnapshotDAO.save(originalDto);
    Assert.assertEquals(alerSnapshotDAO.findAll().size(), 1);

    // test read
    AlertSnapshotDTO fetchedDto = alerSnapshotDAO.findById(id);
    Assert.assertEquals(fetchedDto, originalDto);

    // test update
    Multimap<String, AnomalyNotifiedStatus> snapshot = originalDto.getSnapshot();
    snapshot.put("test::{country=[NA]}", new AnomalyNotifiedStatus(15l, 0.2));
    originalDto.setSnapshot(snapshot);
    long updateId = alerSnapshotDAO.update(originalDto);
    Assert.assertEquals(updateId, id);
    Assert.assertEquals(alerSnapshotDAO.findById(updateId).getSnapshot().size(), 4);

    // test delete
    alerSnapshotDAO.delete(originalDto);
    Assert.assertEquals(alerSnapshotDAO.findAll().size(), 0);
  }
}
