package com.linkedin.thirdeye.datalayer.bao;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.alert.commons.AnomalyNotifiedStatus;
import com.linkedin.thirdeye.datalayer.DaoTestUtils;
import com.linkedin.thirdeye.datalayer.dto.AlertSnapshotDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
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
