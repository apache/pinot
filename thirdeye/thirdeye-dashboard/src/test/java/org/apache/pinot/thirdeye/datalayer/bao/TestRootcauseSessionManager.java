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
import org.apache.pinot.thirdeye.datalayer.dto.RootcauseSessionDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestRootcauseSessionManager {

  private DAOTestBase testDAOProvider;
  private RootcauseSessionManager sessionDAO;

  @BeforeMethod
  void beforeMethod() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    sessionDAO = daoRegistry.getRootcauseSessionDAO();
  }

  @AfterMethod(alwaysRun = true)
  void afterMethod() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreateSession() {
    this.sessionDAO.save(makeDefault());
  }

  @Test
  public void testUpdateSession() throws Exception {
    RootcauseSessionDTO session = makeDefault();
    this.sessionDAO.save(session);

    session.setName("mynewname");
    this.sessionDAO.save(session);

    RootcauseSessionDTO read = this.sessionDAO.findById(session.getId());

    Assert.assertEquals(read.getName(), "mynewname");
  }

  @Test
  public void testFindSessionById() {
    Long id = this.sessionDAO.save(makeDefault());

    RootcauseSessionDTO session = this.sessionDAO.findById(id);
    Assert.assertEquals(session.getAnomalyRangeStart(), (Long) 1000L);
    Assert.assertEquals(session.getAnomalyRangeEnd(), (Long) 1100L);
    Assert.assertEquals(session.getAnalysisRangeStart(), (Long) 900L);
    Assert.assertEquals(session.getAnalysisRangeEnd(), (Long) 1200L);
    Assert.assertEquals(session.getCreated(), (Long) 1500L);
    Assert.assertEquals(session.getName(), "myname");
    Assert.assertEquals(session.getOwner(), "myowner");
    Assert.assertEquals(session.getText(), "mytext");
    Assert.assertEquals(session.getGranularity(), "mygranularity");
    Assert.assertEquals(session.getCompareMode(), "mycomparemode");
    Assert.assertEquals(session.getPreviousId(), (Long) 12345L);
  }

  @Test
  public void testFindSessionByName() {
    this.sessionDAO.save(makeName("A"));
    this.sessionDAO.save(makeName("B"));
    this.sessionDAO.save(makeName("A"));

    List<RootcauseSessionDTO> sessionsA = this.sessionDAO.findByName("A");
    List<RootcauseSessionDTO> sessionsB = this.sessionDAO.findByName("B");
    List<RootcauseSessionDTO> sessionsC = this.sessionDAO.findByName("C");

    Assert.assertEquals(sessionsA.size(), 2);
    Assert.assertEquals(sessionsB.size(), 1);
    Assert.assertEquals(sessionsC.size(), 0);
  }

  @Test
  public void testFindSessionByNameLike() {
    this.sessionDAO.save(makeName("ABC"));
    this.sessionDAO.save(makeName("BDC"));
    this.sessionDAO.save(makeName("CB"));

    List<RootcauseSessionDTO> sessionsAB = this.sessionDAO.findByNameLike(new HashSet<>(Arrays.asList("A", "B")));
    List<RootcauseSessionDTO> sessionsBC = this.sessionDAO.findByNameLike(new HashSet<>(Arrays.asList("B", "C")));
    List<RootcauseSessionDTO> sessionsCD = this.sessionDAO.findByNameLike(new HashSet<>(Arrays.asList("C", "D")));
    List<RootcauseSessionDTO> sessionsABCD = this.sessionDAO.findByNameLike(new HashSet<>(Arrays.asList("A", "B", "C", "D")));

    Assert.assertEquals(sessionsAB.size(), 1);
    Assert.assertEquals(sessionsBC.size(), 3);
    Assert.assertEquals(sessionsCD.size(), 1);
    Assert.assertEquals(sessionsABCD.size(), 0);
  }

  @Test
  public void testFindSessionByOwner() {
    this.sessionDAO.save(makeOwner("X"));
    this.sessionDAO.save(makeOwner("Y"));
    this.sessionDAO.save(makeOwner("Y"));

    List<RootcauseSessionDTO> sessionsX = this.sessionDAO.findByOwner("X");
    List<RootcauseSessionDTO> sessionsY = this.sessionDAO.findByOwner("Y");
    List<RootcauseSessionDTO> sessionsZ = this.sessionDAO.findByOwner("Z");

    Assert.assertEquals(sessionsX.size(), 1);
    Assert.assertEquals(sessionsY.size(), 2);
    Assert.assertEquals(sessionsZ.size(), 0);
  }

  @Test
  public void testFindByCreatedRange() {
    this.sessionDAO.save(makeCreated(800));
    this.sessionDAO.save(makeCreated(900));
    this.sessionDAO.save(makeCreated(1000));

    List<RootcauseSessionDTO> sessionsBefore = this.sessionDAO.findByCreatedRange(700, 800);
    List<RootcauseSessionDTO> sessionsMid = this.sessionDAO.findByCreatedRange(800, 1000);
    List<RootcauseSessionDTO> sessionsEnd = this.sessionDAO.findByCreatedRange(1000, 1500);

    Assert.assertEquals(sessionsBefore.size(), 0);
    Assert.assertEquals(sessionsMid.size(), 2);
    Assert.assertEquals(sessionsEnd.size(), 1);
  }

  @Test
  public void testFindByUpdatedRange() {
    this.sessionDAO.save(makeUpdated(800));
    this.sessionDAO.save(makeUpdated(900));
    this.sessionDAO.save(makeUpdated(1000));

    List<RootcauseSessionDTO> sessionsBefore = this.sessionDAO.findByUpdatedRange(700, 800);
    List<RootcauseSessionDTO> sessionsMid = this.sessionDAO.findByUpdatedRange(800, 1000);
    List<RootcauseSessionDTO> sessionsEnd = this.sessionDAO.findByUpdatedRange(1000, 1500);

    Assert.assertEquals(sessionsBefore.size(), 0);
    Assert.assertEquals(sessionsMid.size(), 2);
    Assert.assertEquals(sessionsEnd.size(), 1);
  }

  @Test
  public void testFindByAnomalyRange() {
    this.sessionDAO.save(makeAnomalyRange(1000, 1200));
    this.sessionDAO.save(makeAnomalyRange(1100, 1150));
    this.sessionDAO.save(makeAnomalyRange(1150, 1300));

    List<RootcauseSessionDTO> sessionsBefore = this.sessionDAO.findByAnomalyRange(0, 1000);
    List<RootcauseSessionDTO> sessionsMid = this.sessionDAO.findByAnomalyRange(1000, 1100);
    List<RootcauseSessionDTO> sessionsEnd = this.sessionDAO.findByAnomalyRange(1100, 1175);

    Assert.assertEquals(sessionsBefore.size(), 0);
    Assert.assertEquals(sessionsMid.size(), 1);
    Assert.assertEquals(sessionsEnd.size(), 3);
  }

  @Test
  public void testFindByPreviousId() {
    this.sessionDAO.save(makePrevious(0));
    this.sessionDAO.save(makePrevious(1));
    this.sessionDAO.save(makePrevious(1));
    this.sessionDAO.save(makePrevious(2));

    List<RootcauseSessionDTO> sessions0 = this.sessionDAO.findByPreviousId(0);
    List<RootcauseSessionDTO> sessions1 = this.sessionDAO.findByPreviousId(1);
    List<RootcauseSessionDTO> sessions2 = this.sessionDAO.findByPreviousId(2);
    List<RootcauseSessionDTO> sessions3 = this.sessionDAO.findByPreviousId(3);

    Assert.assertEquals(sessions0.size(), 1);
    Assert.assertEquals(sessions1.size(), 2);
    Assert.assertEquals(sessions2.size(), 1);
    Assert.assertEquals(sessions3.size(), 0);
  }

  @Test
  public void testFindByAnomalyId() {
    this.sessionDAO.save(makeAnomaly(0));
    this.sessionDAO.save(makeAnomaly(1));
    this.sessionDAO.save(makeAnomaly(1));
    this.sessionDAO.save(makeAnomaly(2));

    List<RootcauseSessionDTO> sessions0 = this.sessionDAO.findByAnomalyId(0);
    List<RootcauseSessionDTO> sessions1 = this.sessionDAO.findByAnomalyId(1);
    List<RootcauseSessionDTO> sessions2 = this.sessionDAO.findByAnomalyId(2);
    List<RootcauseSessionDTO> sessions3 = this.sessionDAO.findByAnomalyId(3);

    Assert.assertEquals(sessions0.size(), 1);
    Assert.assertEquals(sessions1.size(), 2);
    Assert.assertEquals(sessions2.size(), 1);
    Assert.assertEquals(sessions3.size(), 0);
  }

  private static RootcauseSessionDTO makeDefault() {
    return DaoTestUtils.getTestRootcauseSessionResult(1000, 1100, 1500, 2000, "myname", "myowner",
        "mytext", "mygranularity", "mycomparemode", 12345L, 1234L);
  }

  private static RootcauseSessionDTO makeName(String name) {
    RootcauseSessionDTO session = makeDefault();
    session.setName(name);
    return session;
  }

  private static RootcauseSessionDTO makeOwner(String owner) {
    RootcauseSessionDTO session = makeDefault();
    session.setOwner(owner);
    return session;
  }

  private static RootcauseSessionDTO makeCreated(long created) {
    RootcauseSessionDTO session = makeDefault();
    session.setCreated(created);
    return session;
  }

  private static RootcauseSessionDTO makeUpdated(long updated) {
    RootcauseSessionDTO session = makeDefault();
    session.setUpdated(updated);
    return session;
  }

  private static RootcauseSessionDTO makeAnomaly(long anomalyId) {
    RootcauseSessionDTO session = makeDefault();
    session.setAnomalyId(anomalyId);
    return session;
  }

  private static RootcauseSessionDTO makeAnomalyRange(long start, long end) {
    RootcauseSessionDTO session = makeDefault();
    session.setAnomalyRangeStart(start);
    session.setAnomalyRangeEnd(end);
    return session;
  }

  private static RootcauseSessionDTO makePrevious(long previousId) {
    RootcauseSessionDTO session = makeDefault();
    session.setPreviousId(previousId);
    return session;
  }
}
