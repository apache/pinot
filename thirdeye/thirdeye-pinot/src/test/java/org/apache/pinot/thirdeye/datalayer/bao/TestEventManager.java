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

import org.apache.pinot.thirdeye.anomaly.events.EventType;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestEventManager {
  long testEventId;

  private DAOTestBase testDAOProvider;
  private EventManager eventDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    DAORegistry daoRegistry = DAORegistry.getInstance();
    eventDAO = daoRegistry.getEventDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.cleanup();
  }

  @Test
  public void testCreate() {
    EventDTO eventDTO = new EventDTO();
    eventDTO.setName("test");
    eventDTO.setMetric("test");
    eventDTO.setEventType(EventType.DEPLOYMENT.name());
    eventDTO.setService("testService");
    eventDTO.setStartTime(System.currentTimeMillis() - 10);
    eventDTO.setEndTime(System.currentTimeMillis());
    Map<String, List<String>> targetDimensionsMap = new HashMap<>();
    eventDTO.setTargetDimensionMap(targetDimensionsMap);

    testEventId = eventDAO.save(eventDTO);
    Assert.assertTrue(testEventId > 0);
  }

  @Test(dependsOnMethods = { "testCreate" })
  public void testGetById() {
    EventDTO testEventDTO = eventDAO.findById(testEventId);
    Assert.assertEquals(testEventDTO.getId().longValue(), testEventId);
    System.out.println(testEventDTO.getStartTime());
    System.out.println(testEventDTO.getEndTime());
    System.out.println(testEventDTO.getEventType());
    List<EventDTO> results0 = eventDAO.findByEventType(EventType.DEPLOYMENT.name());
    Assert.assertEquals(results0.size(), 1);

    List<EventDTO> results1 = eventDAO
        .findEventsBetweenTimeRange(EventType.DEPLOYMENT.name(), 0, System.currentTimeMillis());
    Assert.assertEquals(results1.size(), 1);
  }

  @Test(dependsOnMethods = { "testGetById" })
  public void testDelete() {
    eventDAO.deleteById(testEventId);
    EventDTO testEventDTO = eventDAO.findById(testEventId);
    Assert.assertNull(testEventDTO);
  }
}
