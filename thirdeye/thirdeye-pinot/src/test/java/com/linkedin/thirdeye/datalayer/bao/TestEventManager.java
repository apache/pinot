package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
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
