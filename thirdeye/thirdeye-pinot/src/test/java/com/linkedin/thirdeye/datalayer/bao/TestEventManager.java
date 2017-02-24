package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestEventManager extends AbstractManagerTestBase {
  long testEventId;

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
    EventDTO eventDTO = new EventDTO();
    eventDTO.setName("test");
    eventDTO.setMetric("test");
    eventDTO.setEventType(EventType.DEPLOYMENT.name());
    eventDTO.setService("testService");
    eventDTO.setStartTime(System.currentTimeMillis() - 10);
    eventDTO.setEndTime(System.currentTimeMillis());
    Map<String, List<String>> targetDimensionsMap = new HashMap<>();
    eventDTO.setTargetDimensionMap(targetDimensionsMap);

    testEventId = eventManager.save(eventDTO);
    Assert.assertTrue(testEventId > 0);
  }

  @Test(dependsOnMethods = { "testCreate" })
  public void testGetById() {
    EventDTO testEventDTO = eventManager.findById(testEventId);
    Assert.assertEquals(testEventDTO.getId().longValue(), testEventId);
    System.out.println(testEventDTO.getStartTime());
    System.out.println(testEventDTO.getEndTime());
    System.out.println(testEventDTO.getEventType());
    List<EventDTO> results0 = eventManager.findByEventType(EventType.DEPLOYMENT.name());
    Assert.assertEquals(results0.size(), 1);

    List<EventDTO> results1 = eventManager
        .findEventsBetweenTimeRange(EventType.DEPLOYMENT.name(), 0, System.currentTimeMillis());
    Assert.assertEquals(results1.size(), 1);
  }

  @Test(dependsOnMethods = { "testGetById" })
  public void testDelete() {
    eventManager.deleteById(testEventId);
    EventDTO testEventDTO = eventManager.findById(testEventId);
    Assert.assertNull(testEventDTO);
  }
}
