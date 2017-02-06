package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestEventManager extends AbstractManagerTestBase {
  long testEventId;

  @Test
  public void testCreate() {
    EventDTO eventDTO = new EventDTO();
    eventDTO.setName("test");
    eventDTO.setMetric("test");
    eventDTO.setEventType(EventType.DEPLOYMENT);
    eventDTO.setService("testService");
    eventDTO.setStartTime(System.currentTimeMillis());
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
  }

  @Test(dependsOnMethods = { "testGetById" })
  public void testDelete() {
    eventManager.deleteById(testEventId);
    EventDTO testEventDTO = eventManager.findById(testEventId);
    Assert.assertNull(testEventDTO);
  }
}
