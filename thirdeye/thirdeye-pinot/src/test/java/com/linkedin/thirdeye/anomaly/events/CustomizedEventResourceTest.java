package com.linkedin.thirdeye.anomaly.events;

import com.linkedin.thirdeye.dashboard.resources.CustomizedEventResource;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import java.util.HashSet;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class CustomizedEventResourceTest {

  private CustomizedEventResource customizedEventResource;
  private EventManager eventDAO = new MockEventsManager(new HashSet<EventDTO>());

  @BeforeMethod
  public void beforeMethod() {
    customizedEventResource = new CustomizedEventResource(eventDAO);
  }

  @Test
  public void testCreateCustomizedEvent() {
    customizedEventResource.createCustomizedEvent("test event", 1521832836L, 1521849599L);

    List<EventDTO> events = eventDAO.findAll();
    Assert.assertEquals(events.size(), 1);
    Assert.assertEquals(events.get(0).getName(), "test event");
    Assert.assertEquals(events.get(0).getStartTime(), 1521832836L);
    Assert.assertEquals(events.get(0).getEndTime(), 1521849599L);
  }
}