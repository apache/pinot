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

package org.apache.pinot.thirdeye.anomaly.events;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.pinot.thirdeye.dashboard.resources.CustomizedEventResource;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class CustomizedEventResourceTest {

  private CustomizedEventResource customizedEventResource;
  private EventManager eventDAO = new MockEventsManager(new HashSet<>(), null);

  @BeforeMethod
  public void beforeMethod() {
    customizedEventResource = new CustomizedEventResource(eventDAO);
  }

  @Test
  public void testCreateCustomizedEvent() {
    customizedEventResource.createCustomizedEvent("test event", 1521832836L, 1521849599L, Arrays.asList("US", "CA"));

    List<EventDTO> events = eventDAO.findAll();
    Assert.assertEquals(events.size(), 1);
    Assert.assertEquals(events.get(0).getName(), "test event");
    Assert.assertEquals(events.get(0).getStartTime(), 1521832836L);
    Assert.assertEquals(events.get(0).getEndTime(), 1521849599L);
    Assert.assertEquals(events.get(0).getTargetDimensionMap().get("countryCode"), Arrays.asList("US", "CA"));
  }
}
