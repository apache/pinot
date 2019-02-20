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

package org.apache.pinot.thirdeye.eventprovider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import org.apache.pinot.thirdeye.anomaly.events.EventFilter;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;

public class TestEventFilter {


  @Test
  public void testApplyEventDimensionFilter() throws Exception {
    EventDTO event1 = new EventDTO();
    event1.setName("event1");

    EventDTO event2 = new EventDTO();
    event2.setName("event2");
    Map<String, List<String>> eventDimensionMap2 = new HashMap<>();
    eventDimensionMap2.put("country_code", Lists.newArrayList("peru", "brazil"));
    eventDimensionMap2.put("BrowserName", Lists.newArrayList("chrome"));
    event2.setTargetDimensionMap(eventDimensionMap2);

    EventDTO event3 = new EventDTO();
    event3.setName("event3");
    Map<String, List<String>> eventDimensionMap3 = new HashMap<>();
    eventDimensionMap3.put("country_code", Lists.newArrayList("srilanka", "india"));
    event3.setTargetDimensionMap(eventDimensionMap3);


    List<EventDTO> allEvents = new ArrayList<>();
    Map<String, List<String>> eventFilterDimensionMap = new HashMap<>();
    List<EventDTO> filteredEvents;

    // empty allEvents
    filteredEvents = EventFilter.applyDimensionFilter(allEvents , eventFilterDimensionMap);
    Assert.assertEquals(filteredEvents.size(), 0);

    // empty filters map
    allEvents.add(event1);
    filteredEvents = EventFilter.applyDimensionFilter(allEvents , eventFilterDimensionMap);
    Assert.assertEquals(filteredEvents.size(), 1);

    // non empty filters map, empty event dimensions map
    eventFilterDimensionMap.put("country", Lists.newArrayList("brazil"));
    filteredEvents = EventFilter.applyDimensionFilter(allEvents , eventFilterDimensionMap);
    Assert.assertEquals(filteredEvents.size(), 0);

    // an event passes one filter
    allEvents.add(event2);
    allEvents.add(event3);
    filteredEvents = EventFilter.applyDimensionFilter(allEvents , eventFilterDimensionMap);
    Assert.assertEquals(filteredEvents.size(), 1);
    Assert.assertEquals(filteredEvents.get(0).getName(), "event2");

    // an event passes multiple filters
    eventFilterDimensionMap.put("browser", Lists.newArrayList("chrome"));
    filteredEvents = EventFilter.applyDimensionFilter(allEvents , eventFilterDimensionMap);
    Assert.assertEquals(filteredEvents.size(), 1);
    Assert.assertEquals(filteredEvents.get(0).getName(), "event2");

    // multiple events pass a filter
    eventFilterDimensionMap.remove("browser");
    eventFilterDimensionMap.put("country", Lists.newArrayList("brazil", "india"));
    filteredEvents = EventFilter.applyDimensionFilter(allEvents , eventFilterDimensionMap);
    Assert.assertEquals(filteredEvents.size(), 2);
    Assert.assertEquals(filteredEvents.get(0).getName(), "event2");
    Assert.assertEquals(filteredEvents.get(1).getName(), "event3");

    // an event passes a filter, another event passes another filter
    eventFilterDimensionMap.put("country", Lists.newArrayList("india"));
    eventFilterDimensionMap.put("browser", Lists.newArrayList("chrome"));
    filteredEvents = EventFilter.applyDimensionFilter(allEvents , eventFilterDimensionMap);
    Assert.assertEquals(filteredEvents.size(), 2);
    Assert.assertEquals(filteredEvents.get(0).getName(), "event2");
    Assert.assertEquals(filteredEvents.get(1).getName(), "event3");

    // no events pass filter
    eventFilterDimensionMap.put("country", Lists.newArrayList("france"));
    eventFilterDimensionMap.put("browser", Lists.newArrayList("mozilla"));
    filteredEvents = EventFilter.applyDimensionFilter(allEvents , eventFilterDimensionMap);
    Assert.assertEquals(filteredEvents.size(), 0);

    // all events pass filter
    Map<String, List<String>> eventDimensionMap1 = new HashMap<>();
    eventDimensionMap1.put("country_code", Lists.newArrayList("india"));
    event1.setTargetDimensionMap(eventDimensionMap1);
    eventFilterDimensionMap.remove("browser");
    eventFilterDimensionMap.put("country", Lists.newArrayList("brazil", "india"));
    filteredEvents = EventFilter.applyDimensionFilter(allEvents , eventFilterDimensionMap);
    Assert.assertEquals(filteredEvents.size(), 3);

  }

}
