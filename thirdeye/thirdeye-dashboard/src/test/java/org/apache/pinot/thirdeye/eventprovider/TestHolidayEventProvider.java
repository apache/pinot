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

import com.google.common.collect.Lists;
import org.apache.pinot.thirdeye.anomaly.events.HolidayEventProvider;
import org.apache.pinot.thirdeye.anomaly.events.EventFilter;
import org.apache.pinot.thirdeye.anomaly.events.EventType;
import org.apache.pinot.thirdeye.datalayer.bao.DAOTestBase;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;

import org.apache.pinot.thirdeye.datasource.DAORegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
public class TestHolidayEventProvider {



  public class TestEventManager {
    long testEventId;
    HolidayEventProvider holidayEventProvider = null;
    long hoursAgo5 = new DateTime().minusHours(5).getMillis();
    long hoursAgo4 = new DateTime().minusHours(4).getMillis();
    long hoursAgo3 = new DateTime().minusHours(3).getMillis();
    long hoursAgo2 = new DateTime().minusHours(2).getMillis();

    private DAOTestBase testDAOProvider;
    private EventManager eventDAO;
    @BeforeClass
    void beforeClass() {
      testDAOProvider = DAOTestBase.getInstance();
      DAORegistry daoRegistry = DAORegistry.getInstance();
      eventDAO = daoRegistry.getEventDAO();
      holidayEventProvider = new HolidayEventProvider();
    }

    @AfterClass(alwaysRun = true)
    void afterClass() {
      testDAOProvider.cleanup();
    }

    @Test
    public void testGetEvents() {
      EventDTO event1 = new EventDTO();
      event1.setName("event1");
      event1.setEventType(EventType.DEPLOYMENT.toString());
      eventDAO.save(event1);

      EventDTO event2 = new EventDTO();
      event2.setName("event2");
      event2.setEventType(EventType.HOLIDAY.toString());
      event2.setStartTime(hoursAgo4);
      event2.setEndTime(hoursAgo3);
      Map<String, List<String>> eventDimensionMap2 = new HashMap<>();
      eventDimensionMap2.put("country_code", Lists.newArrayList("peru", "brazil"));
      eventDimensionMap2.put("BrowserName", Lists.newArrayList("chrome"));
      event2.setTargetDimensionMap(eventDimensionMap2);
      eventDAO.save(event2);

      EventDTO event3 = new EventDTO();
      event3.setName("event3");
      event3.setStartTime(hoursAgo3);
      event3.setEndTime(hoursAgo2);
      event3.setEventType(EventType.HOLIDAY.toString());
      Map<String, List<String>> eventDimensionMap3 = new HashMap<>();
      eventDimensionMap3.put("country_code", Lists.newArrayList("srilanka", "india"));
      event3.setTargetDimensionMap(eventDimensionMap3);
      eventDAO.save(event3);

      EventDTO event4 = new EventDTO();
      event4.setName("event4");
      event4.setStartTime(hoursAgo4);
      event4.setEndTime(hoursAgo3);
      event4.setEventType(EventType.HOLIDAY.toString());
      Map<String, List<String>> eventDimensionMap4 = new HashMap<>();
      eventDimensionMap4.put("country_code", Lists.newArrayList("srilanka", "india"));
      event4.setTargetDimensionMap(eventDimensionMap3);
      eventDAO.save(event4);

      Assert.assertEquals(eventDAO.findAll().size(), 4);

      // invalid time
      EventFilter eventFilter = new EventFilter();
      List<EventDTO> events = holidayEventProvider.getEvents(eventFilter);
      Assert.assertEquals(events.size(), 0);

      // check that it gets all HOLIDAY events in time range, and only HOLIDAY events
      eventFilter.setStartTime(hoursAgo5);
      eventFilter.setEndTime(hoursAgo3);
      eventFilter.setEventType(EventType.HOLIDAY.name());
      events = holidayEventProvider.getEvents(eventFilter);
      Assert.assertEquals(events.size(), 2);

      // check for HOLIDAY events in time range and filters
      Map<String, List<String>> filterMap = new HashMap<>();
      filterMap.put("country_code", Lists.newArrayList("india"));
      eventFilter.setTargetDimensionMap(filterMap);
      events = holidayEventProvider.getEvents(eventFilter);
      Assert.assertEquals(events.size(), 1);
    }
  }
}
