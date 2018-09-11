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

package com.linkedin.thirdeye.anomaly.events;

import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;

import java.util.List;

public class HolidayEventProvider implements EventDataProvider<EventDTO> {
  private EventManager eventDAO = DAORegistry.getInstance().getEventDAO();

  @Override
  public List<EventDTO> getEvents(EventFilter eventFilter) {

    List<EventDTO> allEventsBetweenTimeRange =
        eventDAO.findEventsBetweenTimeRange(EventType.HOLIDAY.name(), eventFilter.getStartTime(),
            eventFilter.getEndTime());

    List<EventDTO> holidayEvents = EventFilter.applyDimensionFilter(allEventsBetweenTimeRange, eventFilter.getTargetDimensionMap());
    return holidayEvents;
  }

  @Override
  public String getEventType() {
    return EventType.HOLIDAY.toString();
  }

}
