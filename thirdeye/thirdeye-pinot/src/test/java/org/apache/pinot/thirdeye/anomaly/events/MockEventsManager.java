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

import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.jdbc.AbstractManagerImpl;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.EventBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * The Mock events manager for testing
 */
public class MockEventsManager extends AbstractManagerImpl<EventDTO> implements EventManager {

  private Collection<EventDTO> entities;

  /**
   * Instantiates a new Mock events manager.
   *
   * @param entities the collection of entities
   */
  MockEventsManager(Collection<EventDTO> entities) {
    super(EventDTO.class, EventBean.class);
    this.entities = entities;
  }

  @Override
  public List<EventDTO> findAll() {
    return new ArrayList<>(entities);
  }

  @Override
  public Long save(EventDTO entity) {
    entities.add(entity);
    return entity.getId();
  }

  @Override
  public int update(EventDTO entity) {
    for (EventDTO eventDTO : entities) {
      if (eventDTO.getId().equals(entity.getId())) {
        eventDTO = entity;
        return 1;
      }
    }
    return 0;
  }

  @Override
  public int delete(EventDTO entity) {
    for (EventDTO eventDTO : entities) {
      if (eventDTO.getId().equals(entity.getId())) {
        entities.remove(eventDTO);
        return 1;
      }
    }
    return 0;
  }

  @Override
  public List<EventDTO> findByEventType(String eventType) {
    throw new AssertionError("not implemented");
  }

  @Override
  public List<EventDTO> findEventsBetweenTimeRange(String eventType, long startTime, long endTime) {
    throw new AssertionError("not implemented");
  }

  @Override
  public List<EventDTO> findEventsBetweenTimeRangeByName(String eventType, String name, long startTime, long endTime) {
    throw new AssertionError("not implemented");
  }
}
