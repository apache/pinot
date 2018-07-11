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
package com.linkedin.pinot.minion.events;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Registry for all {@link MinionEventObserverFactory}.
 */
public class EventObserverFactoryRegistry {
  private final Map<String, MinionEventObserverFactory> _eventObserverFactoryRegistry = new HashMap<>();

  /**
   * Registers an event observer factory.
   *
   * @param taskType Task type
   * @param eventObserverFactory Event observer factory associated with the task type
   */
  public void registerEventObserverFactory(@Nonnull String taskType,
      @Nonnull MinionEventObserverFactory eventObserverFactory) {
    _eventObserverFactoryRegistry.put(taskType, eventObserverFactory);
  }

  /**
   * Returns the event observer factory for the given task type.
   *
   * @param taskType Task type
   * @return Event observer factory associated with the given task type, or default event observer if no one registered
   */
  public MinionEventObserverFactory getEventObserverFactory(@Nonnull String taskType) {
    return _eventObserverFactoryRegistry.getOrDefault(taskType, DefaultMinionEventObserverFactory.getInstance());
  }
}
