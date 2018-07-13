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
package com.linkedin.pinot.core.query.scheduler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.configuration.Configuration;

import static org.testng.Assert.*;


class TestSchedulerGroupFactory implements SchedulerGroupFactory {
  AtomicInteger numCalls = new AtomicInteger(0);
  ConcurrentHashMap<String, TestSchedulerGroup> groupMap = new ConcurrentHashMap<>();
  @Override
  public SchedulerGroup create(Configuration config, String groupName) {
    numCalls.incrementAndGet();
    assertNull(groupMap.get(groupName));
    TestSchedulerGroup group = new TestSchedulerGroup(groupName);
    groupMap.put(groupName, group);
    return group;
  }

  public void reset() {
    numCalls.set(0);
    groupMap.clear();
  }
}
