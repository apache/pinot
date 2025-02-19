/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.tasks;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Objects;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MinionTaskBaseObserverStatsTest {
  private static final String TEST_PROPERTY = "some test property";
  private static final String TASK_ID = "randomString";
  private static final String CURRENT_STATE = "IN_PROGRESS";
  private static final long TS = System.currentTimeMillis();
  private static final String STATUS = "task status";

  @Test
  public void testSerDeser()
      throws JsonProcessingException {
    TestObserverStats stats = (TestObserverStats) new TestObserverStats()
        .setTestProperty(TEST_PROPERTY)
        .setTaskId(TASK_ID)
        .setCurrentState(CURRENT_STATE)
        .setStartTimestamp(TS);
    stats.getProgressLogs().offer(new MinionTaskBaseObserverStats.StatusEntry(
        TS, MinionTaskBaseObserverStats.StatusEntry.LogLevel.INFO, STATUS));
    String statsString = getTestObjectString();
    TestObserverStats stats2 = stats.fromJsonString(statsString);
    Assert.assertEquals(stats2, stats);
  }

  private String getTestObjectString() {
    return "{\"testProperty\":\"" + TEST_PROPERTY + "\",\"startTimestamp\":" + TS
        + ",\"currentState\":\"" + CURRENT_STATE + "\",\"progressLogs\":[{\"level\":\"INFO\",\"status\":\"" + STATUS
        + "\",\"ts\":" + TS + "}],\"taskId\":\"" + TASK_ID + "\"}";
  }

  public static class TestObserverStats extends MinionTaskBaseObserverStats {
    private String _testProperty;

    public String getTestProperty() {
      return _testProperty;
    }

    public TestObserverStats setTestProperty(String testProperty) {
      _testProperty = testProperty;
      return this;
    }

    @Override
    public TestObserverStats fromJsonString(String statsJson)
        throws JsonProcessingException {
      return JsonUtils.stringToObject(statsJson, TestObserverStats.class);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
      return true;
    }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestObserverStats stats = (TestObserverStats) o;
      return super.equals(stats) && _testProperty.equals(stats.getTestProperty());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getTaskId(), getCurrentState(), getStartTimestamp(), getTestProperty());
    }
  }
}
