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
package org.apache.pinot.minion.api.resources;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Map;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.minion.event.MinionProgressObserver;
import org.apache.pinot.minion.event.MinionTaskState;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PinotTaskProgressResourceTest {

  @Test
  public void testGetSubtaskWithGivenStateProgress()
      throws IOException {
    MinionEventObserver observer1 = new MinionProgressObserver();
    observer1.notifyTaskStart(null);
    MinionEventObservers.getInstance().addMinionEventObserver("t01", observer1);

    MinionEventObserver observer2 = new MinionProgressObserver();
    observer2.notifyProgress(null, "");
    MinionEventObservers.getInstance().addMinionEventObserver("t02", observer2);

    MinionEventObserver observer3 = new MinionProgressObserver();
    observer3.notifyTaskSuccess(null, "");
    MinionEventObservers.getInstance().addMinionEventObserver("t03", observer3);

    PinotTaskProgressResource pinotTaskProgressResource = new PinotTaskProgressResource();
    String subtaskWithInProgressState =
        pinotTaskProgressResource.getSubtaskWithGivenStateProgress(MinionTaskState.IN_PROGRESS.toString());
    assertInProgressSubtasks(subtaskWithInProgressState);

    String subtaskWithUndefinedState =
        pinotTaskProgressResource.getSubtaskWithGivenStateProgress("Undefined");
    assertInProgressSubtasks(subtaskWithUndefinedState);

    String subtaskWithSucceededState =
        pinotTaskProgressResource.getSubtaskWithGivenStateProgress(MinionTaskState.SUCCEEDED.toString());
    JsonNode jsonNode = JsonUtils.stringToJsonNode(subtaskWithSucceededState);
    Map<String, Object> subtaskProgressMap = JsonUtils.jsonNodeToMap(jsonNode);
    assertEquals(subtaskProgressMap.size(), 1);

    String subtaskWithUnknownState =
        pinotTaskProgressResource.getSubtaskWithGivenStateProgress(MinionTaskState.UNKNOWN.toString());
    assertNoSubtaskWithTheGivenState(subtaskWithUnknownState);

    String subtaskWithCancelledState =
        pinotTaskProgressResource.getSubtaskWithGivenStateProgress(MinionTaskState.UNKNOWN.toString());
    assertNoSubtaskWithTheGivenState(subtaskWithCancelledState);

    String subtaskWithErrorState =
        pinotTaskProgressResource.getSubtaskWithGivenStateProgress(MinionTaskState.UNKNOWN.toString());
    assertNoSubtaskWithTheGivenState(subtaskWithErrorState);
  }

  private void assertInProgressSubtasks(String subtaskWithInProgressState)
      throws IOException {
    JsonNode jsonNode = JsonUtils.stringToJsonNode(subtaskWithInProgressState);
    Map<String, Object> subtaskProgressMap = JsonUtils.jsonNodeToMap(jsonNode);
    assertEquals(subtaskProgressMap.size(), 2);
  }

  private void assertNoSubtaskWithTheGivenState(String subtaskWithTheGivenState)
      throws IOException {
    JsonNode jsonNode = JsonUtils.stringToJsonNode(subtaskWithTheGivenState);
    Map<String, Object> subtaskProgressMap = JsonUtils.jsonNodeToMap(jsonNode);
    assertEquals(subtaskProgressMap.size(), 0);
  }
}
