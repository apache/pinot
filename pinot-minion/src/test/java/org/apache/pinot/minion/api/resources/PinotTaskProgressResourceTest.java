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

import java.io.IOException;
import java.util.Map;
import javax.ws.rs.WebApplicationException;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.minion.event.MinionProgressObserver;
import org.apache.pinot.minion.event.MinionTaskState;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class PinotTaskProgressResourceTest {

  @Test
  public void testGetGivenSubtaskOrStateProgress()
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

    // get all sub task progress
    String allSubTaskProgress = pinotTaskProgressResource.getSubtaskProgress(null, null);
    Map<String, Object> subtaskProgressMap = JsonUtils.stringToObject(allSubTaskProgress, Map.class);
    assertEquals(subtaskProgressMap.size(), 3);

    // get subtasks with given state
    String subtaskWithInProgressState =
        pinotTaskProgressResource.getSubtaskProgress(null, MinionTaskState.IN_PROGRESS.toString());
    assertInProgressSubtasks(subtaskWithInProgressState);

    String subtaskWithUndefinedState =
        pinotTaskProgressResource.getSubtaskProgress(null, "Undefined");
    assertInProgressSubtasks(subtaskWithUndefinedState);

    String subtaskWithSucceededState =
        pinotTaskProgressResource.getSubtaskProgress(null, MinionTaskState.SUCCEEDED.toString());
    subtaskProgressMap = JsonUtils.stringToObject(subtaskWithSucceededState, Map.class);
    assertEquals(subtaskProgressMap.size(), 1);

    String subtaskWithUnknownState =
        pinotTaskProgressResource.getSubtaskProgress(null, MinionTaskState.UNKNOWN.toString());
    assertNoSubtaskWithTheGivenState(subtaskWithUnknownState);

    String subtaskWithCancelledState =
        pinotTaskProgressResource.getSubtaskProgress(null, MinionTaskState.CANCELLED.toString());
    assertNoSubtaskWithTheGivenState(subtaskWithCancelledState);

    String subtaskWithErrorState =
        pinotTaskProgressResource.getSubtaskProgress(null, MinionTaskState.ERROR.toString());
    assertNoSubtaskWithTheGivenState(subtaskWithErrorState);

    // get subtasks with given name
    String subTasksWithGivenNamesProgress = pinotTaskProgressResource.getSubtaskProgress(" t01 , t02 ", null);
    assertInProgressSubtasks(subTasksWithGivenNamesProgress);

    // get subtasks with given names and state
    assertThrows(WebApplicationException.class,
        () -> pinotTaskProgressResource.getSubtaskProgress(" t01 , t02 ", MinionTaskState.IN_PROGRESS.toString()));
  }

  private void assertInProgressSubtasks(String subtaskWithInProgressState)
      throws IOException {
    Map<String, Object> subtaskProgressMap = JsonUtils.stringToObject(subtaskWithInProgressState, Map.class);
    assertEquals(subtaskProgressMap.size(), 2);
  }

  private void assertNoSubtaskWithTheGivenState(String subtaskWithTheGivenState)
      throws IOException {
    Map<String, Object> subtaskProgressMap = JsonUtils.stringToObject(subtaskWithTheGivenState, Map.class);
    assertEquals(subtaskProgressMap.size(), 0);
  }
}
