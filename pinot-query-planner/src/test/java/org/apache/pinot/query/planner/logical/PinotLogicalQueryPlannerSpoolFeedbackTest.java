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
package org.apache.pinot.query.planner.logical;

import java.util.List;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PinotLogicalQueryPlannerSpoolFeedbackTest {

  @Test
  public void feedbackListsEverySubstitution()
      throws Exception {
    List<SpoolSubstitution> substitutions = List.of(
        new SpoolSubstitution(4, 5, 3, SpoolSubstitution.Outcome.APPLIED, null),
        new SpoolSubstitution(6, 7, 3, SpoolSubstitution.Outcome.REJECTED_BY_GUARD, "some reason"));
    String expected = "{\"mode\":\"auto\",\"substitutions\":["
        + "{\"receiverStage\":4,\"replacedStage\":5,\"leaderStage\":3,\"outcome\":\"APPLIED\"},"
        + "{\"receiverStage\":6,\"replacedStage\":7,\"leaderStage\":3,\"outcome\":\"REJECTED_BY_GUARD\","
        + "\"reason\":\"some reason\"}]}";
    assertEquals(PinotLogicalQueryPlanner.toFeedback(SpoolMode.AUTO, substitutions),
        JsonUtils.stringToJsonNode(expected));
  }

  @Test
  public void feedbackReportsTheModeWhenSpoolsAreOff()
      throws Exception {
    assertEquals(PinotLogicalQueryPlanner.toFeedback(SpoolMode.OFF, List.of()),
        JsonUtils.stringToJsonNode("{\"mode\":\"off\",\"substitutions\":[]}"));
  }
}
