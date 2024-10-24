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

import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class GroupedStagesTest extends StagesTestBase {

  @Test
  public void addOrdered() {
    plan(
        join(
            exchange(1, tableScan("T1")),
            exchange(2, tableScan("T2"))
        )
    );

    GroupedStages.Mutable mutable = new GroupedStages.Mutable()
        .addNewGroup(stage(0))
        .addNewGroup(stage(1))
        .addNewGroup(stage(2));
    assertEquals(mutable.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void addUnordered() {
    plan(
        join(
            exchange(1, tableScan("T1")),
            exchange(2, tableScan("T2"))
        )
    );
    GroupedStages.Mutable mutable = new GroupedStages.Mutable()
        .addNewGroup(stage(2))
        .addNewGroup(stage(1))
        .addNewGroup(stage(0));
    assertEquals(mutable.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void addEquivalence() {
    plan(
        join(
            exchange(1, tableScan("T1")),
            exchange(2, tableScan("T2"))
        )
    );
    GroupedStages.Mutable mutable = new GroupedStages.Mutable()
        .addNewGroup(stage(0))
        .addNewGroup(stage(1))
        .addToGroup(stage(0), stage(2));
    assertEquals(mutable.toString(), "[[0, 2], [1]]");
  }
}
