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

import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class EquivalentStagesReplacerTest extends StagesTestBase {

  @Test
  public void test() {
    when(// stage 0
        exchange(1,
          join(
              exchange(2,
                join(
                  exchange(3, tableScan("T1")),
                  exchange(4, tableScan("T2"))
                )
              ),
              exchange(5,
                join(
                  exchange(6, tableScan("T1")),
                  exchange(7, tableScan("T3"))
                )
              )
          )
        )
    );

    GroupedStages groupedStages = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(groupedStages.toString(), "[[0], [1], [2], [3, 6], [4], [5], [7]]");

    MailboxSendNode rootStage = stage(0);
    EquivalentStagesReplacer.replaceEquivalentStages(rootStage, groupedStages);

    cleanup();
    SpoolBuilder readT1 = new SpoolBuilder(3, tableScan("T1"));
    MailboxSendNode expected = when(// stage 0
        exchange(1,
            join(
                exchange(2,
                    join(
                        readT1.newReceiver(),
                        exchange(4, tableScan("T2"))
                    )
                ),
                exchange(5,
                    join(
                        readT1.newReceiver(),
                        exchange(7, tableScan("T3"))
                    )
                )
            )
        )
    );

    assertEqualPlan(rootStage, expected);
  }

  @Test
  void notUniqueReceiversInStage() {
    when(// stage 0
        exchange(1,
            join(
                exchange(2, tableScan("T1")),
                exchange(3, tableScan("T1"))
            )
        )
    );
    GroupedStages groupedStages = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(groupedStages.toString(), "[[0], [1], [2, 3]]");

    MailboxSendNode rootStage = stage(0);
    EquivalentStagesReplacer.replaceEquivalentStages(rootStage, groupedStages);

    cleanup();
    MailboxSendNode expected = when(// stage 0
        exchange(1,
            join(
                exchange(2, tableScan("T1")),
                exchange(3, tableScan("T1"))
            )
        )
    );
    assertEqualPlan(rootStage, expected);
  }

  @Test
  void groupSendingToTheSameStage() {
    when(// stage 0
        exchange(1,
            join(
                exchange(2, tableScan("T1")),
                exchange(3,
                    join(
                        exchange(4, tableScan("T1")),
                        exchange(5, tableScan("T1"))
                    )
                )
            )
        )
    );
    GroupedStages groupedStages = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(groupedStages.toString(), "[[0], [1], [2, 4, 5], [3]]");

    MailboxSendNode rootStage = stage(0);
    EquivalentStagesReplacer.replaceEquivalentStages(rootStage, groupedStages);

    cleanup();
    SpoolBuilder readT1 = new SpoolBuilder(2, tableScan("T1"));
    MailboxSendNode expected = when(// stage 0
        exchange(1,
            join(
                readT1.newReceiver(),
                exchange(3,
                    join(
                        readT1.newReceiver(),
                        exchange(5, tableScan("T1"))
                    )
                )
            )
        )
    );
    assertEqualPlan(rootStage, expected);
  }
}
