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

import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class EquivalentStagesFinderTest extends StagesTestBase {

  private final DataSchema _dataSchema1 = new DataSchema(
      new String[]{"col1"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}
  );
  private final DataSchema _dataSchema2 = new DataSchema(
      new String[]{"col2"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}
  );

  @Test
  public void justScan() {
    MailboxSendNode root = when(tableScan("T1"));
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(root);
    assertEquals(result.toString(), "[[0]]");
  }

  @Test
  public void independentJoin() {
    when(
        join(
            exchange(1, tableScan("T1")),
            exchange(2, tableScan("T2"))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void sharedJoin() {
    when(
        join(
            exchange(1, tableScan("T1")),
            exchange(2, tableScan("T1"))
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2]]");
  }

  @Test
  public void sameHintsDontBreakEquivalence() {
    when(
        join(
            exchange(
                1,
                tableScan("T1")
                    .withHints("hint1", Map.of("key1", "value1"))
            ),
            exchange(
                2,
                tableScan("T1")
                    .withHints("hint1", Map.of("key1", "value1"))
            )
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2]]");
  }

  @Test
  public void differentHintsImplyNotEquivalent() {
    when(
        join(
            exchange(
                1,
                tableScan("T1")
                    .withHints("hint1", Map.of("key1", "value1"))
            ),
            exchange(
                2,
                tableScan("T1")
                    .withHints("hint1", Map.of("key1", "value2"))
            )
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void differentHintsOneNullImplyNotEquivalent() {
    when(
        join(
            exchange(1, tableScan("T1")),
            exchange(
                2,
                tableScan("T1")
                    .withHints("hint1", Map.of("key1", "value2"))
            )
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void differentDataSchemaBreakEquivalence() {
    when(
        join(
            exchange(1, tableScan("T1").withDataSchema(_dataSchema1)),
            exchange(
                2,
                tableScan("T1")
                    .withDataSchema(_dataSchema2)
            )
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void differentDataSchemaOneNullBreakEquivalence() {
    when(
        join(
            exchange(1, tableScan("T1")),
            exchange(
                2,
                tableScan("T1")
                    .withDataSchema(_dataSchema2)
            )
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1], [2]]");
  }

  @Test
  public void deepShared() {
    when(
        join(
            exchange(1,
                join(
                    exchange(3, tableScan("T1")),
                    exchange(4, tableScan("T1"))
                )
            ),
            exchange(2,
                join(
                    exchange(5, tableScan("T1")),
                    exchange(6, tableScan("T1"))
                )
            )
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2], [3, 4, 5, 6]]");
  }

  @Test
  public void deepSharedDifferentTables() {
    when(
        join(
            exchange(1,
                join(
                    exchange(3, tableScan("T1")),
                    exchange(4, tableScan("T2"))
                )
            ),
            exchange(2,
                join(
                    exchange(5, tableScan("T1")),
                    exchange(6, tableScan("T2"))
                )
            )
        )
    );
    GroupedStages result = EquivalentStagesFinder.findEquivalentStages(stage(0));
    assertEquals(result.toString(), "[[0], [1, 2], [3, 5], [4, 6]]");
  }
}
