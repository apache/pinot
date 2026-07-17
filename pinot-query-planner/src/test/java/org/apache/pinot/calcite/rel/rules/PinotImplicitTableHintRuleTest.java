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
package org.apache.pinot.calcite.rel.rules;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Tests for {@link PinotImplicitTableHintRule}, verifying the table options hint that ends up on the table scan after
 * partition hint inference.
 */
public class PinotImplicitTableHintRuleTest extends QueryEnvironmentTestBase {

  @Test
  public void testInferenceSkippedForReplicatedTableScan() {
    // Table 'b' is hinted as replicated, so partition hint inference should leave its hint untouched (no partition
    // options injected) even though 'b' is a partitioned table. Table 'a' has no explicit hint, so inference should
    // add the partition options to its scan.
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(
        "SET inferPartitionHint=true; SELECT /*+ joinOptions(left_distribution_type='local', "
            + "right_distribution_type='local') */ a.col1, b.col2 FROM a JOIN b "
            + "/*+ tableOptions(is_replicated = 'true') */ ON a.col1 = b.col1");

    Map<String, String> replicatedScanOptions = getTableOptions(dispatchableSubPlan, "b");
    assertEquals(replicatedScanOptions, Map.of(PinotHintOptions.TableHintOptions.IS_REPLICATED, "true"));

    Map<String, String> inferredScanOptions = getTableOptions(dispatchableSubPlan, "a");
    assertEquals(inferredScanOptions, Map.of(
        PinotHintOptions.TableHintOptions.PARTITION_KEY, "col2",
        PinotHintOptions.TableHintOptions.PARTITION_FUNCTION, "Hashcode",
        PinotHintOptions.TableHintOptions.PARTITION_SIZE, String.valueOf(PARTITION_COUNT)));
  }

  @Test
  public void testExplicitIsReplicatedFalseCarriedOverDuringInference() {
    // An explicit is_replicated='false' should not skip inference, and should be carried over into the rebuilt hint.
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(
        "SET inferPartitionHint=true; SELECT * FROM a /*+ tableOptions(is_replicated = 'false') */ LIMIT 10");

    Map<String, String> tableOptions = getTableOptions(dispatchableSubPlan, "a");
    assertEquals(tableOptions, Map.of(
        PinotHintOptions.TableHintOptions.PARTITION_KEY, "col2",
        PinotHintOptions.TableHintOptions.PARTITION_FUNCTION, "Hashcode",
        PinotHintOptions.TableHintOptions.PARTITION_SIZE, String.valueOf(PARTITION_COUNT),
        PinotHintOptions.TableHintOptions.IS_REPLICATED, "false"));
  }

  @Test
  public void testTableOptionsModelsAllTableHintOptions()
      throws IllegalAccessException {
    // PinotImplicitTableHintRule rebuilds the table options hint entirely from TableOptions, so every option defined
    // in PinotHintOptions.TableHintOptions must be modeled by TableOptions and emitted in withNewTableOptions —
    // otherwise an explicitly supplied value for the unmodeled option would be silently dropped when the hint is
    // rebuilt with the inferred partition options.
    Set<String> definedOptions = new HashSet<>();
    for (Field field : PinotHintOptions.TableHintOptions.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
        definedOptions.add((String) field.get(null));
      }
    }
    Set<String> modeledOptions = Set.of(PinotHintOptions.TableHintOptions.PARTITION_KEY,
        PinotHintOptions.TableHintOptions.PARTITION_FUNCTION, PinotHintOptions.TableHintOptions.PARTITION_SIZE,
        PinotHintOptions.TableHintOptions.PARTITION_PARALLELISM, PinotHintOptions.TableHintOptions.IS_REPLICATED);
    assertEquals(definedOptions, modeledOptions,
        "A new table hint option must be modeled in TableOptions and emitted in "
            + "PinotImplicitTableHintRule#withNewTableOptions (then added to this test), or it will be silently "
            + "dropped from explicit hints when partition hint inference is enabled");
  }

  private static Map<String, String> getTableOptions(DispatchableSubPlan dispatchableSubPlan, String tableName) {
    for (DispatchablePlanFragment fragment : dispatchableSubPlan.getQueryStageMap().values()) {
      TableScanNode tableScanNode = findTableScan(fragment.getPlanFragment().getFragmentRoot(), tableName);
      if (tableScanNode != null) {
        Map<String, String> tableOptions =
            tableScanNode.getNodeHint().getHintOptions().get(PinotHintOptions.TABLE_HINT_OPTIONS);
        assertNotNull(tableOptions, "No table options hint found on scan of table: " + tableName);
        return tableOptions;
      }
    }
    throw new AssertionError("No table scan found for table: " + tableName);
  }

  private static TableScanNode findTableScan(PlanNode node, String tableName) {
    if (node instanceof TableScanNode && ((TableScanNode) node).getTableName().equals(tableName)) {
      return (TableScanNode) node;
    }
    for (PlanNode input : node.getInputs()) {
      TableScanNode found = findTableScan(input, tableName);
      if (found != null) {
        return found;
      }
    }
    return null;
  }
}
