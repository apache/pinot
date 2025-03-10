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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.calcite.rel.logical.PinotLogicalTableScan;


public class PinotTableScanConverterRule extends RelOptRule {
  public static final PinotTableScanConverterRule INSTANCE =
      new PinotTableScanConverterRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotTableScanConverterRule(RelBuilderFactory factory) {
    super(operand(TableScan.class, none()), factory, null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    TableScan tableScan = call.rel(0);
    if (tableScan instanceof LogicalTableScan) {
      call.transformTo(PinotLogicalTableScan.create(call.rel(0)));
    } else if (!(tableScan instanceof PinotLogicalTableScan)) {
      throw new IllegalStateException("Unknown table scan in PinotTableScanConverterRule: " + tableScan.getClass());
    }
  }
}
