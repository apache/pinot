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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.util.CancelFlag;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Checks when [LogicalPlanner#checkCancel()] stops planning.
public class LogicalPlannerTest {
  private static final HepProgram EMPTY_PROGRAM = HepProgram.builder().build();

  @Test
  public void testCheckCancelThrowsWhenThreadIsInterrupted() {
    LogicalPlanner planner = new LogicalPlanner(EMPTY_PROGRAM, Contexts.empty());
    planner.checkCancel();

    Thread.currentThread().interrupt();
    try {
      assertThrows(EarlyTerminationException.class, planner::checkCancel);
      assertTrue(Thread.currentThread().isInterrupted(), "Interrupt status must stay set");
    } finally {
      // Clear the interrupt status so that it does not leak into other tests
      Thread.interrupted();
    }
  }

  @Test
  public void testCheckCancelThrowsWhenCancelFlagIsSet() {
    CancelFlag cancelFlag = new CancelFlag(new AtomicBoolean());
    LogicalPlanner planner = new LogicalPlanner(EMPTY_PROGRAM, Contexts.of(cancelFlag));
    planner.checkCancel();

    cancelFlag.requestCancel();
    assertThrows(CalciteException.class, planner::checkCancel);
  }
}
