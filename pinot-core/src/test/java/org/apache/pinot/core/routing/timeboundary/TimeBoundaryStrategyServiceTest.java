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
package org.apache.pinot.core.routing.timeboundary;

import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


public class TimeBoundaryStrategyServiceTest {

  @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "No TimeBoundaryStrategy found for name: invalidStrategy")
  public void testInvalidTimeBoundaryStrategy() {
    TimeBoundaryStrategyService.getInstance().getTimeBoundaryStrategy("invalidStrategy");
  }

  @Test
  public void testMinTimeBoundaryStrategy() {
    TimeBoundaryStrategy timeBoundaryStrategy = TimeBoundaryStrategyService.getInstance()
        .getTimeBoundaryStrategy("min");
    assertTrue(timeBoundaryStrategy instanceof MinTimeBoundaryStrategy, "Expected MinTimeBoundaryStrategy instance");
  }
}
