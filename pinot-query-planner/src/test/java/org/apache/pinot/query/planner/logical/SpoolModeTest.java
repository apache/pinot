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

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class SpoolModeTest {

  @DataProvider
  public static Object[][] validValues() {
    return new Object[][]{
        {"off", SpoolMode.OFF}, {"OFF", SpoolMode.OFF}, {"false", SpoolMode.OFF}, {"False", SpoolMode.OFF},
        {"auto", SpoolMode.AUTO}, {" Auto ", SpoolMode.AUTO},
        {"on", SpoolMode.ON}, {"true", SpoolMode.ON}, {"TRUE", SpoolMode.ON}
    };
  }

  @Test(dataProvider = "validValues")
  public void fromString(String value, SpoolMode expected) {
    assertEquals(SpoolMode.fromString(value), expected);
  }

  @Test
  public void fromStringRejectsUnknownValues() {
    assertThrows(IllegalArgumentException.class, () -> SpoolMode.fromString("yes"));
    assertThrows(IllegalArgumentException.class, () -> SpoolMode.fromString(""));
  }

  @Test
  public void flags() {
    assertFalse(SpoolMode.OFF.isEnabled());
    assertTrue(SpoolMode.AUTO.isEnabled());
    assertTrue(SpoolMode.ON.isEnabled());
    assertTrue(SpoolMode.AUTO.isGuarded());
    assertFalse(SpoolMode.ON.isGuarded());
  }
}
