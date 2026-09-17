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
package org.apache.pinot.common.function.scalar;

import java.sql.Timestamp;
import java.util.UUID;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Tests array construction behavior that depends on runtime Java element types.
public class ArrayFunctionsTest {

  @Test
  public void testBytesArrayValueConstructor() {
    byte[][] expected = {{0}, {1, 2}};

    Assert.assertEquals(ArrayFunctions.arrayValueConstructor(expected[0], expected[1]), expected);
  }

  @Test
  public void testTimestampArrayValueConstructor() {
    Timestamp[] expected = {new Timestamp(1000L), new Timestamp(2000L)};

    Assert.assertEquals(ArrayFunctions.arrayValueConstructor(expected[0], expected[1]), expected);
  }

  @Test
  public void testUuidArrayValueConstructor() {
    UUID[] expected = {UUID.randomUUID(), UUID.randomUUID()};

    Assert.assertEquals(ArrayFunctions.arrayValueConstructor(expected[0], expected[1]), expected);
  }
}
