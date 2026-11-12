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
package org.apache.pinot.query.runtime.function;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.pinot.common.function.scalar.LogicalFunctions;
import org.apache.pinot.core.udf.Udf;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Guards the [Udf] service registrations against a scalar function signature changing underneath them.
///
/// A [Udf.FromAnnotatedMethod] subclass binds to its scalar function reflectively in its constructor, so changing
/// that method's signature makes the constructor throw. `ServiceLoader` turns that into a
/// `ServiceConfigurationError`, which fails every caller that iterates the SPI rather than only the provider at
/// fault, so one stale lookup disables UDF loading altogether.
public class UdfServiceLoaderTest {

  /// Iterating the SPI must instantiate every registered implementation. This is the failure mode a broken
  /// reflective lookup produces, and it covers implementations added later without listing them here.
  @Test
  public void testEveryRegisteredUdfCanBeInstantiated() {
    List<String> udfNames = new ArrayList<>();
    for (Udf udf : ServiceLoader.load(Udf.class)) {
      assertNotNull(udf.getMainName(), udf.getClass().getName() + " has no main name");
      udfNames.add(udf.getClass().getName());
    }
    assertFalse(udfNames.isEmpty(), "No Udf implementation was registered");
    assertTrue(udfNames.contains(NotUdf.class.getName()), "NotUdf is not registered: " + udfNames);
  }

  /// `LogicalFunctions.not` takes a nullable `Boolean`, and no primitive `boolean` overload exists, so `NotUdf`
  /// has to look up the boxed one. Constructing it is the assertion: the constructor resolves that method
  /// reflectively and throws `NoSuchMethodException` when the lookup names a signature that is not there.
  @Test
  public void testNotUdfBindsToTheNullableBooleanOverload()
      throws NoSuchMethodException {
    assertNotNull(LogicalFunctions.class.getMethod("not", Boolean.class));
    // Pin the reason the boxed lookup is required: the primitive overload the stale lookup asked for is gone.
    assertThrows(NoSuchMethodException.class, () -> LogicalFunctions.class.getMethod("not", boolean.class));

    NotUdf notUdf = new NotUdf();
    assertEquals(notUdf.getMainName(), "not");
    assertTrue(notUdf.getAllNames().contains("not"), "NotUdf must expose the 'not' name: " + notUdf.getAllNames());
  }
}
