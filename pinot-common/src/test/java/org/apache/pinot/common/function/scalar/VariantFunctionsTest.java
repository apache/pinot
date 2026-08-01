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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests strict and tolerant behavior exposed by the public scalar VARIANT function facade.
public class VariantFunctionsTest {
  @Test
  public void testScalarFunctionFacade() {
    byte[] variant = VariantFunctions.parseJsonToVariant(
        "{\"eventType\":\"click\",\"payload\":{\"count\":7},\"variantNull\":null}");

    assertEquals(VariantFunctions.variantGet(variant, "$.eventType", "STRING"), "click");
    assertEquals(VariantFunctions.variantToJson(VariantFunctions.variantGet(variant, "$.payload")), "{\"count\":7}");
    assertEquals(VariantFunctions.tryVariantGet(variant, "$.payload", "JSON"), "{\"count\":7}");
    assertEquals(VariantFunctions.variantToJson(VariantFunctions.tryVariantGet(variant, "$.eventType")), "\"click\"");
    assertTrue(VariantFunctions.variantExists(variant, "$.variantNull"));
    assertFalse(VariantFunctions.variantExists(variant, "$.missing"));
    assertFalse(VariantFunctions.isVariantNull(variant));
    assertTrue(VariantFunctions.isVariantNull(variant, "$.variantNull"));
    assertEquals(VariantFunctions.variantTypeOf(variant), "OBJECT");
    assertEquals(VariantFunctions.variantTypeOf(variant, "$.payload"), "OBJECT");
    assertEquals(VariantFunctions.variantToJson(variant),
        "{\"eventType\":\"click\",\"payload\":{\"count\":7},\"variantNull\":null}");

    byte[] tolerant = VariantFunctions.tryParseJsonToVariant("{\"value\":11}");
    assertEquals(VariantFunctions.variantGet(tolerant, "$.value", "INT"), 11);
    assertNull(VariantFunctions.tryParseJsonToVariant("{not-json"));
    assertNull(VariantFunctions.tryVariantGet(variant, "$.missing"));
    assertNull(VariantFunctions.tryVariantGet(variant, "$.missing", "STRING"));
  }
}
