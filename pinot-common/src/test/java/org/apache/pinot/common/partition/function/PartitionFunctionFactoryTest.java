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
package org.apache.pinot.common.partition.function;

import java.util.Map;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.PartitionIntNormalizer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Coverage for [PartitionFunctionFactory] dynamic registry behavior.
public class PartitionFunctionFactoryTest {

  @Test
  public void testAllBuiltInFunctionsRegistered() {
    // Resolves every built-in name. The test fails if classpath-scan misses any annotated impl.
    assertTrue(
        PartitionFunctionFactory.getPartitionFunction("Modulo", 4, null) instanceof ModuloPartitionFunction);
    assertTrue(
        PartitionFunctionFactory.getPartitionFunction("Murmur", 4, null) instanceof MurmurPartitionFunction);
    assertTrue(
        PartitionFunctionFactory.getPartitionFunction("Murmur2", 4, null) instanceof MurmurPartitionFunction);
    assertTrue(
        PartitionFunctionFactory.getPartitionFunction("Murmur3", 4, null) instanceof Murmur3PartitionFunction);
    assertTrue(
        PartitionFunctionFactory.getPartitionFunction("Fnv", 4, null) instanceof FnvPartitionFunction);
    assertTrue(
        PartitionFunctionFactory.getPartitionFunction("HashCode", 4, null) instanceof HashCodePartitionFunction);
    assertTrue(
        PartitionFunctionFactory.getPartitionFunction("ByteArray", 4, null) instanceof ByteArrayPartitionFunction);
    assertTrue(
        PartitionFunctionFactory.getPartitionFunction("BoundedColumnValue", 2,
            Map.of("columnValues", "a", "columnValuesDelimiter", "|"))
            instanceof BoundedColumnValuePartitionFunction);
  }

  @Test
  public void testCaseInsensitiveLookup() {
    // Names are matched after lower-casing. Both spellings resolve to the same impl class.
    PartitionFunction lowerCase = PartitionFunctionFactory.getPartitionFunction("murmur3", 8, null);
    PartitionFunction mixedCase = PartitionFunctionFactory.getPartitionFunction("MuRmUr3", 8, null);
    assertEquals(lowerCase.getClass(), Murmur3PartitionFunction.class);
    assertEquals(mixedCase.getClass(), Murmur3PartitionFunction.class);
  }

  @Test
  public void testMurmurAndMurmur2AliasResolveToSameClass() {
    // Aliases declared in @PartitionFunctionType(names = {"Murmur", "Murmur2"}) both register.
    assertEquals(PartitionFunctionFactory.getPartitionFunction("Murmur", 4, null).getClass(),
        PartitionFunctionFactory.getPartitionFunction("Murmur2", 4, null).getClass());
  }

  @Test
  public void testUnknownNameThrows() {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
        () -> PartitionFunctionFactory.getPartitionFunction("DoesNotExist", 4, null));
    assertTrue(e.getMessage().contains("DoesNotExist"));
  }

  @Test
  public void testInitIsIdempotent() {
    // Multiple components in the same JVM (e.g. controller + embedded broker in a quickstart) call
    // init() independently. Repeated calls must not blow up.
    PartitionFunctionFactory.init();
    PartitionFunctionFactory.init();
    PartitionFunctionFactory.init();
    assertTrue(
        PartitionFunctionFactory.getPartitionFunction("Modulo", 4, null) instanceof ModuloPartitionFunction);
  }

  @Test
  public void testGetPartitionIdNormalizerPerImpl() {
    // Locks the descriptive normalizer label that each built-in impl reports.
    assertEquals(new ModuloPartitionFunction(4, null).getPartitionIdNormalizer(),
        PartitionIntNormalizer.POSITIVE_MODULO.name());
    assertEquals(new MurmurPartitionFunction(4, null).getPartitionIdNormalizer(),
        PartitionIntNormalizer.MASK.name());
    assertEquals(new Murmur3PartitionFunction(4, null).getPartitionIdNormalizer(),
        PartitionIntNormalizer.MASK.name());
    assertEquals(new HashCodePartitionFunction(4, null).getPartitionIdNormalizer(),
        PartitionIntNormalizer.ABS.name());
    assertEquals(new ByteArrayPartitionFunction(4, null).getPartitionIdNormalizer(),
        PartitionIntNormalizer.ABS.name());
    // FNV defaults to MASK; ABS is reachable via config.
    assertEquals(new FnvPartitionFunction(4, null).getPartitionIdNormalizer(),
        PartitionIntNormalizer.MASK.name());
    assertEquals(new FnvPartitionFunction(4, Map.of("negativePartitionHandling", "abs")).getPartitionIdNormalizer(),
        PartitionIntNormalizer.ABS.name());
    // BoundedColumnValue does not map onto any standard normalizer.
    PartitionFunction boundedColumnValue = new BoundedColumnValuePartitionFunction(2,
        Map.of("columnValues", "a", "columnValuesDelimiter", "|"));
    assertNull(boundedColumnValue.getPartitionIdNormalizer());
  }
}
