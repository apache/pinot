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
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Coverage for [PartitionFunctionFactory] dynamic registry behavior.
public class PartitionFunctionFactoryTest {

  @Test
  public void testAllBuiltInFunctionsRegistered() {
    // Resolves every built-in name. The test fails if the classpath subtype scan misses any impl
    // or if its `getNames()` doesn't surface the expected canonical name.
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
    // MurmurPartitionFunction overrides getNames() to return ["Murmur", "Murmur2"] so both
    // names register against the same impl.
    assertEquals(PartitionFunctionFactory.getPartitionFunction("Murmur", 4, null).getClass(),
        PartitionFunctionFactory.getPartitionFunction("Murmur2", 4, null).getClass());
  }

  @Test
  public void testGetNamesDefaultsToSingletonOfGetName() {
    // The interface's default getNames() returns [getName()]. Verify a non-overriding impl
    // surfaces a single-entry list whose only entry equals getName().
    Murmur3PartitionFunction fn = new Murmur3PartitionFunction(4, null);
    assertEquals(fn.getNames(), java.util.List.of(fn.getName()));
  }

  @Test
  public void testMurmurOverridesGetNamesWithTwoAliases() {
    // MurmurPartitionFunction is the only built-in that overrides getNames(); verify the
    // override surfaces both aliases.
    MurmurPartitionFunction fn = new MurmurPartitionFunction(4, null);
    assertEquals(fn.getNames(), java.util.List.of("Murmur", "Murmur2"));
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
        PartitionIdNormalizer.POSITIVE_MODULO);
    assertEquals(new MurmurPartitionFunction(4, null).getPartitionIdNormalizer(),
        PartitionIdNormalizer.MASK);
    assertEquals(new Murmur3PartitionFunction(4, null).getPartitionIdNormalizer(),
        PartitionIdNormalizer.MASK);
    assertEquals(new HashCodePartitionFunction(4, null).getPartitionIdNormalizer(),
        PartitionIdNormalizer.PRE_MODULO_ABS);
    assertEquals(new ByteArrayPartitionFunction(4, null).getPartitionIdNormalizer(),
        PartitionIdNormalizer.PRE_MODULO_ABS);
    // FNV defaults to MASK; any normalizer is selectable through the partitionIdNormalizer config.
    assertEquals(new FnvPartitionFunction(4, null).getPartitionIdNormalizer(),
        PartitionIdNormalizer.MASK);
    assertEquals(new FnvPartitionFunction(4, Map.of("partitionIdNormalizer", "abs")).getPartitionIdNormalizer(),
        PartitionIdNormalizer.ABS);
    // BoundedColumnValue's output is already in [0, N); reports NO_OP (identity).
    PartitionFunction boundedColumnValue = new BoundedColumnValuePartitionFunction(2,
        Map.of("columnValues", "a", "columnValuesDelimiter", "|"));
    assertEquals(boundedColumnValue.getPartitionIdNormalizer(), PartitionIdNormalizer.NO_OP);
  }

  @Test
  public void testPartitionIdNormalizerConfigOverridesDefaultAcrossImpls() {
    // Every impl exposes the same `partitionIdNormalizer` config key. Verify that overriding the
    // default rewires the actual partition-id computation (not just the reported label).
    Map<String, String> mask = Map.of("partitionIdNormalizer", "MASK");

    // HashCode: configured normalizer drives the output. Pick a value whose hashCode is negative
    // (sweep until found) so PRE_MODULO_ABS vs MASK produces observably different partition ids.
    String negativeHashValue = null;
    int negativeHash = 0;
    for (int i = 0; i < 1000 && negativeHashValue == null; i++) {
      String candidate = "value-" + i;
      if (candidate.hashCode() < 0) {
        negativeHashValue = candidate;
        negativeHash = candidate.hashCode();
      }
    }
    assertTrue(negativeHashValue != null, "Failed to find a string with a negative hashCode in the search range");
    assertEquals(new HashCodePartitionFunction(8, null).getPartition(negativeHashValue),
        PartitionIdNormalizer.PRE_MODULO_ABS.getPartitionId(negativeHash, 8));
    assertEquals(new HashCodePartitionFunction(8, mask).getPartition(negativeHashValue),
        PartitionIdNormalizer.MASK.getPartitionId(negativeHash, 8));

    // Modulo: explicit MASK on a negative input differs from the default POSITIVE_MODULO output.
    long signedValue = -10L;
    int posMod = new ModuloPartitionFunction(7, null).getPartition(Long.toString(signedValue));
    int maskMod = new ModuloPartitionFunction(7, mask).getPartition(Long.toString(signedValue));
    assertEquals(posMod, PartitionIdNormalizer.POSITIVE_MODULO.getPartitionId(signedValue, 7));
    assertEquals(maskMod, PartitionIdNormalizer.MASK.getPartitionId(signedValue, 7));

    // ByteArray: PRE_MODULO_ABS default; verify the override label round-trips on the SPI.
    PartitionFunction byteArrayWithKafkaAbs = new ByteArrayPartitionFunction(4,
        Map.of("partitionIdNormalizer", "PRE_MODULO_ABS"));
    assertEquals(byteArrayWithKafkaAbs.getPartitionIdNormalizer(), PartitionIdNormalizer.PRE_MODULO_ABS);
  }
}
