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
package org.apache.pinot.segment.spi.partition.pipeline;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.function.ToIntFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.hash.FnvHashFunctions;
import org.apache.pinot.spi.utils.hash.MurmurHashFunctions;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class PartitionFunctionExprCompilerTest {
  @Test
  public void testCompilePartitionFunctionForMd5Fnv() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("raw_key", "fnv1a_32(md5(raw_key))", 64);

    String digestHex = BytesUtils.toHexString(md5("Pinot".getBytes(UTF_8)));
    int expectedHash = FnvHashFunctions.fnv1aHash32(digestHex.getBytes(UTF_8));
    int expectedPartition = positiveModulo(expectedHash, 64);
    assertEquals(partitionFunction.getPartition("Pinot"), expectedPartition);
    assertEquals(partitionFunction.getFunctionExpr(), "fnv1a_32(md5(raw_key))");
    assertEquals(partitionFunction.getPartitionIdNormalizer(), PartitionIdNormalizer.POSITIVE_MODULO);
  }

  @Test
  public void testCompilePartitionFunctionForMd5FnvWithExactUuidUsingMaskNormalizer() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("id", "fnv1a_32(md5(id))", 128, "MASK");

    assertEquals(partitionFunction.getPartition("000016be-9d72-466c-9632-cfa680dc8fa3"), 104);
    assertEquals(partitionFunction.getPartitionIdNormalizer(), PartitionIdNormalizer.MASK);
  }

  @Test
  public void testCompilePartitionFunctionForLowerMurmur2() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("raw_key", "murmur2(lower(raw_key))", 32);

    int expectedHash = MurmurHashFunctions.murmurHash2("hello".getBytes(UTF_8));
    assertEquals(partitionFunction.getPartition("HeLLo"), positiveModulo(expectedHash, 32));
  }

  @Test
  public void testCompilePartitionFunctionWithLiteralArgument() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("timestampMillis", "bucket(timestampMillis, 1000)", 128);

    assertEquals(partitionFunction.getPartition("54321"), 54);
    assertEquals(partitionFunction.getFunctionExpr(), "bucket(timestampmillis, 1000)");
  }

  @Test
  public void testCanonicalizationPreservesQuotedLiteralPayload() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("raw_key",
            "MURMUR2( CONCAT( raw_key , 'SALT Value' ) )", 64);

    int expectedHash = MurmurHashFunctions.murmurHash2("PinotSALT Value".getBytes(UTF_8));
    assertEquals(partitionFunction.getPartition("Pinot"), positiveModulo(expectedHash, 64));
    assertEquals(partitionFunction.getFunctionExpr(), "murmur2(concat(raw_key, 'SALT Value'))");
  }

  @Test
  public void testCompiledPartitionPipelineImplementsFunctionEvaluator() {
    FunctionEvaluator evaluator = PartitionFunctionExprCompiler.compile("raw_key", "murmur2(lower(raw_key))");
    GenericRow row = new GenericRow();
    row.putValue("raw_key", "HeLLo");

    assertEquals(evaluator.getArguments(), List.of("raw_key"));
    assertEquals(evaluator.evaluate(row), MurmurHashFunctions.murmurHash2("hello".getBytes(UTF_8)));
    assertEquals(evaluator.evaluate(new Object[]{"HeLLo"}), MurmurHashFunctions.murmurHash2("hello".getBytes(UTF_8)));
  }

  @Test
  public void testCompiledPartitionPipelineCoercesGenericRowValuesToString() {
    FunctionEvaluator evaluator = PartitionFunctionExprCompiler.compile("timestampMillis", "bucket(timestampMillis, "
        + "1000)");
    GenericRow row = new GenericRow();
    row.putValue("timestampMillis", 54321L);

    assertEquals(evaluator.evaluate(row), 54L);
    assertEquals(evaluator.evaluate(new Object[]{54321L}), 54L);
  }

  @Test
  public void testIntegralExpressionDefaultsToPositiveModulo() {
    int numPartitions = 13;
    PartitionPipeline pipeline = PartitionFunctionExprCompiler.compile("raw_key", "murmur2(raw_key)");
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("raw_key", "murmur2(raw_key)", numPartitions);
    PartitionFunction legacyPartitionFunction =
        PartitionFunctionFactory.getPartitionFunction("Murmur2", numPartitions, null);
    String value = findValueWithNegativeHash(input -> MurmurHashFunctions.murmurHash2(input.getBytes(UTF_8)));
    int hash = MurmurHashFunctions.murmurHash2(value.getBytes(UTF_8));

    assertTrue(hash < 0);
    assertNotEquals(positiveModulo(hash, numPartitions), legacyPartitionFunction.getPartition(value));
    assertEquals(pipeline.getIntNormalizer(), null);
    assertEquals(partitionFunction.getPartition(value), positiveModulo(hash, numPartitions));
    assertEquals(partitionFunction.getPartitionIdNormalizer(), PartitionIdNormalizer.POSITIVE_MODULO);
  }

  @Test
  public void testExplicitMaskNormalizerMatchesLegacyMurmur2Partitioning() {
    int numPartitions = 13;
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("raw_key", "identity(murmur2(raw_key))",
            numPartitions, "MASK");
    PartitionFunction legacyPartitionFunction =
        PartitionFunctionFactory.getPartitionFunction("Murmur2", numPartitions, null);
    String value = findValueWithNegativeHash(input -> MurmurHashFunctions.murmurHash2(input.getBytes(UTF_8)));

    assertEquals(partitionFunction.getPartition(value), legacyPartitionFunction.getPartition(value));
    assertEquals(partitionFunction.getPartitionIdNormalizer(), PartitionIdNormalizer.MASK);
  }

  @Test
  public void testExplicitAbsNormalizerUsesAbsoluteRemainder() {
    int numPartitions = 13;
    String value = findValueWithNegativeHash(input -> MurmurHashFunctions.murmurHash2(input.getBytes(UTF_8)));
    int hash = MurmurHashFunctions.murmurHash2(value.getBytes(UTF_8));
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("raw_key", "identity(murmur2(raw_key))",
            numPartitions, "ABS");

    assertTrue(hash < 0);
    assertEquals(partitionFunction.getPartition(value), absoluteModulo(hash, numPartitions));
    assertEquals(partitionFunction.getPartitionIdNormalizer(), PartitionIdNormalizer.ABS);
  }

  @Test
  public void testRejectsWrongColumnReference() {
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> PartitionFunctionExprCompiler.compile("raw_key", "md5(other_key)"));
    assertTrue(error.getMessage().contains("must reference exactly that column"),
        "Unexpected error: " + error.getMessage());
  }

  @Test
  public void testRejectsNonIntPartitionFunctionOutput() {
    // md5 returns a String; the error is thrown at evaluation time when normalizeResult() checks the return type
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("raw_key", "md5(raw_key)", 16);
    IllegalStateException error = expectThrows(IllegalStateException.class,
        () -> partitionFunction.getPartition("hello"));
    assertTrue(error.getMessage().contains("must return a numeric value"),
        "Unexpected error: " + error.getMessage());
  }

  @Test
  public void testRejectsNonDeterministicFunction() {
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> PartitionFunctionExprCompiler.compile("raw_key", "randomBucket(raw_key)"));
    assertEquals(error.getMessage(),
        "Partition scalar function 'randombucket' is not allowed because it is non-deterministic");
  }

  @Test
  public void testRejectsNonDeterministicAliasedFunction() {
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> PartitionFunctionExprCompiler.compile("raw_key", "cid(raw_key)"));
    assertEquals(error.getMessage(),
        "Partition scalar function 'cid' is not allowed because it is non-deterministic");
  }

  private static byte[] md5(byte[] input) {
    try {
      return MessageDigest.getInstance("MD5").digest(input);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 digest is not available", e);
    }
  }

  private static int positiveModulo(int value, int modulus) {
    int partition = value % modulus;
    return partition < 0 ? partition + modulus : partition;
  }

  private static int absoluteModulo(int value, int modulus) {
    int partition = value % modulus;
    return partition < 0 ? -partition : partition;
  }

  private static String findValueWithNegativeHash(ToIntFunction<String> hashFunction) {
    for (int i = 0; i < 10_000; i++) {
      String value = "value_" + i;
      if (hashFunction.applyAsInt(value) < 0) {
        return value;
      }
    }
    throw new IllegalStateException("Failed to find a value with a negative hash");
  }
}
