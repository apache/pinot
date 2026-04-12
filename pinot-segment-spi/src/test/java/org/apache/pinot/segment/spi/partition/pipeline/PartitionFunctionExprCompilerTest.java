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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.ToIntFunction;
import org.apache.pinot.segment.spi.function.FunctionEvaluator;
import org.apache.pinot.segment.spi.function.scalar.PartitionFunctionExprRacyTestFunctions;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.data.readers.GenericRow;
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
  public void testCompileHexStringPipeline() {
    PartitionPipeline pipeline = PartitionFunctionExprCompiler.compile("raw_key", "  MD5 ( raw_key ) ");

    assertEquals(pipeline.getCanonicalFunctionExpr(), "md5(raw_key)");
    assertEquals(pipeline.getOutputType(), PartitionValueType.STRING);
    assertEquals(pipeline.evaluate("hello").getStringValue(), "5d41402abc4b2a76b9719d911017c592");
  }

  @Test
  public void testCompileRawBytesPipeline() {
    PartitionPipeline pipeline = PartitionFunctionExprCompiler.compile("raw_key", "  MD5_RAW ( raw_key ) ");

    assertEquals(pipeline.getCanonicalFunctionExpr(), "md5_raw(raw_key)");
    assertEquals(pipeline.getOutputType(), PartitionValueType.BYTES);
    assertEquals(BytesUtils.toHexString(pipeline.evaluate("hello").getBytesValue()),
        "5d41402abc4b2a76b9719d911017c592");
  }

  @Test
  public void testCompilePartitionFunctionForMd5Fnv() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("raw_key", "fnv1a_32(md5(raw_key))", 64);

    String digestHex = BytesUtils.toHexString(md5("Pinot".getBytes(UTF_8)));
    int expectedHash = FnvHashFunctions.fnv1aHash32(digestHex.getBytes(UTF_8));
    int expectedPartition = positiveModulo(expectedHash, 64);
    assertEquals(partitionFunction.getPartition("Pinot"), expectedPartition);
    assertEquals(partitionFunction.getFunctionExpr(), "fnv1a_32(md5(raw_key))");
    assertEquals(partitionFunction.getPartitionIdNormalizer(), "POSITIVE_MODULO");
  }

  @Test
  public void testCompilePartitionFunctionForMd5FnvWithExactUuidUsingMaskNormalizer() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("id", "fnv1a_32(md5(id))", 128, "MASK");

    assertEquals(partitionFunction.getPartition("000016be-9d72-466c-9632-cfa680dc8fa3"), 104);
    assertEquals(partitionFunction.getPartitionIdNormalizer(), "MASK");
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
    assertEquals(partitionFunction.getFunctionExpr(), "bucket(timestampMillis, 1000)");
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
    assertEquals(pipeline.getIntNormalizer(), PartitionIntNormalizer.POSITIVE_MODULO);
    assertEquals(partitionFunction.getPartition(value), positiveModulo(hash, numPartitions));
    assertEquals(partitionFunction.getPartitionIdNormalizer(), "POSITIVE_MODULO");
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
    assertEquals(partitionFunction.getPartitionIdNormalizer(), "MASK");
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
    assertEquals(partitionFunction.getPartitionIdNormalizer(), "ABS");
  }

  @Test
  public void testRejectsWrongColumnReference() {
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> PartitionFunctionExprCompiler.compile("raw_key", "md5(other_key)"));
    assertEquals(error.getMessage(),
        "Partition function expression must reference only partition column 'raw_key', got 'other_key'");
  }

  @Test
  public void testRejectsUnsupportedFunction() {
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> PartitionFunctionExprCompiler.compile("raw_key", "sha256(raw_key)"));
    assertEquals(error.getMessage(), "Unsupported partition scalar function: sha256");
  }

  @Test
  public void testRejectsNonIntPartitionFunctionOutput() {
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> PartitionFunctionExprCompiler.compilePartitionFunction("raw_key", "md5(raw_key)", 16));
    assertEquals(error.getMessage(), "Partition pipeline must produce INT or LONG output, got: STRING");
  }

  @Test
  public void testRejectsInvalidGrammar() {
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> PartitionFunctionExprCompiler.compile("raw_key", "fnv1a_32(md5(raw_key), lower(raw_key))"));
    assertEquals(error.getMessage(),
        "Partition function expression must reference partition column 'raw_key' through a single argument chain");
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

  @Test
  public void testRejectsConstantFunctionArgument() {
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> PartitionFunctionExprCompiler.compilePartitionFunction("timestampMillis",
            "bucket(timestampMillis, identity(1000))", 128));
    assertEquals(error.getMessage(),
        "Partition function expression only supports literal constants, got function subexpression: identity(1000)");
  }

  @Test(timeOut = 10_000L)
  public void testNonStaticScalarFunctionsUseThreadLocalTargets()
      throws Exception {
    PartitionFunctionExprRacyTestFunctions.reset();
    PartitionPipeline pipeline = PartitionFunctionExprCompiler.compile("raw_key", "racy_echo(raw_key)");
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    try {
      Future<String> firstResult = executorService.submit(() -> pipeline.evaluate("first").getStringValue());
      Future<String> secondResult = executorService.submit(() -> pipeline.evaluate("second").getStringValue());

      assertEquals(firstResult.get(5, TimeUnit.SECONDS), "first");
      assertEquals(secondResult.get(5, TimeUnit.SECONDS), "second");
    } finally {
      executorService.shutdownNow();
    }
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
