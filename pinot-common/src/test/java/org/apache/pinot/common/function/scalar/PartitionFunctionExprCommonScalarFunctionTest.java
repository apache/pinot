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

import org.apache.pinot.segment.spi.partition.pipeline.PartitionFunctionExprCompiler;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionPipelineFunction;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class PartitionFunctionExprCommonScalarFunctionTest {
  @Test
  public void testClassBasedScalarFunctionSupportsRawStringNumericInput() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("timestampMillis",
            "plus(intDiv(timestampMillis, 1000), 7)", 128);

    assertEquals(partitionFunction.getPartition("54321"), 61);
  }

  @Test
  public void testRejectsSideEffectScalarFunction() {
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> PartitionFunctionExprCompiler.compile("raw_key", "sleep(raw_key)"));
    assertEquals(error.getMessage(),
        "Partition scalar function 'sleep' is not allowed because it is non-deterministic");
  }

  @Test
  public void testRejectsNonDeterministicNowFunction() {
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> PartitionFunctionExprCompiler.compile("raw_key", "now()"));
    assertEquals(error.getMessage(),
        "Partition scalar function 'now' is not allowed because it is non-deterministic");
  }

  @Test
  public void testRejectsNonDeterministicAgoFunction() {
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        () -> PartitionFunctionExprCompiler.compile("raw_key", "ago('PT1H')"));
    assertEquals(error.getMessage(),
        "Partition scalar function 'ago' is not allowed because it is non-deterministic");
  }

  /// Regression for commit 9bcd3ee63f. Pinot scalar functions box integral arithmetic to Double; the partition
  /// function must accept integral-valued Doubles (e.g. plus(54L, 7L) -> 61.0) without rejecting them outright.
  @Test
  public void testIntegralValuedDoubleAcceptedAsPartitionResult() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("col", "plus(intDiv(col, 1000), 7)", 128);
    // intDiv(54321, 1000) = 54; plus(54, 7) = 61.0 (Double); 61 % 128 = 61
    assertEquals(partitionFunction.getPartition("54321"), 61);
  }

  /// Regression: Float/Double whose absolute value reaches 2^53 must be rejected even if the value is "integral",
  /// because mantissa precision loss causes silent partition-id collapse. The bound is strict: 2^53 itself is
  /// representable but 2^53+1 collapses onto 2^53, so admitting 2^53 would let 2^53+1 silently collide with it.
  @Test
  public void testDoubleBeyondMantissaPrecisionIsRejected() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("col", "plus(col, 0)", 128);
    // 2^54 — clearly past the boundary
    IllegalStateException error = expectThrows(IllegalStateException.class,
        () -> partitionFunction.getPartition("18014398509481984"));
    assertTrue(error.getMessage().contains("|x| < 2^53"),
        "Expected precision-bound error, got: " + error.getMessage());
  }

  /// Boundary regression for the off-by-one in the precision check: 2^53+1 (long) silently rounds to 2^53.0
  /// (Double); admitting 2^53 would let 2^53+1 collide with 2^53 onto the same partition id. Strict `< 2^53`
  /// is required.
  @Test
  public void testDoubleAtMantissaBoundaryIsRejected() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("col", "plus(col, 0)", 128);
    // 2^53 itself — boundary value where "integral" check passes but next integer collides
    IllegalStateException error = expectThrows(IllegalStateException.class,
        () -> partitionFunction.getPartition("9007199254740992"));
    assertTrue(error.getMessage().contains("|x| < 2^53"),
        "Expected precision-bound error at 2^53 boundary, got: " + error.getMessage());
  }

  /// Regression for commit 28546f1523. The expression chain may yield null mid-evaluation (e.g. via tryCast or null
  /// column input). Surface this as null on the FunctionEvaluator interface so ingestion treats it as "no partition"
  /// rather than literal partition id -1.
  @Test
  public void testNullMidChainReturnsNullOnFunctionEvaluatorSurface() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("col", "plus(col, 0)", 128);
    GenericRow row = new GenericRow();
    row.putValue("col", null);
    assertNull(partitionFunction.evaluate(row),
        "evaluate(GenericRow) must return null when the column value is null");
    assertNull(partitionFunction.evaluate(new Object[]{null}),
        "evaluate(Object[]) must return null when the input is null");
  }

  /// Regression for commit 9c031aa76e. Expressions whose final type is non-numeric (e.g. md5() returns STRING) must
  /// be rejected at validation time with a clear "must produce INT or LONG" error rather than passing config
  /// validation and surfacing the failure later at segment creation.
  @Test
  public void testValidateOutputTypeRejectsStringExpressionResult() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("col", "md5(col)", 8);
    IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
        partitionFunction::validateOutputType);
    assertTrue(error.getMessage().contains("STRING"),
        "Expected error to mention STRING output, got: " + error.getMessage());
  }

  @Test
  public void testValidateOutputTypeRejectsFractionalBigDecimalResult() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("col", "fractionalBigDecimal(col)", 8);
    IllegalArgumentException validationError = expectThrows(IllegalArgumentException.class,
        partitionFunction::validateOutputType);
    assertTrue(validationError.getMessage().contains("integral value"),
        "Expected validation to reject fractional BigDecimal output, got: " + validationError.getMessage());

    IllegalStateException runtimeError = expectThrows(IllegalStateException.class,
        () -> partitionFunction.getPartition("1"));
    assertTrue(runtimeError.getMessage().contains("integral value"),
        "Expected runtime to reject fractional BigDecimal output, got: " + runtimeError.getMessage());
  }
}
