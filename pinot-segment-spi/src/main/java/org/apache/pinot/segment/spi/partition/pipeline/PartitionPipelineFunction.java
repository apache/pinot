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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Locale;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.apache.pinot.spi.utils.BytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// [PartitionFunction] adapter for expression-mode partition pipelines.
///
/// The user's `functionExpr` is expected to produce an integral value already in `[0, numPartitions)`. This wrapper
/// does no normalization — it narrows long output to int and returns it directly. Out-of-range output is the user's
/// responsibility (e.g. wrap your hash expression with `mod(..., numPartitions)` if it does not already wrap).
///
/// **Note on `Serializable`:** [PartitionFunction] extends [java.io.Serializable] for historical reasons, but
/// partition functions are never Java-serialized in Pinot's runtime.
@SuppressWarnings("serial")
public class PartitionPipelineFunction implements PartitionFunction, FunctionEvaluator {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionPipelineFunction.class);
  /// Function-name value reported via [#getName()] and written into segment metadata for expression-mode
  /// pipelines. Bound to the reserved sentinel constant in [V1Constants.MetadataKeys.Column] so the
  /// cross-version fail-fast contract for old readers cannot drift via duplicate string literals.
  public static final String NAME = V1Constants.MetadataKeys.Column.PARTITION_FUNCTION_EXPR_SENTINEL;
  // Sentinel returned when the partition expression evaluates to null for a given value (e.g. null input).
  // Callers that update per-segment partition sets must skip this value.
  public static final int NULL_RESULT_PARTITION_ID = -1;
  // Largest integer that Double can represent exactly: any |x| > 2^53 is implicitly "integral" because the mantissa
  // cannot hold the fractional bit, but multiple distinct longs collapse to the same double in this range.
  private static final double MAX_PRECISE_DOUBLE_INTEGRAL = 1L << 53;

  private final PartitionPipeline _pipeline;
  private final int _numPartitions;

  public PartitionPipelineFunction(PartitionPipeline pipeline, int numPartitions) {
    Preconditions.checkNotNull(pipeline, "Partition pipeline must be configured");
    Preconditions.checkArgument(numPartitions > 0, "Number of partitions must be > 0");
    _pipeline = pipeline;
    _numPartitions = numPartitions;
  }

  public PartitionPipeline getPartitionPipeline() {
    return _pipeline;
  }

  /// Validates that the compiled expression produces an integral numeric output by probing it with several sample
  /// values that cover common input shapes (numeric strings, alpha strings, raw bytes). Throws
  /// [IllegalArgumentException] if at least one probe completes and the output is non-numeric (e.g. STRING from
  /// `md5(col)`), or if every probe throws (cannot determine output type - likely a misconfigured pipeline).
  public void validateOutputType() {
    // Probe set covers numeric strings, alpha strings, raw bytes (for BYTES input), and null. The null sample
    // mirrors the runtime ingestion path that already handles nulls, so the validation also exercises that the
    // pipeline does not blow up on null input. Non-null samples are checked for non-numeric output types.
    // Track non-null-probe success separately: a pipeline that only succeeds on null (e.g. "case when col is null
    // then 0 else fail() end") would otherwise pass validation and crash on the first real row.
    Object[] samples = _pipeline.isBytesInput()
        ? new Object[]{new byte[]{0}, new byte[]{1, 2, 3}, new byte[0], null}
        : new Object[]{"1", "0", "abc", null};
    boolean anyNonNullProbeSucceeded = false;
    RuntimeException lastFailure = null;
    for (Object sample : samples) {
      Object probe;
      try {
        probe = _pipeline.evaluate(new Object[]{sample});
      } catch (RuntimeException e) {
        lastFailure = e;
        continue;
      }
      if (sample != null) {
        anyNonNullProbeSucceeded = true;
      }
      if (probe == null) {
        continue;
      }
      if (!(probe instanceof Number)) {
        throw new IllegalArgumentException(
            "Partition pipeline must produce INT or LONG output, got: " + javaClassToTypeName(probe.getClass()));
      }
      try {
        toPartitionId(probe);
      } catch (IllegalStateException e) {
        throw new IllegalArgumentException(
            "Partition pipeline must produce INT or LONG output: " + e.getMessage(), e);
      }
    }
    if (!anyNonNullProbeSucceeded) {
      throw new IllegalArgumentException(
          "Partition pipeline for column '" + _pipeline.getRawColumn() + "' failed to evaluate against any non-null "
              + "sample input; check the expression is well-formed and accepts the column's stored type. Last error: "
              + (lastFailure != null ? lastFailure.getMessage() : "unknown"));
    }
  }

  private static String javaClassToTypeName(Class<?> cls) {
    if (cls == String.class) {
      return "STRING";
    }
    if (cls == Integer.class) {
      return "INT";
    }
    if (cls == Long.class) {
      return "LONG";
    }
    if (cls == Float.class) {
      return "FLOAT";
    }
    if (cls == Double.class) {
      return "DOUBLE";
    }
    if (cls == byte[].class) {
      return "BYTES";
    }
    if (cls == Boolean.class) {
      return "BOOLEAN";
    }
    return cls.getSimpleName().toUpperCase(Locale.ROOT);
  }

  @Override
  public int getPartition(String value) {
    // BYTES-input pipelines expect raw bytes. When the caller provides a string (e.g. a hex-encoded predicate value
    // from broker routing), convert the hex string back to raw bytes so the partition computation matches ingestion.
    if (_pipeline.isBytesInput()) {
      return getPartition(BytesUtils.toBytes(value));
    }
    return toPartitionId(_pipeline.evaluate(new Object[]{value}));
  }

  /// Overrides the default bytes partition to pass raw bytes directly through the pipeline when this pipeline was
  /// compiled with [PartitionValueType#BYTES] input type, avoiding the hex-encoding round-trip.
  @Override
  public int getPartition(byte[] bytes) {
    if (!_pipeline.isBytesInput()) {
      return getPartition(BytesUtils.toHexString(bytes));
    }
    return toPartitionId(_pipeline.evaluate(new Object[]{bytes}));
  }

  /// Converts the integral numeric output from the user's expression into a partition id. The expression is expected
  /// to already produce a value in `[0, numPartitions)`; out-of-range output is the user's responsibility (the value
  /// is returned unchanged after narrowing long → int).
  private int toPartitionId(Object result) {
    if (result == null) {
      LOGGER.debug("Partition expression for column '{}' returned null; skipping partition assignment for this value",
          _pipeline.getRawColumn());
      return NULL_RESULT_PARTITION_ID;
    }
    Preconditions.checkState(result instanceof Number,
        "Partition expression for column '%s' must return a numeric value, got: %s",
        _pipeline.getRawColumn(), result.getClass().getSimpleName());
    Number num = (Number) result;
    // Pinot scalar functions commonly box integral arithmetic to Double (e.g. plus(long, long) → Double). Accept
    // Float/Double iff the value is integral (no fractional part) AND strictly fits within Double's 53-bit mantissa.
    // The bound is strict (|x| < 2^53) because the next representable integer above 2^53 is 2^53+2: the long
    // value 2^53+1 silently rounds to 2^53.0, which would collide with 2^53 itself if we admitted it. Require the
    // user to cast to LONG explicitly in the expression to opt into the wider integer range.
    if (num instanceof Float || num instanceof Double) {
      double d = num.doubleValue();
      Preconditions.checkState(!Double.isNaN(d) && !Double.isInfinite(d) && d == Math.floor(d)
              && Math.abs(d) < MAX_PRECISE_DOUBLE_INTEGRAL,
          "Partition expression for column '%s' must return an integral value within Double precision "
              + "(|x| < 2^53), got: %s (%s); cast the expression result to LONG explicitly to avoid this",
          _pipeline.getRawColumn(), result, result.getClass().getSimpleName());
    }
    // BigInteger and BigDecimal get dedicated paths so Number.longValue()/intValue() cannot silently truncate
    // fractional values or values outside the LONG range.
    if (num instanceof BigInteger) {
      return narrowToInt((BigInteger) num, result);
    }
    if (num instanceof BigDecimal) {
      try {
        return narrowToInt(((BigDecimal) num).toBigIntegerExact(), result);
      } catch (ArithmeticException e) {
        throw new IllegalStateException(
            "Partition expression for column '" + _pipeline.getRawColumn()
                + "' must return an integral value, got: " + result + " (BigDecimal)", e);
      }
    }
    if (num instanceof Long || num instanceof Float || num instanceof Double) {
      return (int) num.longValue();
    }
    if (num instanceof Integer || num instanceof Short || num instanceof Byte) {
      return num.intValue();
    }
    throw new IllegalStateException(
        "Partition expression for column '" + _pipeline.getRawColumn()
            + "' must return INT or LONG-compatible output, got: " + javaClassToTypeName(result.getClass()));
  }

  private int narrowToInt(BigInteger value, Object originalResult) {
    try {
      return (int) value.longValueExact();
    } catch (ArithmeticException e) {
      throw new IllegalStateException(
          "Partition expression for column '" + _pipeline.getRawColumn()
              + "' must return a value within LONG range, got: " + originalResult, e);
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getNumPartitions() {
    return _numPartitions;
  }

  @Override
  @JsonIgnore
  public String getPartitionColumn() {
    return _pipeline.getRawColumn();
  }

  @Override
  @JsonIgnore(false)  // Override @JsonIgnore on PartitionFunction#getFunctionExpr to expose this in serialization.
  @JsonProperty("functionExpr")
  public String getFunctionExpr() {
    return _pipeline.getCanonicalFunctionExpr();
  }

  /// Expression-mode pipelines pass the integral expression output through unchanged (the framework's `NO_OP`
  /// normalizer label). The user's expression is responsible for producing a value already in `[0, numPartitions)`.
  @Override
  @JsonIgnore
  public PartitionIdNormalizer getPartitionIdNormalizer() {
    return PartitionIdNormalizer.NO_OP;
  }

  // FunctionEvaluator implementation

  @Override
  public List<String> getArguments() {
    return _pipeline.getArguments();
  }

  @Override
  public Object evaluate(GenericRow genericRow) {
    // Delegate to the underlying pipeline directly: it already handles the column lookup, the BYTES-vs-STRING
    // dispatch, and the null-input early return. Avoids duplicating that logic here and saves one Object[]
    // allocation per row vs the getPartition(...) path.
    Object rawResult = _pipeline.evaluate(genericRow);
    int partitionId = toPartitionId(rawResult);
    return partitionId == NULL_RESULT_PARTITION_ID ? null : partitionId;
  }

  @Override
  public Object evaluate(Object[] values) {
    Preconditions.checkArgument(values.length == 1,
        "Partition pipeline function for column '%s' expects exactly 1 positional argument, got: %s",
        _pipeline.getRawColumn(), values.length);
    Object inputValue = values[0];
    if (inputValue == null) {
      return null;
    }
    int partitionId = (inputValue instanceof byte[] && _pipeline.isBytesInput())
        ? getPartition((byte[]) inputValue)
        : getPartition(FieldSpec.getStringValue(inputValue));
    return partitionId == NULL_RESULT_PARTITION_ID ? null : partitionId;
  }

  @Override
  public String toString() {
    return NAME;
  }
}
