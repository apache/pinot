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

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Compiles a partition-function expression into a [PartitionPipeline] backed by a
/// [org.apache.pinot.spi.function.FunctionEvaluator] provided by [PartitionEvaluatorFactory].
///
/// The compiled expression is expected to produce an integral output already in `[0, numPartitions)`.
/// No normalization is applied — the caller's expression is responsible for producing a valid partition id
/// (e.g. `mod(murmur2(col), 8)` or `bucket(timestampMillis, 1000)` when bucket already wraps at numPartitions).
public final class PartitionFunctionExprCompiler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionFunctionExprCompiler.class);
  // Cap on user-supplied expression length, sized to fit comfortably within ZK / segment metadata size budgets while
  // allowing realistic chained expressions (e.g. fnv1a_32(md5(lower(trim(col))))).
  private static final int MAX_EXPRESSION_LENGTH = 256;

  // Cache compiled pipelines so a broker/server with thousands of segments sharing the same partition expression
  // only Calcite-parses the expression once. PartitionPipeline is stateless except for per-thread scratch arrays in
  // its inner ExecutableNode tree, so the cached instance is safe to share across multiple PartitionPipelineFunction
  // wrappers (each wrapper applies its own numPartitions).
  //
  // Bounded by maximumSize so that long-lived processes with churning table configs (creates/drops/expression edits)
  // don't accumulate compiled pipelines indefinitely. The cap is sized for the realistic case of a handful of
  // distinct (column, expression) tuples per table × hundreds of tables.
  // expireAfterWrite (not access) ensures that an edited expression (rare, but legitimate during schema evolution)
  // takes effect within the TTL even on a hot table whose cache entry would otherwise never expire. Combined with
  // maximumSize, this caps both staleness and memory.
  private static final long PIPELINE_CACHE_MAX_SIZE = 10_000L;
  private static final long PIPELINE_CACHE_EXPIRE_HOURS = 1L;
  private static final Cache<PipelineCacheKey, PartitionPipeline> PIPELINE_CACHE = CacheBuilder.newBuilder()
      .maximumSize(PIPELINE_CACHE_MAX_SIZE)
      .expireAfterWrite(PIPELINE_CACHE_EXPIRE_HOURS, TimeUnit.HOURS)
      .build();

  private PartitionFunctionExprCompiler() {
  }

  // Lazy holder: initialized on first use so that class-load of this utility does not fail when
  // pinot-common (which provides InbuiltPartitionEvaluatorFactory) is not yet on the classpath.
  // The JVM class-initialization guarantee makes this inherently thread-safe and single-init.
  private static final class EvaluatorFactoryHolder {
    static final PartitionEvaluatorFactory INSTANCE = loadEvaluatorFactory();

    private static PartitionEvaluatorFactory loadEvaluatorFactory() {
      List<PartitionEvaluatorFactory> factories = new ArrayList<>();
      for (PartitionEvaluatorFactory f : ServiceLoader.load(PartitionEvaluatorFactory.class)) {
        factories.add(f);
      }
      Preconditions.checkState(!factories.isEmpty(),
          "No PartitionEvaluatorFactory implementation found on the classpath");
      // Tolerate multiple implementations (common in shaded-jar / test classpaths where pinot-common appears via more
      // than one route). Prefer the built-in InbuiltPartitionEvaluatorFactory when present; otherwise pick the first
      // and log the choice. Hard-failing here would abort every segment load that reaches the compiler.
      if (factories.size() == 1) {
        return factories.get(0);
      }
      // Match by FQN, not getSimpleName(): a user-supplied factory in a different package with the same simple name
      // would silently shadow the real built-in. Hard-coding the FQN here is acceptable because pinot-common is
      // always on the broker/server/controller classpath.
      String inbuiltFqn = "org.apache.pinot.common.evaluator.InbuiltPartitionEvaluatorFactory";
      for (PartitionEvaluatorFactory f : factories) {
        if (f.getClass().getName().equals(inbuiltFqn)) {
          LOGGER.info("Found {} PartitionEvaluatorFactory implementations on the classpath: {}; preferring {}",
              factories.size(), factories, f.getClass().getName());
          return f;
        }
      }
      PartitionEvaluatorFactory chosen = factories.get(0);
      LOGGER.info("Found {} PartitionEvaluatorFactory implementations on the classpath: {}; using first: {}",
          factories.size(), factories, chosen.getClass().getName());
      return chosen;
    }
  }

  public static PartitionPipeline compile(String rawColumn, String functionExpr) {
    return compile(rawColumn, PartitionValueType.STRING, functionExpr);
  }

  /// Compiles a partition pipeline with an explicit input type.
  ///
  /// Use [PartitionValueType#BYTES] when the partition column stores raw byte arrays so that functions in the
  /// expression receive the original bytes directly rather than a hex-encoded string representation.
  public static PartitionPipeline compile(String rawColumn, PartitionValueType inputType, String functionExpr) {
    Preconditions.checkArgument(rawColumn != null && !rawColumn.trim().isEmpty(), "Raw column must be configured");
    Preconditions.checkArgument(inputType != null, "Input type must be configured");
    Preconditions.checkArgument(functionExpr != null && !functionExpr.trim().isEmpty(),
        "'functionExpr' must be configured");
    Preconditions.checkArgument(functionExpr.length() <= MAX_EXPRESSION_LENGTH,
        "'functionExpr' must be <= %s characters", MAX_EXPRESSION_LENGTH);

    String canonicalExpr = canonicalize(functionExpr);
    boolean isBytesInput = inputType == PartitionValueType.BYTES;
    PipelineCacheKey cacheKey = new PipelineCacheKey(rawColumn, isBytesInput, canonicalExpr);
    try {
      return PIPELINE_CACHE.get(cacheKey, () -> {
        FunctionEvaluator evaluator = EvaluatorFactoryHolder.INSTANCE.compile(rawColumn, canonicalExpr);
        return new PartitionPipeline(rawColumn, isBytesInput, canonicalExpr, evaluator);
      });
    } catch (UncheckedExecutionException e) {
      // Loader threw a RuntimeException (e.g. invalid expression). Unwrap so callers see the original failure.
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new IllegalStateException("Failed to compile partition pipeline for column '" + rawColumn + "'", cause);
    } catch (ExecutionException e) {
      // Loader threw a checked exception. Unwrap to surface the original cause.
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new IllegalStateException("Failed to compile partition pipeline for column '" + rawColumn + "'", cause);
    }
  }

  public static PartitionPipelineFunction compilePartitionFunction(String rawColumn, String functionExpr,
      int numPartitions) {
    return compilePartitionFunction(rawColumn, PartitionValueType.STRING, functionExpr, numPartitions);
  }

  /// Compiles a partition pipeline function with an explicit input type.
  ///
  /// Use [PartitionValueType#BYTES] when the partition column stores raw byte arrays so that functions in the
  /// expression receive the original bytes directly rather than a hex-encoded string representation.
  public static PartitionPipelineFunction compilePartitionFunction(String rawColumn, PartitionValueType inputType,
      String functionExpr, int numPartitions) {
    PartitionPipeline pipeline = compile(rawColumn, inputType, functionExpr);
    return new PartitionPipelineFunction(pipeline, numPartitions);
  }

  /// Returns a canonical form of the expression: trimmed, lowercased, with spaces removed around
  /// `(`, `)`, and `,`. Quoted literals are preserved byte-for-byte because partition expressions
  /// can use case-sensitive salts, JSON paths, or regex patterns as function arguments.
  static String canonicalize(String expression) {
    String trimmed = expression.trim();
    StringBuilder builder = new StringBuilder(trimmed.length());
    boolean pendingSpace = false;
    boolean afterComma = false;
    char quote = 0;
    int i = 0;
    while (i < trimmed.length()) {
      char ch = trimmed.charAt(i);
      if (quote != 0) {
        builder.append(ch);
        if (ch == quote) {
          // SQL escapes quotes inside string literals by doubling them. Preserve both characters and stay inside the
          // literal so canonicalization cannot alter the literal payload.
          if (i + 1 < trimmed.length() && trimmed.charAt(i + 1) == quote) {
            builder.append(trimmed.charAt(++i));
          } else {
            quote = 0;
          }
        }
        i++;
        continue;
      }
      if (ch == '\'' || ch == '"') {
        appendPendingSpace(builder, pendingSpace, afterComma);
        pendingSpace = false;
        afterComma = false;
        quote = ch;
        builder.append(ch);
        i++;
        continue;
      }
      if (Character.isWhitespace(ch)) {
        pendingSpace = true;
        i++;
        continue;
      }
      if (ch == '(' || ch == ')') {
        pendingSpace = false;
        afterComma = false;
        builder.append(ch);
        i++;
        continue;
      }
      if (ch == ',') {
        pendingSpace = false;
        builder.append(", ");
        afterComma = true;
        i++;
        continue;
      }
      appendPendingSpace(builder, pendingSpace, afterComma);
      pendingSpace = false;
      afterComma = false;
      builder.append(String.valueOf(ch).toLowerCase(Locale.ROOT));
      i++;
    }
    int length = builder.length();
    return length > 0 && builder.charAt(length - 1) == ' '
        ? builder.substring(0, length - 1)
        : builder.toString();
  }

  private static void appendPendingSpace(StringBuilder builder, boolean pendingSpace, boolean afterComma) {
    if (pendingSpace && !afterComma && builder.length() > 0 && builder.charAt(builder.length() - 1) != '(') {
      builder.append(' ');
    }
  }

  private static final class PipelineCacheKey {
    private final String _rawColumn;
    private final boolean _isBytesInput;
    private final String _canonicalExpr;

    PipelineCacheKey(String rawColumn, boolean isBytesInput, String canonicalExpr) {
      _rawColumn = rawColumn;
      _isBytesInput = isBytesInput;
      _canonicalExpr = canonicalExpr;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof PipelineCacheKey)) {
        return false;
      }
      PipelineCacheKey other = (PipelineCacheKey) obj;
      return _isBytesInput == other._isBytesInput
          && _rawColumn.equals(other._rawColumn)
          && _canonicalExpr.equals(other._canonicalExpr);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_rawColumn, _isBytesInput, _canonicalExpr);
    }
  }
}
