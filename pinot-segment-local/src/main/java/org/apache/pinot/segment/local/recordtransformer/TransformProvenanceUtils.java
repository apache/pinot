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
package org.apache.pinot.segment.local.recordtransformer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.pinot.common.evaluator.FunctionEvaluatorFactory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.apache.pinot.spi.utils.IngestionConfigUtils;


/// Computes dependency-aware transform provenance for persisted derived columns.
public final class TransformProvenanceUtils {
  public static final int CURRENT_VERSION = 2;

  private TransformProvenanceUtils() {
  }

  /// Returns a stable fingerprint for every configured transform output. A fingerprint includes the direct expression
  /// plus every configured upstream transform reachable through its arguments, including non-schema intermediates.
  public static Map<String, String> getTransformFingerprints(TableConfig tableConfig, Schema schema) {
    Map<String, String> transformFunctionByColumn =
        IngestionConfigUtils.getTransformFunctionByColumn(tableConfig, schema);
    Map<String, String> fingerprints = new HashMap<>();
    for (String column : new TreeSet<>(transformFunctionByColumn.keySet())) {
      computeFingerprint(column, transformFunctionByColumn, fingerprints, new HashSet<>());
    }
    return fingerprints;
  }

  /// Returns the configured transform ancestors of the supplied outputs, including the outputs themselves. Replaying
  /// a changed dependent must also re-evaluate known derived ancestors because their persisted, schema-normalized
  /// values can differ from the raw evaluator values consumed by a fresh transform chain.
  public static Set<String> getTransformDependencyClosure(Set<String> columns,
      Map<String, String> transformFunctionByColumn) {
    Set<String> closure = new HashSet<>(columns);
    ArrayDeque<String> pending = new ArrayDeque<>(columns);
    while (!pending.isEmpty()) {
      String column = pending.removeFirst();
      String expression = transformFunctionByColumn.get(column);
      if (expression == null) {
        continue;
      }
      FunctionEvaluator evaluator = FunctionEvaluatorFactory.getExpressionEvaluator(expression);
      for (String argument : evaluator.getArguments()) {
        if (transformFunctionByColumn.containsKey(argument) && closure.add(argument)) {
          pending.addLast(argument);
        }
      }
    }
    return closure;
  }

  /// Returns whether known stored provenance differs from the current expression graph. Version-1 metadata tracked
  /// only the direct expression, so it can safely detect only direct changes. Rebuilding an unchanged version-1 column
  /// merely to upgrade its provenance could overwrite values that depended on an authoritative, unpersisted
  /// intermediate transform output.
  public static boolean hasTransformChanged(ColumnMetadata columnMetadata, @Nullable String currentExpression,
      @Nullable String currentFingerprint) {
    if (columnMetadata.getTransformFunctionProvenanceVersion() == ColumnMetadata.UNAVAILABLE) {
      return false;
    }
    if (!Objects.equals(columnMetadata.getTransformFunction(), currentExpression)) {
      return true;
    }
    if (currentExpression == null) {
      return false;
    }
    return columnMetadata.getTransformFunctionProvenanceVersion() == CURRENT_VERSION
        && !Objects.equals(columnMetadata.getTransformFunctionFingerprint(), currentFingerprint);
  }

  /// Returns whether the stored values have dependency-closed provenance for the active transform graph.
  public static boolean hasCurrentDependencyClosedProvenance(ColumnMetadata columnMetadata,
      @Nullable String currentExpression, @Nullable String currentFingerprint) {
    return currentExpression != null
        && columnMetadata.getTransformFunctionProvenanceVersion() == CURRENT_VERSION
        && Objects.equals(columnMetadata.getTransformFunction(), currentExpression)
        && Objects.equals(columnMetadata.getTransformFunctionFingerprint(), currentFingerprint);
  }

  private static String computeFingerprint(String column, Map<String, String> transformFunctionByColumn,
      Map<String, String> fingerprints, Set<String> visiting) {
    String existing = fingerprints.get(column);
    if (existing != null) {
      return existing;
    }
    if (!visiting.add(column)) {
      throw new IllegalStateException("Expression cycle found for column '" + column
          + "' in transform function definitions.");
    }
    String expression = transformFunctionByColumn.get(column);
    MessageDigest digest = newDigest();
    update(digest, "transform-provenance-v2");
    update(digest, column);
    update(digest, expression);
    FunctionEvaluator evaluator = FunctionEvaluatorFactory.getExpressionEvaluator(expression);
    Set<String> dependencies = new TreeSet<>();
    for (String argument : evaluator.getArguments()) {
      if (transformFunctionByColumn.containsKey(argument)) {
        dependencies.add(argument);
      }
    }
    for (String dependency : dependencies) {
      update(digest, dependency);
      update(digest,
          computeFingerprint(dependency, transformFunctionByColumn, fingerprints, visiting));
    }
    visiting.remove(column);
    String fingerprint = Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
    fingerprints.put(column, fingerprint);
    return fingerprint;
  }

  private static MessageDigest newDigest() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }

  private static void update(MessageDigest digest, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    digest.update(Integer.toString(bytes.length).getBytes(StandardCharsets.US_ASCII));
    digest.update((byte) ':');
    digest.update(bytes);
  }
}
