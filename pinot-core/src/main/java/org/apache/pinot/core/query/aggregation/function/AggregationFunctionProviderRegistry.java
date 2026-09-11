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
package org.apache.pinot.core.query.aggregation.function;

import com.google.common.annotations.VisibleForTesting;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.AggregationFunctionType;

import static com.google.common.base.Preconditions.checkState;


/// Immutable aggregate-provider lookup, initialized once from the application's service registrations. Conflicting
/// registrations fail at initialization instead of depending on classpath order. Safe for concurrent query creation.
public final class AggregationFunctionProviderRegistry {
  private static final Map<AggregationFunctionType, AggregationFunctionProvider> PROVIDERS = loadProviders(
      ServiceLoader.load(AggregationFunctionProvider.class, AggregationFunctionProvider.class.getClassLoader()));

  private AggregationFunctionProviderRegistry() {
  }

  @Nullable
  public static AggregationFunctionProvider getProvider(AggregationFunctionType type) {
    return PROVIDERS.get(type);
  }

  @VisibleForTesting
  static Map<AggregationFunctionType, AggregationFunctionProvider> loadProviders(
      Iterable<AggregationFunctionProvider> providers) {
    Map<AggregationFunctionType, AggregationFunctionProvider> result = new EnumMap<>(AggregationFunctionType.class);
    for (AggregationFunctionProvider provider : providers) {
      AggregationFunctionType type = Objects.requireNonNull(provider.getType(), "Provider must declare an aggregate");
      AggregationFunctionProvider previous = result.putIfAbsent(type, provider);
      checkState(previous == null, "Duplicate aggregation provider for %s: %s and %s", type,
          previous != null ? previous.getClass().getName() : "", provider.getClass().getName());
    }
    return Map.copyOf(result);
  }
}
