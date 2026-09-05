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
package org.apache.pinot.core.query.aggregation.groupby;

import java.util.Optional;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;


/// Provides an optional specialized [GroupKeyGenerator] for a single-segment group-by query.
///
/// [#tryCreate] is called synchronously by the segment execution thread, at most once while each ordinary,
/// non-filtered, non-grouping-set SSE group-by executor is constructed. Callers may reuse one provider across segment
/// plans and queries, so a shared provider can receive concurrent calls and must be thread-safe.
///
/// A provider must not retain the supplied context and must return an empty [Optional] when it cannot safely handle it;
/// Pinot then uses its built-in generator selection unchanged. Each returned generator must be a fresh, query-owned
/// instance. Pinot owns it until ownership is transferred with the raw group-by result or Pinot makes one close
/// attempt. Resource-owning generators must release their resources or durably transfer cleanup to an independent
/// retry owner before [GroupKeyGenerator#close] returns or throws; Pinot may discard the generator after that attempt.
@FunctionalInterface
@InterfaceAudience.LimitedPrivate("StarTree")
@InterfaceStability.Unstable
public interface GroupKeyGeneratorProvider {
  /// The built-in provider sentinel. Callers use identity comparison with this exact instance to preserve Pinot's
  /// existing generator-selection hot path without collecting the additional [GroupKeyGeneratorContext] metadata.
  GroupKeyGeneratorProvider DEFAULT = context -> Optional.empty();

  Optional<GroupKeyGenerator> tryCreate(GroupKeyGeneratorContext context);
}
