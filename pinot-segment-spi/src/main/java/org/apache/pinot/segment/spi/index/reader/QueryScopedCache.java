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
package org.apache.pinot.segment.spi.index.reader;

import java.util.function.Function;


/// A cache whose lifetime is a single query, letting the segments of one request share a value that
/// is expensive to derive and identical for all of them.
///
/// Index readers live longer than a query and are shared by every query, so they cannot hold
/// per-query state themselves; the caller owns the cache and passes it in.
///
/// This interface exists so that segment-level code can use a query-scoped cache without depending
/// on the query engine. Implementations must be safe for concurrent use, because the segments of
/// one request are processed in parallel.
public interface QueryScopedCache {

  /// Returns the value for `key`, computing it with `mapper` if it is not already present.
  ///
  /// Computation is atomic per `(type, key)`: concurrent callers that miss the same key block until
  /// one `mapper` completes, and all of them observe that single result. A `mapper` that throws or
  /// returns `null` leaves nothing cached, so a later call for the same key runs it again. These are
  /// the semantics of [java.util.concurrent.ConcurrentHashMap#computeIfAbsent]; implementations
  /// backed by it satisfy this contract.
  ///
  /// @param type the type of the value, which keeps unrelated callers from colliding on equal keys
  /// @param key the key identifying the value; must be immutable with structural equality
  /// @param mapper computes the value the first time the key is seen
  <K, V> V getOrComputeSharedValue(Class<V> type, K key, Function<K, V> mapper);
}
