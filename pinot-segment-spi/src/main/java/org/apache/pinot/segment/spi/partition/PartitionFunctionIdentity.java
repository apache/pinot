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
package org.apache.pinot.segment.spi.partition;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


/// Canonical identity for partition functions that produce the same partition id for every input.
///
/// The hash is computed once during function construction. Hash collisions are resolved with exact equality while
/// interning, so query-time compatibility checks can safely compare canonical identities by reference. The weak
/// interner does not extend an identity's lifetime beyond the partition functions that use it.
///
/// Every setting passed to [#of(Class, int, PartitionIdNormalizer, Object...)] must be immutable and must participate
/// in `equals()` and `hashCode()` according to its effective partitioning behavior.
public final class PartitionFunctionIdentity implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Interner<PartitionFunctionIdentity> INTERNER = Interners.newWeakInterner();

  private final Class<? extends PartitionFunction> _functionClass;
  private final int _numPartitions;
  private final PartitionIdNormalizer _normalizer;
  private final List<Object> _settings;
  private final int _hashCode;

  private PartitionFunctionIdentity(Class<? extends PartitionFunction> functionClass, int numPartitions,
      PartitionIdNormalizer normalizer, Object[] settings) {
    _functionClass = Objects.requireNonNull(functionClass);
    _numPartitions = numPartitions;
    _normalizer = Objects.requireNonNull(normalizer);
    _settings = Collections.unmodifiableList(Arrays.asList(settings.clone()));

    int hashCode = functionClass.hashCode();
    hashCode = 31 * hashCode + numPartitions;
    hashCode = 31 * hashCode + normalizer.hashCode();
    _hashCode = 31 * hashCode + _settings.hashCode();
  }

  public static PartitionFunctionIdentity of(Class<? extends PartitionFunction> functionClass, int numPartitions,
      PartitionIdNormalizer normalizer, Object... settings) {
    return INTERNER.intern(new PartitionFunctionIdentity(functionClass, numPartitions, normalizer, settings));
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof PartitionFunctionIdentity)) {
      return false;
    }
    PartitionFunctionIdentity identity = (PartitionFunctionIdentity) other;
    return _hashCode == identity._hashCode && _functionClass == identity._functionClass
        && _numPartitions == identity._numPartitions && _normalizer == identity._normalizer
        && _settings.equals(identity._settings);
  }

  @Override
  public int hashCode() {
    return _hashCode;
  }

  private Object readResolve() {
    return of(_functionClass, _numPartitions, _normalizer, _settings.toArray());
  }
}
