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
package org.apache.pinot.spi.ingest;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Locale;
import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.InterfaceStability;

/// States of a synchronous ROW insert. ACCEPTED transitions to VISIBLE or ABORTED; a crash can
/// leave ACCEPTED with an uncertain outcome. REJECTED means no new manifest was accepted.
/// Manifests and requestId reservations are retained until table deletion. Enum values are immutable.
@InterfaceStability.Evolving
public enum InsertStatementState {
  ACCEPTED,
  VISIBLE,
  ABORTED,
  REJECTED;

  /// Strict JSON deserializer that fails loudly on unknown values. A future controller version that
  /// introduces a new state name and writes it to ZK will not have its manifests silently mis-parsed
  /// by an older reader — instead the reader gets a clear error pointing at the unknown name.
  @JsonCreator
  @Nullable
  public static InsertStatementState fromJson(@Nullable String value) {
    if (value == null) {
      return null;  /// null/absent → caller defaults; consistent with InsertConsistencyMode.fromJson
    }
    try {
      return InsertStatementState.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unknown InsertStatementState: '" + value + "'. This controller version does not "
              + "recognize that state. Supported: ACCEPTED, VISIBLE, ABORTED, REJECTED.");
    }
  }
}
