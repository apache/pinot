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


/// Controls how long the broker waits before returning a response for an INSERT INTO statement.
///
/// v1 ships with exactly one mode: {@link #WAIT_FOR_ACCEPT} — return after the coordinator has
/// accepted the statement. Future versions may add modes such as `FIRE_AND_FORGET` (return
/// immediately) or `WAIT_FOR_VISIBLE` (return after the data is queryable); those enum names
/// are NOT reserved here because their precise wire semantics depend on a v2 two-phase-commit
/// design that has not yet been pinned down. Adding them now would prematurely lock the wire
/// contract before the implementation is built.
///
/// **Wire compatibility: enum names are permanent once shipped.** Values are
/// serialized into the `InsertRequest` JSON payload and must not be renamed. To add a new
/// mode, append a new value; never reuse or rename existing ones.
///
/// **Forward compatibility: unknown explicit values fail loudly.** A new broker
/// version that sends a future mode name to an older controller MUST be designed expecting the
/// older controller to reject the request — see {@link #fromJson}. This avoids the silent-downgrade
/// failure mode where Jackson would otherwise map an unknown enum string to `null` and the
/// default `WAIT_FOR_ACCEPT` would be applied without the broker realizing.
///
/// **Null/missing handling is intentional, not a downgrade.** {@link #fromJson}
/// preserves a literal `null` JSON value (or absent field) and the {@link InsertRequest}
/// constructor applies the v1 default `WAIT_FOR_ACCEPT`. This is the explicit "use server
/// default" path — clients that want the default omit the field; only unknown string values fail
/// the strict check above.
///
/// This enum is thread-safe (immutable).
@InterfaceStability.Evolving
public enum InsertConsistencyMode {
  WAIT_FOR_ACCEPT;

  /// Strict JSON deserializer for explicit string values: an unknown string is rejected rather than
  /// silently mapped to `null`. A literal `null` (or absent field) is preserved as
  /// `null` so the {@link InsertRequest} constructor can apply the v1 default
  /// (`WAIT_FOR_ACCEPT`). The two cases are distinct: unknown-string is a forward-compat
  /// mismatch and must fail; null/absent is the intentional "use server default" signal.
  @JsonCreator
  @Nullable
  public static InsertConsistencyMode fromJson(@Nullable String value) {
    if (value == null) {
      return null;  /// null/absent → caller defaults; see Javadoc on the enum
    }
    try {
      return InsertConsistencyMode.valueOf(value.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unknown InsertConsistencyMode: '" + value + "'. This controller version supports only "
              + "WAIT_FOR_ACCEPT. Future versions may add additional modes; until then, unknown "
              + "values are rejected to surface forward-compat mismatches loudly.");
    }
  }
}
