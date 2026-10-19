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
package org.apache.pinot.query.planner.logical;

import java.util.Locale;


/// How the multi-stage planner shares equivalent stages between consumers (spools).
///
/// A spool replaces two or more equivalent stages with one stage that sends its output to every consumer. A spool
/// can block a query: the shared stage sends to all its consumers at the pace of the slowest one, so a consumer that
/// reads its input only after another input finishes can stop the stage that feeds that other input.
/// [SpoolDeadlockGuard] detects that case.
///
/// The mode comes from the [org.apache.pinot.spi.utils.CommonConstants.Broker#CONFIG_OF_SPOOLS] broker config and the
/// [org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey#USE_SPOOLS] query option.
public enum SpoolMode {
  /// Never share stages.
  OFF,
  /// Share equivalent stages only when [SpoolDeadlockGuard] accepts the result.
  AUTO,
  /// Share every equivalent stage the planner supports, without the guard. This is the behavior of the former
  /// `useSpools=true`.
  ON;

  /// Returns true if the planner looks for equivalent stages in this mode.
  public boolean isEnabled() {
    return this != OFF;
  }

  /// Returns true if the planner must verify each spool with [SpoolDeadlockGuard] in this mode.
  public boolean isGuarded() {
    return this == AUTO;
  }

  /// Parses a mode, ignoring case. `false` and `no` are aliases of [#OFF], and `true` and `yes` are aliases of [#ON].
  ///
  /// @throws IllegalArgumentException if the value is not a known mode.
  public static SpoolMode fromString(String value) {
    switch (value.trim().toLowerCase(Locale.ROOT)) {
      case "off":
      case "false":
      case "no":
        return OFF;
      case "auto":
        return AUTO;
      case "on":
      case "true":
      case "yes":
        return ON;
      default:
        throw new IllegalArgumentException(
            "Invalid spool mode '" + value + "'. Valid values are: off, auto, on (or false, no, true, yes)");
    }
  }
}
