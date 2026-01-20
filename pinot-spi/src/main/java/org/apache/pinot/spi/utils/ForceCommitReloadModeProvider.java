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
package org.apache.pinot.spi.utils;

import java.util.function.Supplier;


/**
 * Provider for accessing the current ForceCommitReloadMode from anywhere in the codebase.
 * This allows pinot-segment-local to access the dynamically configured mode without
 * depending on pinot-core.
 *
 * The supplier is registered by UpsertInconsistentStateConfig during server/controller startup.
 */
public final class ForceCommitReloadModeProvider {

  /**
   * Enum defining the reload behavior for upsert tables with inconsistent state configurations
   * (partial upsert or dropOutOfOrderRecord=true with consistency mode NONE and replication > 1).
   */
  public enum Mode {
    /**
     * Reload is disabled for tables with inconsistent state configurations.
     * Safe option that prevents potential data inconsistency issues.
     */
    NO_RELOAD(false),

    /**
     * Reload is enabled but only for tables that do not have inconsistent state configurations.
     * Tables with partial upsert or dropOutOfOrderRecord=true (with replication > 1) will be skipped.
     * When inconsistencies are detected during reload/force commit, upsert metadata is reverted.
     */
    PROTECTED_RELOAD(true),

    /**
     * Reload is enabled for all tables regardless of their configuration.
     * Use with caution as this may cause data inconsistency for partial-upsert tables
     * or upsert tables with dropOutOfOrderRecord enabled when replication > 1.
     * Inconsistency checks and metadata revert are skipped.
     */
    UNSAFE_RELOAD(true);

    private final boolean _reloadEnabled;

    Mode(boolean reloadEnabled) {
      _reloadEnabled = reloadEnabled;
    }

    /**
     * Returns whether reload operations are enabled for this mode.
     * For NO_RELOAD, returns false; for PROTECTED_RELOAD and UNSAFE_RELOAD, returns true.
     */
    public boolean isReloadEnabled() {
      return _reloadEnabled;
    }

    /**
     * Returns whether this mode is UNSAFE_RELOAD, which bypasses inconsistent state checks.
     */
    public boolean isUnsafe() {
      return this == UNSAFE_RELOAD;
    }

    /**
     * Parses a string value to Mode.
     * Supports case-insensitive matching and also legacy boolean values for backward compatibility.
     *
     * @param value the string value to parse
     * @param defaultMode the default mode to return if value is null or invalid
     * @return the parsed Mode
     */
    public static Mode fromString(String value, Mode defaultMode) {
      if (value == null || value.trim().isEmpty()) {
        return defaultMode;
      }

      String trimmedValue = value.trim().toUpperCase();

      // Try to match enum name directly
      for (Mode mode : values()) {
        if (mode.name().equals(trimmedValue)) {
          return mode;
        }
      }

      // Support legacy boolean values for backward compatibility
      if ("TRUE".equals(trimmedValue)) {
        return PROTECTED_RELOAD;
      }
      if ("FALSE".equals(trimmedValue)) {
        return NO_RELOAD;
      }

      return defaultMode;
    }
  }

  private static volatile Supplier<Mode> _modeSupplier = () -> Mode.PROTECTED_RELOAD;

  private ForceCommitReloadModeProvider() {
  }

  /**
   * Registers the supplier that provides the current Mode.
   * Should be called during server/controller startup.
   *
   * @param modeSupplier the supplier to register
   */
  public static void register(Supplier<Mode> modeSupplier) {
    _modeSupplier = modeSupplier;
  }

  /**
   * Returns the current Mode from the registered supplier.
   */
  public static Mode getMode() {
    return _modeSupplier.get();
  }
}
