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

import com.google.common.annotations.VisibleForTesting;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provider for accessing the current ForceCommitReloadMode from anywhere in the codebase.
 * This allows pinot-segment-local to access the dynamically configured mode without
 * depending on pinot-core.
 *
 * The supplier is registered by UpsertInconsistentStateConfig during server/controller startup.
 */
public final class ConsumingSegmentCommitModeProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumingSegmentCommitModeProvider.class);
  private static final Supplier<Mode> DEFAULT_SUPPLIER = () -> Mode.NONE;

  /**
   * Enum defining the reload behavior for upsert tables with inconsistent state configurations
   * (partial upsert or dropOutOfOrderRecord=true with consistency mode NONE and replication > 1).
   */
  public enum Mode {
    /**
     * Reload is disabled for tables with inconsistent state configurations.
     * Safe option that prevents potential data inconsistency issues.
     */
    NONE(false),

    /**
     * Reload is enabled but only for tables that do not have inconsistent state configurations.
     * Tables with partial upsert or dropOutOfOrderRecord=true (with replication > 1) will be skipped.
     * When inconsistencies are detected during reload/force commit, upsert metadata is reverted.
     */
    PROTECTED(true),

    /**
     * Reload is enabled for all tables regardless of their configuration.
     * Use with caution as this may cause data inconsistency for partial-upsert tables
     * or upsert tables with dropOutOfOrderRecord enabled when replication > 1.
     * Inconsistency checks and metadata revert are skipped.
     */
    UNSAFE(true);

    private final boolean _reloadEnabled;

    Mode(boolean reloadEnabled) {
      _reloadEnabled = reloadEnabled;
    }

    /**
     * Returns whether reload operations are enabled for this mode.
     * For NONE, returns false; for PROTECTED and UNSAFE, returns true.
     */
    public boolean isReloadEnabled() {
      return _reloadEnabled;
    }

    /**
     * Returns whether this mode is UNSAFE, which bypasses inconsistent state checks.
     */
    public boolean isUnsafe() {
      return this == UNSAFE;
    }

    /**
     * Returns whether this mode is PROTECTED, which reverts metadata on inconsistencies.
     */
    public boolean isProtected() {
      return this == PROTECTED;
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
        return PROTECTED;
      }
      if ("FALSE".equals(trimmedValue)) {
        return NONE;
      }

      return defaultMode;
    }
  }

  private static volatile Supplier<Mode> _modeSupplier = DEFAULT_SUPPLIER;
  private static volatile boolean _registered = false;

  private ConsumingSegmentCommitModeProvider() {
  }

  /**
   * Registers the supplier that provides the current Mode.
   * Should be called during server/controller startup.
   * Logs a warning if called multiple times (e.g., in tests).
   *
   * @param modeSupplier the supplier to register
   */
  public static void register(Supplier<Mode> modeSupplier) {
    if (_registered) {
      LOGGER.warn("ConsumingSegmentCommitModeProvider already registered, overwriting previous supplier");
    }
    _modeSupplier = modeSupplier;
    _registered = true;
  }

  /**
   * Returns the current Mode from the registered supplier.
   */
  public static Mode getMode() {
    return _modeSupplier.get();
  }

  /**
   * Resets the provider to its default state.
   * This is intended for testing purposes only.
   */
  @VisibleForTesting
  public static void reset() {
    _modeSupplier = DEFAULT_SUPPLIER;
    _registered = false;
  }
}
