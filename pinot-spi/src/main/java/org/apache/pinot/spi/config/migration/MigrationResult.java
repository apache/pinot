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
package org.apache.pinot.spi.config.migration;

/// The outcome of running the migration chain over a single config object.
///
/// - [#getConfig()] is the (possibly upgraded) config. When [#isChanged()] is {@code false} this is
///   the same instance that was passed in.
/// - [#getVersion()] is the migration version of [#getConfig()] — always equal to the registry's
///   current version after a successful migration.
/// - [#isChanged()] is {@code true} when at least one migrator was applied, i.e. the stored config
///   was at an older version and must be persisted. When {@code false}, the caller should skip the
///   write entirely.
///
/// @param <T> the configuration type (e.g. {@code TableConfig} or {@code Schema})
public class MigrationResult<T> {
  private final T _config;
  private final int _version;
  private final boolean _changed;

  public MigrationResult(T config, int version, boolean changed) {
    _config = config;
    _version = version;
    _changed = changed;
  }

  public T getConfig() {
    return _config;
  }

  public int getVersion() {
    return _version;
  }

  public boolean isChanged() {
    return _changed;
  }
}
