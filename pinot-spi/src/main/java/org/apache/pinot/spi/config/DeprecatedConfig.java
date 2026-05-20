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
package org.apache.pinot.spi.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/// Marks a config getter as a deprecated JSON property in `TableConfig` (or any nested config bean).
///
/// **Metadata-only**: this annotation has no runtime effect on serialization or deserialization. It is consumed by
/// the controller's `DeprecatedTableConfigValidationUtils` to gate creation and update of table configs. Severity
/// is determined by comparing [#since()] against the running Pinot version: properties deprecated in the current
/// major.minor release are reported as **warnings**, while properties deprecated in any earlier release are
/// reported as **errors**.
///
/// Place this annotation on the Jackson-visible getter (the one that drives JSON property naming) so the discovery
/// walk picks up the correct serialized name.
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DeprecatedConfig {

  /// Human-readable replacement guidance, surfaced verbatim in the user-facing error/warning. Should describe what
  /// the user should set instead, e.g. `"Use 'segmentsConfig.replication' instead."`.
  String replacement();

  /// The Pinot release in which this property was deprecated, e.g. `"1.6.0"`. Used to decide whether a violation is
  /// reported as a warning (current major.minor) or an error (older). Format: `MAJOR.MINOR` or `MAJOR.MINOR.PATCH`
  /// with an optional `-SNAPSHOT` qualifier; only the leading major.minor pair is compared.
  String since();
}
