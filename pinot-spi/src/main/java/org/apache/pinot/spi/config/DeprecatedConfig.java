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
/// the controller's `DeprecatedTableConfigValidationUtils` to gate creation and update of table configs.
///
/// **Current behaviour (soft-launch)**: every parseable `since` value classifies the violation as a **warning**.
/// Warnings are surfaced in the REST response's `deprecationWarnings` field but never block the request. The only
/// outcome that becomes an **error** today is an unparseable `since` string — that is treated as a code-side
/// annotation bug and is locked out at build time by the controller's `testEveryRuleHasParseableSince`.
///
/// **Future behaviour (after the soft-launch flag flips)**: severity will be derived by comparing `since()`
/// against the running Pinot version — properties deprecated in the current major.minor release stay as warnings,
/// while properties deprecated in any earlier release escalate to errors that reject the REST request with HTTP
/// 400 BAD_REQUEST. The flip is gated on four documented promotion pre-conditions (see
/// `DeprecatedTableConfigValidationUtils.SOFT_LAUNCH_WARNING_ONLY`'s Javadoc).
///
/// Place this annotation on the Jackson-visible getter (the one that drives JSON property naming) so the discovery
/// walk picks up the correct serialized name.
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DeprecatedConfig {

  /// Human-readable replacement guidance, surfaced verbatim in the user-facing error/warning. Should describe what
  /// the user should set instead, e.g. `"Use 'segmentsConfig.replication' instead."`.
  String replacement();

  /// The Pinot release in which this property was deprecated, e.g. `"1.6.0"`. Today (under the soft-launch
  /// policy) every parseable value classifies as **warning** regardless of running version. After the
  /// soft-launch flag flips, the value will be compared against the running version to decide warning vs error.
  ///
  /// Format: `MAJOR.MINOR` or `MAJOR.MINOR.PATCH` with an optional `-SNAPSHOT` qualifier; only the leading
  /// major.minor pair is compared.
  ///
  /// **Strict format**: unparseable values are classified as `ERROR` by the controller's reflective validator —
  /// a code-side bug, not a user-supplied input issue. Even under the soft-launch policy, an unparseable `since`
  /// fires the throw branch in `validateOnCreate`/`validateOnUpdate` and the REST endpoint returns 400 BAD_REQUEST,
  /// breaking every PUT/POST that touches the affected table-config path. The controller test
  /// `testEveryRuleHasParseableSince` locks this at build time for in-tree annotations; external annotators
  /// should ensure their own coverage exists.
  String since();
}
