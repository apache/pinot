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
package org.apache.pinot.spi.ingestion.error;

import javax.annotation.Nullable;


/// A rule's decision on one throwable: a category, or [#none] when the throwable shows the failure is not a failure to
/// access the source (Pinot's own cancellation, or a client Pinot closed).
///
/// The code and target are bounded tokens, safe for log lines: SDK codes are short identifiers, but a configured
/// endpoint can answer with anything.
public final class IngestionSourceErrorVerdict {
  /// The target for a Kafka broker.
  public static final String TARGET_BROKER = "broker";
  /// The target for a schema registry a decoder looks schemas up in.
  public static final String TARGET_SCHEMA_REGISTRY = "schema_registry";
  /// The target for AWS STS, answering for the credential rather than the source.
  public static final String TARGET_STS = "sts";
  /// The target for AWS KMS, answering for the key that encrypts the source's data.
  public static final String TARGET_KMS = "kms";
  /// The target for an identity provider: an OAuth token endpoint or a Kerberos KDC.
  public static final String TARGET_IDP = "idp";
  /// The target for an admin endpoint of the source, e.g. Pulsar's admin API.
  public static final String TARGET_ADMIN = "admin";

  private static final String TOKEN_PUNCTUATION = "_.:/-";
  private static final int MAX_TOKEN_LENGTH = 64;

  @Nullable
  private final IngestionSourceErrorCategory _category;
  @Nullable
  private final IngestionSourceErrorSubReason _subReason;
  @Nullable
  private final String _code;
  @Nullable
  private final String _target;

  private IngestionSourceErrorVerdict(@Nullable IngestionSourceErrorCategory category,
      @Nullable IngestionSourceErrorSubReason subReason, @Nullable String code, @Nullable String target) {
    _category = category;
    _subReason = subReason;
    _code = token(code);
    _target = token(target);
  }

  /// Authentication or authorization failed. `target` names the service that answered when it isn't the source
  /// itself, or is null when no service answered (a credential that couldn't be loaded or built).
  public static IngestionSourceErrorVerdict auth(IngestionSourceErrorSubReason subReason, @Nullable String code,
      @Nullable String target) {
    return new IngestionSourceErrorVerdict(IngestionSourceErrorCategory.AUTH, subReason, code, target);
  }

  /// Reading from the source failed otherwise: it couldn't serve the request or rejected it, never answered, or the
  /// client couldn't be set up.
  public static IngestionSourceErrorVerdict sourceRead(@Nullable String code, @Nullable String target) {
    return new IngestionSourceErrorVerdict(IngestionSourceErrorCategory.SOURCE_READ, null, code, target);
  }

  /// Not a failure to access the source. It still decides the chain it was found in, so a wrapper above it can't
  /// reinstate a category.
  public static IngestionSourceErrorVerdict none(@Nullable String code) {
    return new IngestionSourceErrorVerdict(null, null, code, null);
  }

  /// The category, or null for [#none].
  @Nullable
  public IngestionSourceErrorCategory getCategory() {
    return _category;
  }

  public boolean isNone() {
    return _category == null;
  }

  /// The finer reason behind an [IngestionSourceErrorCategory#AUTH] verdict; null otherwise.
  @Nullable
  public IngestionSourceErrorSubReason getSubReason() {
    return _subReason;
  }

  /// The source's own code that decided, e.g. `AccessDeniedException`, `403`, `PERMISSION_DENIED`.
  @Nullable
  public String getCode() {
    return _code;
  }

  /// The service that answered when it isn't the source itself, e.g. [#TARGET_STS] or [#TARGET_SCHEMA_REGISTRY].
  @Nullable
  public String getTarget() {
    return _target;
  }

  /// `value` with every character outside `[A-Za-z0-9_.:/-]` replaced and its length capped, or null. Reads no further
  /// than the cap: a configured endpoint can answer with a code of any length.
  @Nullable
  static String token(@Nullable String value) {
    if (value == null) {
      return null;
    }
    StringBuilder token = new StringBuilder(Math.min(value.length(), MAX_TOKEN_LENGTH));
    for (int i = 0; i < value.length() && token.length() < MAX_TOKEN_LENGTH; ) {
      int codePoint = value.codePointAt(i);
      token.append(isTokenSafe(codePoint) ? (char) codePoint : '_');
      i += Character.charCount(codePoint);
    }
    return token.toString();
  }

  private static boolean isTokenSafe(int codePoint) {
    return (codePoint >= 'A' && codePoint <= 'Z') || (codePoint >= 'a' && codePoint <= 'z')
        || (codePoint >= '0' && codePoint <= '9') || TOKEN_PUNCTUATION.indexOf(codePoint) >= 0;
  }

  @Override
  public String toString() {
    String category = _category != null ? _category.name() : "none";
    return category + (_subReason != null ? "/" + _subReason.label() : "") + (_code != null ? " code=" + _code : "")
        + (_target != null ? " target=" + _target : "");
  }
}
