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


/// The result of [IngestionSourceErrorClassifier#classify]: the category, plus bounded detail for a log line. It holds
/// no message text: source messages carry account ids, principals and endpoints.
public final class IngestionSourceErrorClassification {
  private final IngestionSourceErrorVerdict _verdict;
  private final String _exceptionClass;
  @Nullable
  private final IngestionSourceCredentialMode _credentialMode;
  private final boolean _roleArnConfigured;

  IngestionSourceErrorClassification(IngestionSourceErrorVerdict verdict, String exceptionClass,
      @Nullable IngestionSourceErrorEvidence evidence) {
    _verdict = verdict;
    _exceptionClass = exceptionClass;
    _credentialMode = evidence != null ? evidence.getCredentialMode() : null;
    _roleArnConfigured = evidence != null && evidence.isRoleArnConfigured();
  }

  public IngestionSourceErrorCategory getCategory() {
    return _verdict.getCategory();
  }

  /// The finer reason behind an [IngestionSourceErrorCategory#AUTH] verdict; null for
  /// [IngestionSourceErrorCategory#SOURCE_READ].
  @Nullable
  public IngestionSourceErrorSubReason getSubReason() {
    return _verdict.getSubReason();
  }

  /// The source's own code that decided, as a bounded token.
  @Nullable
  public String getCode() {
    return _verdict.getCode();
  }

  /// The service that answered when it isn't the source itself, e.g. `schema_registry` or `sts`.
  @Nullable
  public String getTarget() {
    return _verdict.getTarget();
  }

  /// The class of the exception that decided.
  public String getExceptionClass() {
    return _exceptionClass;
  }

  /// Whose credential the client used, when it recorded it.
  @Nullable
  public IngestionSourceCredentialMode getCredentialMode() {
    return _credentialMode;
  }

  /// Whether the client assumed a role on top of its base credential.
  public boolean isRoleArnConfigured() {
    return _roleArnConfigured;
  }

  @Override
  public String toString() {
    return _verdict + " exception=" + _exceptionClass + (_credentialMode != null ? " credentialMode=" + _credentialMode
        : "") + (_roleArnConfigured ? " roleArnConfigured" : "");
  }
}
