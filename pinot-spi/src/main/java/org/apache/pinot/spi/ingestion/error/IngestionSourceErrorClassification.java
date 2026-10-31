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


/// The result of [IngestionSourceErrorClassifier#classify]: the category, plus bounded detail for a log line. The
/// detail and [#toString] hold no message text, which carries account ids, principals and endpoints; a caller that
/// needs more reads the deciding exception itself.
public final class IngestionSourceErrorClassification {
  private final IngestionSourceErrorVerdict _verdict;
  private final Throwable _exception;
  @Nullable
  private final IngestionSourceErrorEvidence _evidence;

  IngestionSourceErrorClassification(IngestionSourceErrorVerdict verdict, Throwable exception,
      @Nullable IngestionSourceErrorEvidence evidence) {
    _verdict = verdict;
    _exception = exception;
    _evidence = evidence;
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

  /// The exception that decided.
  public Throwable getException() {
    return _exception;
  }

  /// The class of the exception that decided.
  public String getExceptionClass() {
    return _exception.getClass().getName();
  }

  /// The evidence nearest above the exception that decided, in its own chain, or null.
  @Nullable
  public IngestionSourceErrorEvidence getEvidence() {
    return _evidence;
  }

  @Override
  public String toString() {
    return _verdict + " exception=" + getExceptionClass();
  }
}
