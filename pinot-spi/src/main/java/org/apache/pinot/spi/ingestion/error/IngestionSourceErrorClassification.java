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

  @Nullable
  public String getCode() {
    return _verdict.getCode();
  }

  @Nullable
  public String getTarget() {
    return _verdict.getTarget();
  }

  public Throwable getException() {
    return _exception;
  }

  public String getExceptionClass() {
    return _exception.getClass().getName();
  }

  @Nullable
  public IngestionSourceErrorEvidence getEvidence() {
    return _evidence;
  }

  @Override
  public String toString() {
    return _verdict + " exception=" + getExceptionClass();
  }
}
