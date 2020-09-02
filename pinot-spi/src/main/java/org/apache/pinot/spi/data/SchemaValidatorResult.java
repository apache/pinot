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
package org.apache.pinot.spi.data;

/**
 * The result class to provide the detailed mismatch information.
 */
public class SchemaValidatorResult {
  private long _mismatchCount;
  private StringBuilder _mismatchReason;

  public SchemaValidatorResult() {
    _mismatchCount = 0;
    _mismatchReason = new StringBuilder();
  }

  public boolean isMismatchDetected() {
    return _mismatchCount != 0;
  }

  private void incrementMismatchCount() {
    _mismatchCount++;
  }

  public String getMismatchReason() {
    return _mismatchReason.toString();
  }

  private void addMismatchReason(String reason) {
    if (_mismatchReason.length() > 0) {
      _mismatchReason.append(" ");
    }
    _mismatchReason.append(reason);
  }

  public void incrementMismatchCountWithMismatchReason(String reason) {
    incrementMismatchCount();
    addMismatchReason(reason);
  }
}
