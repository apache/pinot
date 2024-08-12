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
package org.apache.pinot.spi.config.table;

import org.apache.pinot.spi.config.BaseJsonConfig;


public class TablePauseStatus extends BaseJsonConfig {
  public static final TablePauseStatus UNPAUSED = new TablePauseStatus();

  private final boolean _paused;
  private final ReasonCode _reasonCode;
  private final String _comment;
  private final long _timeInMillis;

  public TablePauseStatus(ReasonCode reasonCode, String comment) {
    _reasonCode = reasonCode;
    _comment = comment;
    _timeInMillis = System.currentTimeMillis();
    _paused = true;
  }

  public TablePauseStatus() {
    _reasonCode = null;
    _comment = null;
    _timeInMillis = System.currentTimeMillis();
    _paused = false;
  }

  public boolean isPaused() {
    return _paused;
  }

  public ReasonCode getReasonCode() {
    return _reasonCode;
  }

  public String getComment() {
    return _comment;
  }

  public long getTimeInMillis() {
    return _timeInMillis;
  }

  public enum ReasonCode {
    ADMINISTRATIVE, STORAGE_QUOTA_EXCEEDED;
  }
}
