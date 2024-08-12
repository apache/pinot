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
  private boolean _paused;
  private ReasonCode _reasonCode;
  private String _comment;
  private long _timeInMillis;

  public TablePauseStatus(ReasonCode reasonCode, String comment) {
    _reasonCode = reasonCode;
    _comment = comment;
    _timeInMillis = System.currentTimeMillis();
    _paused = true;
  }

  public TablePauseStatus() {
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

  public void setPaused(boolean paused) {
    _paused = paused;
    _timeInMillis = System.currentTimeMillis();
  }

  public void setReasonCode(ReasonCode reasonCode) {
    _reasonCode = reasonCode;
    _timeInMillis = System.currentTimeMillis();
  }

  public void setComment(String comment) {
    _comment = comment;
    _timeInMillis = System.currentTimeMillis();
  }

  public enum ReasonCode {
    ADMINISTRATIVE, STORAGE_QUOTA_EXCEEDED;
  }
}
