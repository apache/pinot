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
package org.apache.pinot.spi.tasks;

public class StatusEntry {
  private final long _ts;
  private final LogLevel _level;
  private final String _status;

  public StatusEntry(long ts, LogLevel level, String status) {
    _ts = ts;
    _level = level != null ? level : LogLevel.INFO;
    _status = status;
  }

  public long getTs() {
    return _ts;
  }

  public String getStatus() {
    return _status;
  }

  public LogLevel getLevel() {
    return _level;
  }

  @Override
  public String toString() {
    return "StatusEntry{" + "_ts=" + _ts + ", _level='" + _level + "', _status='" + _status + "'" + "}";
  }

  public static class Builder {
    private long _ts;
    private LogLevel _level = LogLevel.INFO;
    private String _status;

    public Builder timestamp(long ts) {
      _ts = ts;
      return this;
    }

    public Builder level(LogLevel level) {
      _level = level;
      return this;
    }

    public Builder status(String status) {
      _status = status;
      return this;
    }

    public org.apache.pinot.spi.tasks.StatusEntry build() {
      if (_ts == 0) {
        _ts = System.currentTimeMillis();
      }
      return new org.apache.pinot.spi.tasks.StatusEntry(_ts, _level, _status);
    }
  }

  public enum LogLevel {
    INFO, WARN, ERROR
  }
}
