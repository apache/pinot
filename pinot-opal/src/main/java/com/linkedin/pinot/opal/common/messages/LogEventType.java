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
package com.linkedin.pinot.opal.common.messages;

import java.util.HashMap;
import java.util.Map;

public enum LogEventType {
  INSERT(0),
  DELETE(1);

  private final int _uuid;
  private static final Map<Integer, LogEventType> UUID_MAP = new HashMap<>();

  static {
    for (LogEventType type: LogEventType.values()) {
      UUID_MAP.put(type.getUUID(), type);
    }
  }

  LogEventType(int uuid) {
    _uuid = uuid;
  }

  public int getUUID() {
    return this._uuid;
  }

  public LogEventType getEventType(int uuid) {
    return UUID_MAP.get(uuid);
  }
}
