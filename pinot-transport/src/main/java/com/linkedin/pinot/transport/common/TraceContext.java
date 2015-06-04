/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.transport.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TraceContext {

  private static Logger LOGGER = LoggerFactory.getLogger(TraceContext.class);
  static ConcurrentHashMap<Long, ConcurrentHashMap<String, Long>> traceInfoMap =
      new ConcurrentHashMap<Long, ConcurrentHashMap<String, Long>>();

  public static void register(Long requestId) {
    traceInfoMap.put(requestId, new ConcurrentHashMap<String, Long>());

  }

  public static void log(Long requestId, String key, long timeInMillis) {
    traceInfoMap.get(requestId).put(key, timeInMillis);
  }

  public static void clear(Long requestId) {
    traceInfoMap.remove(requestId);
  }

  public static void dump(Long requestId) {
    LOGGER.info("Trace Info for request Id:{} : {}", requestId, traceInfoMap.get(requestId));
  }
}
