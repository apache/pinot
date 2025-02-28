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
package org.apache.pinot.spi.trace;

import javax.annotation.Nullable;
import org.slf4j.MDC;


public enum LoggerConstants {

  /**
   * The query id of the query.
   *
   * This value is set by the broker and is kept consistent across all the phases of the query.
   *
   * Note: This is usually the same as the request id concept, but in MSE work associated with the same logical query
   * can have different request ids given the leaf operations use a different id. In SSE, the request is also changes
   * between the real-time and offline parts (one is the negative of the other). Therefore query id and request id
   * start to diverge in these cases.
   */
  QUERY_ID_KEY("pinot.query.id"),
  /**
   * The correlation or query id of the query.
   *
   * Contrary to {@link #QUERY_ID_KEY}, this key is optionally supplied by the client, who may decide to use the same
   * value for multiple queries. This is useful for tracking a single logical query across multiple physical queries.
   */
  CORRELATION_ID_KEY("pinot.query.cid"),
  /**
   * The MSE worker id of the query.
   */
  WORKER_ID_KEY("pinot.mse.workerId"),
  /**
   * The MSE stage id of the query.
   */
  STAGE_ID_KEY("pinot.mse.stageId");

  private final String _key;

  LoggerConstants(String key) {
    _key = key;
  }

  public String getKey() {
    return _key;
  }

  @Nullable
  public String registerInMdc(String value) {
    return registerInMdc(value, true);
  }

  @Nullable
  public String registerInMdc(String value, boolean override) {
    String oldValue = MDC.get(_key);
    if (override || oldValue == null) {
      MDC.put(_key, value);
    }
    return oldValue;
  }

  @Nullable
  public String registerInMdcIfNotSet(String value) {
    return registerInMdc(value, false);
  }

  public void unregisterFromMdc() {
    MDC.remove(_key);
  }

  public boolean isRegistered() {
    return MDC.get(_key) != null;
  }
}
