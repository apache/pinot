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
   * The request id of the query.
   *
   * See {@link org.apache.pinot.spi.query.QueryThreadContext#getRequestId()} to know more about this value and its
   * meaning.
   */
  REQUEST_ID_KEY("pinot.query.id"),
  /**
   * The correlation or query id of the query.
   *
   * See {@link org.apache.pinot.spi.query.QueryThreadContext#getCid()} to know more about this value and its meaning.
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
