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

import org.slf4j.MDC;


public enum LoggerConstants {

  QUERY_ID_KEY("pinot.query.id"),
  /**
   * A logical id for the process.
   *
   * <p>For example, for a broker, this could be the broker id while in MSE it could be the virtual server address.
   */
  COMPONENT_ID_KEY("pinot.component.id"),
  /**
   * A logical id for the process.
   *
   * <p>For example, for a broker, this could be the broker id while in MSE it could be the virtual server address.
   */
  COMPONENT_TYPE_KEY("pinot.component.type"),
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

  public void registerOnMdc(String value) {
    registerOnMdcIfNotSet(value, false);
  }

  public boolean registerOnMdcIfNotSet(String value, boolean override) {
    if (override && MDC.get(_key) != null) {
      return false;
    }
    MDC.put(_key, value);
    return true;
  }

  public void unregisterFromMdc() {
    MDC.remove(_key);
  }

  public boolean isRegistered() {
    return MDC.get(_key) != null;
  }
}
