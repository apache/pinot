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
package com.linkedin.pinot.common.metrics;

import com.linkedin.pinot.common.Utils;


/**
* Enumeration containing all the metrics exposed by the Pinot broker.
*
*/
public enum BrokerMeter implements AbstractMetrics.Meter {
  UNCAUGHT_GET_EXCEPTIONS("exceptions", true),
  UNCAUGHT_POST_EXCEPTIONS("exceptions", true),
  QUERIES("queries", false),
  REQUEST_COMPILATION_EXCEPTIONS("exceptions", true),
  REQUEST_FETCH_EXCEPTIONS("exceptions", false),
  REQUEST_DESERIALIZATION_EXCEPTIONS("exceptions", false),
  DOCUMENTS_SCANNED("documents", false);

  private final String brokerMeterName;
  private final String unit;
  private final boolean global;

  BrokerMeter(String unit, boolean global) {
    this.unit = unit;
    this.global = global;
    this.brokerMeterName = Utils.toCamelCase(name().toLowerCase());
  }

  public String getMeterName() {
    return brokerMeterName;
  }

  public String getUnit() {
    return unit;
  }

  /**
   * Returns true if the metric is global (not attached to a particular resource)
   *
   * @return true if the metric is global
   */
  public boolean isGlobal() {
    return global;
  }
}
