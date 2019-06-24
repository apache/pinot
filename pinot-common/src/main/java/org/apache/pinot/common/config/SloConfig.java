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
package org.apache.pinot.common.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.lang.reflect.Field;
import org.apache.pinot.common.utils.EqualityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Section of the table config that provides service-level objectives for specific dimensions such as
 * latency, freshness.
 *
 * Note that analytical query latencies can be highly variable - the SLO should include the maximum tolerable latency
 * for servicing the common, high latency queries.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SloConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(SloConfig.class);

  @ConfigKey("latencyMs")
  @ConfigDoc("Latency SLO in milliseconds for queries of this table")
  private long latencyMs;

  @ConfigKey("freshnessLagMs")
  @ConfigDoc("Freshness Lag SLO in milliseconds for queries of this table (realtime only)")
  private long freshnessLagMs;

  public long getLatencyMs() {
    return latencyMs;
  }

  public void setLatencyMs(long latencyMs) {
    this.latencyMs = latencyMs;
  }

  public long getFreshnessLagMs() {
    return freshnessLagMs;
  }

  public void setFreshnessLagMs(long freshnessLagMs) {
    this.freshnessLagMs = freshnessLagMs;
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    final String newLine = System.getProperty("line.separator");

    result.append(this.getClass().getName());
    result.append(" Object {");
    result.append(newLine);

    //determine fields declared in this class only (no fields of superclass)
    final Field[] fields = this.getClass().getDeclaredFields();

    //print field names paired with their values
    for (final Field field : fields) {
      result.append("  ");
      try {
        result.append(field.getName());
        result.append(": ");
        //requires access to private field:
        result.append(field.get(this));
      } catch (final IllegalAccessException ex) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("Caught exception while processing field " + field, ex);
        }
      }
      result.append(newLine);
    }
    result.append("}");

    return result.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    SloConfig that = (SloConfig) o;

    return EqualityUtils.isEqual(latencyMs, that.latencyMs) &&
        EqualityUtils.isEqual(freshnessLagMs, that.freshnessLagMs);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(latencyMs);
    result = EqualityUtils.hashCodeOf(result, latencyMs);
    return result;
  }
}
