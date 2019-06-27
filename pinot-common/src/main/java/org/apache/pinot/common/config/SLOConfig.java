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
import org.apache.pinot.common.utils.time.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Section of the table config that provides service-level objectives for specific dimensions such as
 * latency, freshness.
 *
 * Note that analytical query latencies can be highly variable - the SLO should include the maximum tolerable latency
 * for servicing the common, high latency queries.
 *
 * Ideally, SLO config applies broadly at the "raw" table level, i.e., for both realtime and offline alike and should
 * be specified once. However, the current table config structure does not support this and the config needs to be
 * duplicated for both tables. Given that, the configs can diverge, but the application logic might choose to pick
 * the config from one of the tables assuming that the configs don't diverge intentionally.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SLOConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(SLOConfig.class);

  @ConfigKey("latencyMs")
  @ConfigDoc("Latency SLO in milliseconds for queries of this table")
  private long latencyMs;

  @ConfigKey("realtimeFreshnessLagTime")
  @ConfigDoc("Realtime Freshness Lag SLO (can be specified in hours, minutes or seconds) for queries of this table")
  private String realtimeFreshnessLagTime;

  // freshness lag value in milliseconds
  private long realtimeFreshnessLagMs;

  public long getLatencyMs() {
    return latencyMs;
  }

  public void setLatencyMs(long latencyMs) {
    this.latencyMs = latencyMs;
  }

  public String getRealtimeFreshnessLagTime() {
    return realtimeFreshnessLagTime;
  }

  /**
   * Set the freshness lag config.
   * Stores the config and converts to milliseconds and ensure that the string is in the expected format.
   * @throws IllegalArgumentException if the config isn't in the expected format
   */
  public void setRealtimeFreshnessLagTime(String freshnessLagTime) {
    this.realtimeFreshnessLagTime = freshnessLagTime;
    this.realtimeFreshnessLagMs = TimeUtils.convertPeriodToMillis(freshnessLagTime);
  }

  public long getRealtimeFreshnessLagMs() {
    return realtimeFreshnessLagMs;
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

    SLOConfig that = (SLOConfig) o;

    return EqualityUtils.isEqual(latencyMs, that.latencyMs) &&
        EqualityUtils.isEqual(realtimeFreshnessLagTime, that.realtimeFreshnessLagTime);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(latencyMs);
    result = EqualityUtils.hashCodeOf(result, latencyMs);
    return result;
  }
}
