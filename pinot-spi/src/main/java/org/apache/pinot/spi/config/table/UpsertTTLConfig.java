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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;


/**
 * The {@code UpsertTTLConfig} class contains the upsert TTL (time-to-live) related configurations.
 * Pinot upsert keeps track of all primary keys in heap, it's costly and also affects performance when table is large.
 *
 * If primary keys in the table have lifecycle, they won't get updated after a certain period time, then we can use the
 * following configuration to enable upsert ttl feature. Pinot will only keeps track of alive primary keys in heap.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UpsertTTLConfig {

  @JsonPropertyDescription("ttl time unit, supported time units are DAYS, HOURS, MINUTES, SECONDS, MILLISECONDS")
  private String _ttlTimeUnit;
  @JsonPropertyDescription("ttl time value")
  private String _ttlTimeValue;

  @JsonCreator
  public UpsertTTLConfig(@JsonProperty("ttlTimeUnit") @Nullable String ttlTimeUnit,
      @JsonProperty("ttlTimeValue") @Nullable String ttlTimeValue) {
    _ttlTimeUnit = ttlTimeUnit;
    _ttlTimeValue = ttlTimeValue;
  }

  public String getTtlTimeUnit() {
    return _ttlTimeUnit;
  }

  public String getTtlTimeValue() {
    return _ttlTimeValue;
  }

  public void setTtlTimeUnit(String ttlTimeUnit) {
    _ttlTimeUnit = ttlTimeUnit;
  }

  public void setTtlTimeValue(String ttlTimeValue) {
    _ttlTimeValue = ttlTimeValue;
  }

  public long getTtlInMs() {
    try {
      TimeUnit timeUnit = TimeUnit.valueOf(_ttlTimeUnit.toUpperCase());
      long timeValue = Long.parseLong(_ttlTimeValue);
      return timeUnit.toMillis(timeValue);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Failed to init upsert TTL handler with ttl value: %s and ttl unit: %s", _ttlTimeValue,
              _ttlTimeUnit));
    }
  }
}
