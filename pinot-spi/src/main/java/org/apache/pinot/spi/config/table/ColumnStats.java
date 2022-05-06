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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/*
 * Container object for stats of Pinot columns for capacity estimation, output by kafka sampler.
 */
public class ColumnStats {

  public static final String CARDINALITY = "cardinality";
  public static final String PRIMARY_KEY_SIZE = "primaryKeySize";
  public static final String NUM_PARTITIONS = "numPartitions";

  private long _cardinality;
  private int _primaryKeySize = 8;
  private int _numPartitions = 0;

  public ColumnStats(long cardinality) {
    _cardinality = cardinality;
  }

  @JsonCreator
  public ColumnStats(@JsonProperty(value = CARDINALITY, required = true) long cardinality,
      @JsonProperty(value = PRIMARY_KEY_SIZE) int primaryKeySize,
      @JsonProperty(value = NUM_PARTITIONS) int numPartitions) {
    _cardinality = cardinality;
    _primaryKeySize = primaryKeySize;
    _numPartitions = numPartitions;
  }

  @JsonProperty(CARDINALITY)
  public long getCardinality() {
    return _cardinality;
  }

  @Nullable
  @JsonProperty(PRIMARY_KEY_SIZE)
  public int getPrimaryKeySize() {
    return _primaryKeySize;
  }

  @JsonProperty(NUM_PARTITIONS)
  public int getNumPartitions() {
    return _numPartitions;
  }

  public void setCardinality(long cardinality) {
    _cardinality = cardinality;
  }

  public void setPrimaryKeySize(int primaryKeySize) {
    _primaryKeySize = primaryKeySize;
  }

  public void setNumPartitions(int numPartitions) {
    _numPartitions = numPartitions;
  }

  public String toJsonString() {
    try {
      return JsonUtils.objectToString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
