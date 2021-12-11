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
package org.apache.pinot.controller.recommender.rules.io.configs;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;

import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.DEFAULT_NUM_KAFKA_PARTITIONS;
import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.PartitionRule.DEFAULT_NUM_PARTITIONS;


/**
 * The output format of kafka partition and pinot table partition recommendation
 */
public class PartitionConfig {
  String _partitionDimension;
  int _numKafkaPartitions;
  int _numPartitionsOffline;
  int _numPartitionsRealtime;

  boolean _isNumPartitionsOfflineOverwritten = false;
  boolean _isNumPartitionsRealtimeOverwritten = false;
  boolean _isPartitionDimensionOverwritten = false;

  public PartitionConfig() {
    _partitionDimension = "";
    _numKafkaPartitions = DEFAULT_NUM_KAFKA_PARTITIONS;
    _numPartitionsOffline = DEFAULT_NUM_PARTITIONS;
    _numPartitionsRealtime = DEFAULT_NUM_PARTITIONS;
  }

  public int getNumPartitionsRealtime() {
    return _numPartitionsRealtime;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumPartitionsRealtime(int numPartitionsRealtime) {
    _numPartitionsRealtime = numPartitionsRealtime;
    _isNumPartitionsRealtimeOverwritten = true;
  }

  public String getPartitionDimension() {
    return _partitionDimension;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setPartitionDimension(String partitionDimension) {
    _partitionDimension = partitionDimension;
    _isPartitionDimensionOverwritten = true;
  }

  public int getNumPartitionsOffline() {
    return _numPartitionsOffline;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNumPartitionsOffline(int numPartitionsOffline) {
    _numPartitionsOffline = numPartitionsOffline;
    _isNumPartitionsOfflineOverwritten = true;
  }

  public int getNumKafkaPartitions() {
    return _numKafkaPartitions;
  }

  public void setNumKafkaPartitions(int numKafkaPartitions) {
    _numKafkaPartitions = numKafkaPartitions;
  }

  public void setNumPartitionsOfflineOverwritten(boolean numPartitionsOfflineOverwritten) {
    _isNumPartitionsOfflineOverwritten = numPartitionsOfflineOverwritten;
  }

  public void setNumPartitionsRealtimeOverwritten(boolean numPartitionsRealtimeOverwritten) {
    _isNumPartitionsRealtimeOverwritten = numPartitionsRealtimeOverwritten;
  }

  public void setPartitionDimensionOverwritten(boolean partitionDimensionOverwritten) {
    _isPartitionDimensionOverwritten = partitionDimensionOverwritten;
  }

  public boolean isNumPartitionsOfflineOverwritten() {
    return _isNumPartitionsOfflineOverwritten;
  }

  public boolean isNumPartitionsRealtimeOverwritten() {
    return _isNumPartitionsRealtimeOverwritten;
  }

  public boolean isPartitionDimensionOverwritten() {
    return _isPartitionDimensionOverwritten;
  }
}
