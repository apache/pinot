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
package org.apache.pinot.common.metrics;

public class MetricAttributeConstants {

  private MetricAttributeConstants() {
  }

  public static final String TABLE_NAME = "TableName";
  public static final String TASK_TYPE = "TaskType";
  public static final String TASK_NAME = "TaskName";
  public static final String MERGE_LEVEL = "MergeLevel";
  public static final String SEGMENT_NAME = "SegmentName";
  public static final String RESOURCE_NAME = "ResourceName";
  public static final String TOPIC_NAME = "TopicName";
  public static final String PARTITION_ID = "PartitionId";
  public static final String COLUMN_NAME = "ColumnName";
  public static final String POOL_TAG = "PoolTag";
  public static final String POOL_ID = "PoolId";
  public static final String PINOT_METRIC_NAME = "PinotMetricName";
  public static final String WORKLOAD_NAME = "WorkloadName";
}
