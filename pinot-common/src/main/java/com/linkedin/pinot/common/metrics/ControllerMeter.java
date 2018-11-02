/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
 * Meters for the controller.
 */
public enum ControllerMeter implements AbstractMetrics.Meter {
  HELIX_ZOOKEEPER_RECONNECTS("reconnects", true),
  CONTROLLER_INTERNAL_ERROR("InternalError", true),
  CONTROLLER_INSTANCE_GET_ERROR("InstanceGetError", true),
  CONTROLLER_INSTANCE_POST_ERROR("InstancePostError", true),
  CONTROLLER_INSTANCE_DELETE_ERROR("InstanceDeleteError", true),
  CONTROLLER_SEGMENT_GET_ERROR("SegmentGetError", true),
  CONTROLLER_SEGMENT_DELETE_ERROR("SegmentDeleteError", true),
  CONTROLLER_SEGMENT_UPLOAD_ERROR("SegmentUploadError", true),
  CONTROLLER_SCHEMA_GET_ERROR("SchemaGetError", true),
  CONTROLLER_SCHEMA_DELETE_ERROR("SchemaDeleteError", true),
  CONTROLLER_SCHEMA_UPLOAD_ERROR("SchemaUploadError", true),
  CONTROLLER_TABLE_INDEXING_GET_ERROR("TableIndexingGetError", true),
  CONTROLLER_TABLE_INSTANCES_GET_ERROR("TableInstancesGetError", true),
  CONTROLLER_TABLE_ADD_ERROR("TableAddError", true),
  CONTROLLER_TABLE_GET_ERROR("TableGetError", true),
  CONTROLLER_TABLE_UPDATE_ERROR("TableUpdateError", true),
  CONTROLLER_TABLE_SCHEMA_GET_ERROR("TableSchemaGetError", true),
  CONTROLLER_TABLE_SCHEMA_UPDATE_ERROR("TableSchemaUpdateError", true),
  CONTROLLER_TABLE_TENANT_UPDATE_ERROR("TableTenantUpdateError", true),
  CONTROLLER_TABLE_TENANT_CREATE_ERROR("TableTenantCreateError", true),
  CONTROLLER_TABLE_TENANT_DELETE_ERROR("TableTenantDeleteError", true),
  CONTROLLER_TABLE_TENANT_GET_ERROR("TableTenantGetError", true),
  CONTROLLER_REALTIME_TABLE_SEGMENT_ASSIGNMENT_ERROR("errors", true),
  CONTROLLER_NOT_LEADER("notLeader", true),
  LLC_STATE_MACHINE_ABORTS("aborts", false),
  LLC_AUTO_CREATED_PARTITIONS("creates", false),
  LLC_ZOOKEEPER_UPDATE_FAILURES("failures", false),
  LLC_KAFKA_DATA_LOSS("dataLoss", false),
  // Introducing a new stream agnostic metric to replace LLC_KAFKA_DATA_LOSS.
  // We can phase out LLC_KAFKA_DATA_LOSS once we have collected sufficient metrics for the new one
  LLC_STREAM_DATA_LOSS("dataLoss", false),
  NUMBER_TIMES_SCHEDULE_TASKS_CALLED("tasks", true),
  NUMBER_TASKS_SUBMITTED("tasks", false),
  NUMBER_SEGMENT_UPLOAD_TIMEOUT_EXCEEDED("SegmentUploadTimeouts", true),
  PARTITION_ASSIGNMENT_GENERATION_ERROR("partitionAssignmentError", false);


  private final String brokerMeterName;
  private final String unit;
  private final boolean global;

  ControllerMeter(String unit, boolean global) {
    this.unit = unit;
    this.global = global;
    this.brokerMeterName = Utils.toCamelCase(name().toLowerCase());
  }

  @Override
  public String getMeterName() {
    return brokerMeterName;
  }

  @Override
  public String getUnit() {
    return unit;
  }

  /**
   * Returns true if the metric is global (not attached to a particular resource)
   *
   * @return true if the metric is global
   */
  @Override
  public boolean isGlobal() {
    return global;
  }
}
