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

package org.apache.pinot.core.auth;

/**
 * Different action types used in finer grain access control of the rest endpoints
 * Action names are in <verb><noun> format, e.g. GetSchema, ListTables, etc.
 */
public class Actions {
  // Action names for cluster
  public static class Cluster {
    public static final String CANCEL_QUERY = "CancelQuery";
    public static final String CLEANUP_TASK = "CleanupTask";
    public static final String COMMIT_SEGMENT = "CommitSegment";
    public static final String CREATE_INSTANCE = "CreateInstance";
    public static final String CREATE_TASK = "CreateTask";
    public static final String CREATE_TENANT = "CreateTenant";
    public static final String CREATE_USER = "CreateUser";
    public static final String DEBUG_TASK = "DebugTask";
    public static final String DELETE_CLUSTER_CONFIG = "DeleteClusterConfig";
    public static final String DELETE_INSTANCE = "DeleteInstance";
    public static final String DELETE_TASK = "DeleteTask";
    public static final String DELETE_TENANT = "DeleteTenant";
    public static final String DELETE_USER = "DeleteUser";
    public static final String DELETE_ZNODE = "DeleteZnode";
    public static final String ESTIMATE_UPSERT_MEMORY = "EstimateUpsertMemory";
    public static final String EXECUTE_TASK = "ExecuteTask";
    public static final String GET_ADMIN_INFO = "GetAdminInfo";
    public static final String GET_APP_CONFIG = "GetAppConfig";
    public static final String GET_AUTH = "GetAuth";
    public static final String GET_BROKER = "GetBroker";
    public static final String GET_CLUSTER_CONFIG = "GetClusterConfig";
    public static final String GET_FORCE_COMMIT_STATUS = "GetForceCommitStatus";
    public static final String GET_HEALTH = "GetHealth";
    public static final String GET_INSTANCE = "GetInstance";
    public static final String GET_LOGGER = "GetLogger";
    public static final String GET_LOG_FILE = "GetLogFile";
    public static final String GET_REBALANCE_STATUS = "GetRebalanceStatus";
    public static final String GET_RUNNING_QUERY = "GetRunningQuery";
    public static final String GET_SCHEDULER_INFO = "GetSchedulerInfo";
    public static final String GET_SCHEMA = "GetSchema";
    public static final String GET_SEGMENT = "GetSegment";
    public static final String GET_SEGMENT_RELOAD_STATUS = "GetSegmentReloadStatus";
    public static final String GET_SERVER_ROUTING_STATS = "GetServerRoutingStats";
    public static final String GET_TABLE = "GetTable";
    public static final String GET_TABLE_CONFIG = "GetTableConfig";
    public static final String GET_TABLE_LEADER = "GetTableLeader";
    public static final String GET_TASK = "GetTask";
    public static final String GET_TENANT = "GetTenant";
    public static final String GET_USER = "GetUser";
    public static final String GET_VERSION = "GetVersion";
    public static final String GET_ZNODE = "GetZnode";
    public static final String INGEST_FILE = "IngestFile";
    public static final String RECOMMEND_CONFIG = "RecommendConfig";
    public static final String RESET_SEGMENT = "ResetSegment";
    public static final String RESUME_TASK = "ResumeTask";
    public static final String STOP_TASK = "StopTask";
    public static final String UPDATE_BROKER_RESOURCE = "UpdateBrokerResource";
    public static final String UPDATE_CLUSTER_CONFIG = "UpdateClusterConfig";
    public static final String UPDATE_INSTANCE = "UpdateInstance";
    public static final String UPDATE_LOGGER = "UpdateLogger";
    public static final String UPDATE_QPS = "UpdateQPS";
    public static final String UPDATE_TASK_QUEUE = "UpdateTaskQueue";
    public static final String UPDATE_TENANT = "UpdateTenant";
    public static final String UPDATE_TENANT_METADATA = "UpdateTenantMetadata";
    public static final String UPDATE_TIME_INTERVAL = "UpdateTimeInterval";
    public static final String UPDATE_USER = "UpdateUser";
    public static final String UPDATE_ZNODE = "UpdateZnode";
    public static final String UPLOAD_SEGMENT = "UploadSegment";
  }

  // Action names for table
  public static class Table {
    public static final String BUILD_ROUTING = "BuildRouting";
    public static final String CREATE_INSTANCE_PARTITIONS = "CreateInstancePartitions";
    public static final String CREATE_SCHEMA = "CreateSchema";
    public static final String CREATE_TABLE = "CreateTable";
    public static final String DELETE_INSTANCE_PARTITIONS = "DeleteInstancePartitions";
    public static final String DELETE_ROUTING = "DeleteRouting";
    public static final String DELETE_SCHEMA = "DeleteSchema";
    public static final String DELETE_SEGMENT = "DeleteSegment";
    public static final String DELETE_TABLE = "DeleteTable";
    public static final String DELETE_TIME_BOUNDARY = "DeleteTimeBoundary";
    public static final String DISABLE_TABLE = "DisableTable";
    public static final String DOWNLOAD_SEGMENT = "DownloadSegment";
    public static final String ENABLE_TABLE = "EnableTable";
    public static final String FORCE_COMMIT = "ForceCommit";
    public static final String GET_BROKER = "GetBroker";
    public static final String GET_CONFIG = "GetConfig";
    public static final String GET_CONSUMING_SEGMENTS = "GetConsumingSegments";
    public static final String GET_CONTROLLER_JOBS = "GetControllerJobs";
    public static final String GET_DEBUG_INFO = "GetDebugInfo";
    public static final String GET_EXTERNAL_VIEW = "GetExternalView";
    public static final String GET_IDEAL_STATE = "GetIdealState";
    public static final String GET_INSTANCE = "GetInstance";
    public static final String GET_INSTANCE_PARTITIONS = "GetInstancePartitions";
    public static final String GET_METADATA = "GetMetadata";
    public static final String GET_PAUSE_STATUS = "GetPauseStatus";
    public static final String GET_ROUTING_TABLE = "GetRoutingTable";
    public static final String GET_SCHEMA = "GetSchema";
    public static final String GET_SEGMENT = "GetSegment";
    public static final String GET_SEGMENT_LINEAGE = "GetSegmentLineage";
    public static final String GET_SEGMENT_MAP = "GetSegmentMap";
    public static final String GET_SERVER_MAP = "GetServerMap";
    public static final String GET_SIZE = "GetSize";
    public static final String GET_STATE = "GetState";
    public static final String GET_STORAGE_TIER = "GetStorageTier";
    public static final String GET_TABLE_CONFIG = "GetTableConfig";
    public static final String GET_TABLE_LEADER = "GetTableLeader";
    public static final String GET_TIME_BOUNDARY = "GetTimeBoundary";
    public static final String PAUSE_CONSUMPTION = "PauseConsumption";
    public static final String QUERY = "Query";
    public static final String REBALANCE_TABLE = "RebalanceTable";
    public static final String REBUILD_BROKER_RESOURCE = "RebuildBrokerResource";
    public static final String REFRESH_ROUTING = "RefreshRouting";
    public static final String RELOAD_SEGMENT = "ReloadSegment";
    public static final String REPLACE_SEGMENT = "ReplaceSegment";
    public static final String RESUME_CONSUMPTION = "ResumeConsumption";
    public static final String UPDATE_INSTANCE_PARTITIONS = "UpdateInstancePartitions";
    public static final String UPDATE_SCHEMA = "UpdateSchema";
    public static final String UPDATE_TABLE_CONFIG = "UpdateTableConfig";
    public static final String UPDATE_TABLE_CONFIGS = "UpdateTableConfigs";
    public static final String UPLOAD_SEGMENT = "UploadSegment";
    public static final String VALIDATE_CONFIG = "ValidateConfig";
    public static final String VALIDATE_SCHEMA = "ValidateSchema";
  }
}
