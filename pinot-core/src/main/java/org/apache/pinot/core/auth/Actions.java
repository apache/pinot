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
 */
public class Actions {
  // Action names for cluster
  public static class Cluster {
    public static final String GET_TABLE_LEADERS = "GetTableLeaders";
    public static final String LIST_TABLES = "ListTables";
    public static final String RUN_TASK = "RunTask";
    public static final String UPLOAD_SEGMENT = "UploadSegment";
    public static final String GET_APP_CONFIGS = "GetAppConfigs";
    public static final String GET_ROUTING = "GetRouting";
    public static final String GET_SERVER_ROUTING_STATS = "GetServerRoutingStats";
    public static final String GET_HEALTH = "GetHealth";
    public static final String GET_LOGGERS = "GetLoggers";
    public static final String GET_LOGGER = "GetLogger";
    public static final String SET_LOGGER = "SetLogger";
    public static final String GET_LOG_FILES = "GetLogFiles";
    public static final String GET_LOG_FILE = "GetLogFile";
    public static final String CANCEL_QUERY = "CancelQuery";
    public static final String GET_RUNNING_QUERIES = "GetRunningQueries";
    public static final String LIST_USERS = "ListUsers";
    public static final String LIST_USER = "ListUser";
    public static final String ADD_USER = "AddUser";
    public static final String DELETE_USER = "DeleteUser";
    public static final String UPDATE_USER = "UpdateUser";
    public static final String GET_CLUSTER_INFO = "GetClusterInfo";
    public static final String GET_CLUSTER_CONFIG = "GetClusterConfig";
    public static final String UPDATE_CLUSTER_CONFIG = "UpdateClusterConfig";
    public static final String DELETE_CLUSTER_CONFIG = "DeleteClusterConfig";
    public static final String CHECK_AUTH = "CheckAuth";
    public static final String GET_AUTH = "GetAuth";
    public static final String GET_INFO = "GetInfo";
    public static final String CHECK_HEALTH = "CheckHealth";
    public static final String LIST_SEGMENTS = "ListSegments";
    public static final String DUMMY = "Dummy";
    public static final String GET_ADMIN_INFO = "GetAdminInfo";
    public static final String POST_ADMIN_INFO = "PostAdminInfo";
    public static final String GET_TASK_NAMES = "GetTaskNames";
    public static final String INGEST_FILE = "IngestFile";
    public static final String GET_FORCE_COMMIT_STATUS = "GetForceCommitStatus";
    public static final String GET_REBALANCE_STATUS = "GetRebalanceStatus";
    public static final String GET_INSTANCES = "GetInstances";
    public static final String GET_INSTANCE = "GetInstance";
    public static final String CREATE_INSTANCE = "CreateInstance";
    public static final String UPDATE_INSTANCE = "UpdateInstance";
    public static final String DELETE_INSTANCE = "DeleteInstance";
    public static final String UPDATE_TAGS = "UpdateTags";
    public static final String UPDATE_RESOURCE = "UpdateResource";
    public static final String DELETE_QUERY = "DeleteQuery";
    public static final String GET_SCHEMAS = "GetSchemas";
    public static final String RESET_SEGMENTS = "ResetSegments";
    public static final String GET_SEGMENT_RELOAD_STATUS = "GetSegmentReloadStatus";
    public static final String UPDATE_TIME_INTERVAL = "UpdateTimeInterval";
    public static final String LIST_BROKERS = "ListBrokers";
    public static final String RECOMMEND_CONFIG = "RecommendConfig";
    public static final String LIST_TASK_TYPES = "ListTaskTypes";
    public static final String LIST_TASK_QUEUES = "ListTaskQueues";
    public static final String GET_TASK_QUEUE_STATE = "GetTaskQueueState";
    public static final String LIST_TASKS = "ListTasks";
    public static final String GET_TASK_METADATA = "GetTaskMetadata";
    public static final String DELETE_TASK_METADATA = "DeleteTaskMetadata";
    public static final String GET_TASK_COUNTS = "GetTaskCounts";
    public static final String DEBUG_TASKS = "DebugTasks";
    public static final String DEBUG_TASK = "DebugTask";
    public static final String GET_TASK_STATES = "GetTaskStates";
    public static final String GET_TASK_STATE = "GetTaskState";
    public static final String GET_TASK_CONFIG = "GetTaskConfig";
    public static final String GET_ALL_TASK_STATES = "GetAllTaskStates";
    public static final String GET_SCHEDULER_INFO = "GetSchedulerInfo";
    public static final String SCHEDULE_TASK = "ScheduleTask";
    public static final String SCHEDULE_TASKS = "ScheduleTasks";
    public static final String EXECUTE_TASK = "ExecuteTask";
    public static final String CLEANUP_TASK = "CleanupTask";
    public static final String CLEANUP_TASKS = "CleanupTasks";
    public static final String STOP_TASKS = "StopTasks";
    public static final String RESUME_TASK = "ResumeTasks";
    public static final String UPDATE_TASK_QUEUE = "UpdateTaskQueue";
    public static final String DELETE_TASK = "DeleteTask";
    public static final String DELETE_TASKS = "DeleteTasks";
    public static final String DELETE_TASK_QUEUE = "DeleteTaskQueue";
    public static final String CREATE_TENANT = "CreateTenant";
    public static final String UPDATE_TENANT = "UpdateTenant";
    public static final String DELETE_TENANT = "DeleteTenant";
    public static final String LIST_TENANT = "ListTenant";
    public static final String LIST_TENANTS = "ListTenants";
    public static final String GET_TENANT_METADATA = "GetTenantMetadata";
    public static final String UPDATE_TENANT_METADATA = "UpdateTenantMetadata";
    public static final String ESTIMATE_UPSERT_MEMORY = "EstimateUpsertMemory";
    public static final String GET_VERSION = "GetVersion";
    public static final String GET_TABLE_CONFIGS = "GetTableConfigs";
    public static final String GET_ZNODE = "GetZnode";
    public static final String GET_ZNODES = "GetZnodes";
    public static final String DELETE_ZNODE = "DeleteZnode";
    public static final String UPDATE_ZNODE = "UpdateZnode";
    public static final String LIST_ZNODE = "ListZnode";
    public static final String LIST_ZNODES = "ListZnodes";
    public static final String UPDATE_QPS = "UpdateQPS";
    public static final String GET_FIELD_SPEC = "GetFieldSpec";
  }

  // Action names for table
  public static class Table {
    public static final String ADD_TABLE = "AddTable";
    public static final String VALIDATE = "Validate";
    public static final String CREATE_SCHEMA = "CreateSchema";
    public static final String VALIDATE_SCHEMA = "ValidateSchema";
    public static final String ADD_CONFIGS = "AddConfigs";
    public static final String VALIDATE_CONFIGS = "ValidateConfigs";
    public static final String RUN_TASK = "RunTask";
    public static final String UPDATE_CONFIGS = "UpdateConfigs";
    public static final String GET_TIME_BOUNDARY = "GetTimeBoundary";
    public static final String GET_ROUTING_TABLE = "GetRoutingTable";
    public static final String BUILD_ROUTING = "BuildRouting";
    public static final String REFRESH_ROUTING = "RefreshRouting";
    public static final String DELETE_ROUTING = "DeleteRouting";
    public static final String GET_DEBUG_INFO = "GetDebugInfo";
    public static final String GET_SERVER_MAP = "GetServerMap";
    public static final String GET_SEGMENT_LINEAGE = "GetSegmentLineage";
    public static final String GET_SEGMENT_MAP = "GetSegmentMap";
    public static final String GET_METADATA = "GetMetadata";
    public static final String GET_SCHEMA = "GetSchema";
    public static final String DELETE_SCHEMA = "DeleteSchema";
    public static final String UPDATE_SCHEMA = "UpdateSchema";
    public static final String GET_PARTITIONS = "GetPartitions";
    public static final String ASSIGN_INSTANCES = "AssignInstances";
    public static final String DELETE_PARTITIONS = "DeletePartitions";
    public static final String REPLACE_INSTANCE = "ReplaceInstance";
    public static final String GET_TABLE_LEADER = "GetTableLeader";
    public static final String PAUSE_CONSUMPTION = "PauseConsumption";
    public static final String RESUME_CONSUMPTION = "ResumeConsumption";
    public static final String FORCE_COMMIT = "ForceCommit";
    public static final String GET_PAUSE_STATUS = "GetPauseStatus";
    public static final String GET_CONSUMING_SEGMENTS = "GetConsumingSegments";
    public static final String RELOAD_SEGMENT = "ReloadSegment";
    public static final String RELOAD_SEGMENTS = "ReloadSegments";
    public static final String GET_SEGMENTS = "GetSegments";
    public static final String DELETE_SEGMENT = "DeleteSegment";
    public static final String DELETE_SEGMENTS = "DeleteSegments";
    public static final String GET_STORAGE_TIERS = "GetStorageTiers";
    public static final String DOWNLOAD_SEGMENT = "DownloadSegment";
    public static final String UPLOAD_SEGMENTS = "UploadSegments";
    public static final String UPLOAD_SEGMENT = "UploadSegment";
    public static final String REPLACE_SEGMENTS = "ReplaceSegments";
    public static final String LIST_INSTANCES = "ListInstances";
    public static final String LIST_BROKERS = "ListBrokers";
    public static final String DELETE_TABLE = "DeleteTable";
    public static final String UPDATE_TABLE = "UpdateTable";
    public static final String REBALANCE_TABLE = "RebalanceTable";
    public static final String GET_STATE = "GetState";
    public static final String GET_CONTROLLER_JOBS = "GetControllerJobs";
    public static final String UPDATE_TIME_BOUNDARY = "UpdateTimeBoundary";
    public static final String DELETE_TIME_BOUNDARY = "DeleteTimeBoundary";
    public static final String REBUILD_BROKER_RESOURCE = "RebuildBrokerResource";
    public static final String GET_CONFIG = "GetConfig";
    public static final String GET_SIZE = "GetSize";
    public static final String GET_IDEAL_STATE = "GetIdealState";
    public static final String GET_EXTERNAL_VIEW = "GetExternalView";
    public static final String ECHO = "Echo";
    public static final String QUERY = "query";
  }
}
