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
    public static final String UPLOAD_SEGMENTS = "UploadSegments";
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
  }
}
