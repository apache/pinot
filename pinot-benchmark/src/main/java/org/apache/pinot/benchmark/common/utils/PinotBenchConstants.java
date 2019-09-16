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
package org.apache.pinot.benchmark.common.utils;

public class PinotBenchConstants {
  public static final String TABLES_PATH = "tables";
  public static final String TABLE_TEST_SUFFIX = "Test";

  public static class PinotServerType {
    public static final String HYBRID = "HYBRID";
    public static final String OFFLINE = "OFFLINE";
    public static final String REALTIME = "REALTIME";
  }

  public static class PerfTableCreationParameters {
    public static final String TABLE_CONFIG_KEY = "tableConfig";
    public static final String TABLE_SCHEMA_KEY = "schema";
    public static final String TABLE_NAME_KEY = "tableName";
    public static final String TABLE_TYPE_KEY = "tableType";
    public static final String MODE_KEY = "mode";
    public static final String LDAP_KEY = "ldap";
    public static final String TABLE_RETENTION_KEY = "tableRetention";
    public static final String VERSION_ID_KEY = "versionId";
    public static final String NUM_BROKERS_KEY = "numBrokers";
    public static final String NUM_OFFLINE_SERVERS_KEY = "numOfflineServers";
    public static final String NUM_REALTIME_SERVERS_KEY = "numRealtimeServers";
    public static final String CREATION_TIME_MS_KEY = "creationTimeMs";
    public static final String EXPIRATION_TIME_MS_KEY = "expirationTimeMs";
  }

  public static class TenantTags {
    public static final String DEFAULT_UNTAGGED_BROKER_TAG = "broker_untagged";
    public static final String DEFAULT_UNTAGGED_SERVER_TAG = "server_untagged";

    public static final String BROKER_SUFFIX = "_BROKER";
    public static final String OFFLINE_SERVER_SUFFIX = "_OFFLINE";
    public static final String REALTIME_SERVER_SUFFIX = "_REALTIME";

    public static final String PINOT_BENCH_TENANT_TAG_PREFIX = "pinot-bench-";

  }
}
