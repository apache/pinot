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
package org.apache.pinot.controller.api.resources;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.user.ComponentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Constants {
  private Constants() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Constants.class);

  public static final String CLUSTER_TAG = "Cluster";
  public static final String DATABASE_TAG = "Database";
  public static final String TABLE_TAG = "Table";
  public static final String USER_TAG = "User";
  public static final String VERSION_TAG = "Version";
  public static final String HEALTH_TAG = "Health";
  public static final String INSTANCE_TAG = "Instance";
  public static final String QUERY_TAG = "Query";
  public static final String SCHEMA_TAG = "Schema";
  public static final String TENANT_TAG = "Tenant";
  public static final String BROKER_TAG = "Broker";
  public static final String SEGMENT_TAG = "Segment";
  public static final String TASK_TAG = "Task";
  public static final String LEAD_CONTROLLER_TAG = "Leader";
  public static final String TABLE_NAME = "tableName";
  public static final String ZOOKEEPER = "Zookeeper";
  public static final String APP_CONFIGS = "AppConfigs";
  public static final String PERIODIC_TASK_TAG = "PeriodicTask";
  public static final String UPSERT_RESOURCE_TAG = "Upsert";

  public static final String REALTIME_SEGMENT_VALIDATION_MANAGER = "RealtimeSegmentValidationManager";

  public static TableType validateTableType(String tableTypeStr) {
    if (StringUtils.isBlank(tableTypeStr)) {
      return null;
    }
    try {
      return TableType.valueOf(tableTypeStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.info("Illegal table type '{}'", tableTypeStr);
      throw new WebApplicationException("Illegal table type '" + tableTypeStr + "'", Status.BAD_REQUEST);
    }
  }

  public static ComponentType validateComponentType(String componentTypeStr) {
    if (StringUtils.isBlank(componentTypeStr)) {
      return null;
    }
    try {
      return ComponentType.valueOf(componentTypeStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.info("Illegal table type '{}'", componentTypeStr);
      throw new WebApplicationException("Illegal table type '" + componentTypeStr + "'", Status.BAD_REQUEST);
    }
  }

  public static StateType validateState(String stateStr) {
    if (StringUtils.isBlank(stateStr)) {
      return null;
    }
    try {
      return StateType.valueOf(stateStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.info("Illegal state '{}'", stateStr);
      throw new WebApplicationException("Illegal state '" + stateStr + "'", Status.BAD_REQUEST);
    }
  }
}
