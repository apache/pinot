/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.resources;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import com.linkedin.pinot.common.utils.CommonConstants;
import javax.ws.rs.WebApplicationException;


public class Constants {
  private static final Logger LOGGER = LoggerFactory.getLogger(Constants.class);

  public static final String TABLE_TAG = "Table";
  public static final String VERSION_TAG = "Version";
  public static final String HEALTH_TAG = "Health";
  public static final String INSTANCE_TAG = "Instance";
  public static final String SCHEMA_TAG = "Schema";
  public static final String TENANT_TAG = "Tenant";
  public static final String SEGMENT_TAG = "Segment";
  public static final String TASK_TAG = "Task";
  public static final String TABLE_NAME = "tableName";


  public static CommonConstants.Helix.TableType validateTableType(String tableTypeStr) {
    if (tableTypeStr == null || tableTypeStr.isEmpty()) {
      return null;
    }
    try {
      return CommonConstants.Helix.TableType.valueOf(tableTypeStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.info("Illegal table type '{}'", tableTypeStr);
      throw new WebApplicationException("Illegal table type '" + tableTypeStr + "'", PinotSegmentRestletResource.BAD_REQUEST);
    }
  }

  public static StateType validateState(String stateStr) {
    if (stateStr == null) {
      return null;
    }
    try {
      return StateType.valueOf(stateStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.info("Illegal state '{}'", stateStr);
      throw new WebApplicationException("Illegal state '" + stateStr + "'", PinotSegmentRestletResource.BAD_REQUEST);
    }
  }
}
