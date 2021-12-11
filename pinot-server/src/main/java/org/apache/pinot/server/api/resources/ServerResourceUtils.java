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
package org.apache.pinot.server.api.resources;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.server.starter.ServerInstance;


/**
 * Utility class for Server resources.
 */
public class ServerResourceUtils {

  // Disable instantiation.
  private ServerResourceUtils() {

  }

  public static TableDataManager checkGetTableDataManager(ServerInstance serverInstance, String tableName) {
    InstanceDataManager dataManager = checkGetInstanceDataManager(serverInstance);
    TableDataManager tableDataManager = dataManager.getTableDataManager(tableName);
    if (tableDataManager == null) {
      throw new WebApplicationException("Table " + tableName + " does not exist", Response.Status.NOT_FOUND);
    }
    return tableDataManager;
  }

  public static InstanceDataManager checkGetInstanceDataManager(ServerInstance serverInstance) {
    if (serverInstance == null) {
      throw new WebApplicationException("Server initialization error. Missing server instance");
    }
    InstanceDataManager instanceDataManager = serverInstance.getInstanceDataManager();
    if (instanceDataManager == null) {
      throw new WebApplicationException("Server initialization error. Missing data manager",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return instanceDataManager;
  }
}
