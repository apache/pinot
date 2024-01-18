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

import com.google.common.collect.BiMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.CompletionServiceHelper;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;


public class ResourceUtils {
  private ResourceUtils() {
  }

  public static List<String> getExistingTableNamesWithType(PinotHelixResourceManager pinotHelixResourceManager,
      String tableName, @Nullable TableType tableType, Logger logger) {
    try {
      return pinotHelixResourceManager.getExistingTableNamesWithType(tableName, tableType);
    } catch (TableNotFoundException e) {
      throw new ControllerApplicationException(logger, e.getMessage(), Response.Status.NOT_FOUND);
    } catch (IllegalArgumentException e) {
      throw new ControllerApplicationException(logger, e.getMessage(), Response.Status.FORBIDDEN);
    }
  }

  public static ServerReloadTableControllerJobStatusResponse getReloadTableStatusFromServers(
      HttpClientConnectionManager connectionManager, Executor executor,
      PinotHelixResourceManager pinotHelixResourceManager, Map<String, String> controllerJobZKMetadata)
      throws InvalidConfigException {
    String tableNameWithType = controllerJobZKMetadata.get(CommonConstants.ControllerJob.TABLE_NAME_WITH_TYPE);
    String tableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    Set<String> servers = new HashSet<>(pinotHelixResourceManager.getServerInstancesForTable(tableName, tableType));
    BiMap<String, String> serverEndPoints = pinotHelixResourceManager.getDataInstanceAdminEndpoints(servers);
    CompletionServiceHelper completionServiceHelper =
        new CompletionServiceHelper(executor, connectionManager, serverEndPoints);

    List<String> serverUrls = new ArrayList<>();
    BiMap<String, String> endpointsToServers = serverEndPoints.inverse();
    for (String endpoint : endpointsToServers.keySet()) {
      String reloadTaskStatusEndpoint =
          endpoint + "/controllerJob/reloadTableStatus/" + tableNameWithType + "?reloadJobTimestamp="
              + controllerJobZKMetadata.get(CommonConstants.ControllerJob.SUBMISSION_TIME_MS);
      serverUrls.add(reloadTaskStatusEndpoint);
    }

    CompletionServiceHelper.CompletionServiceResponse serviceResponse =
        completionServiceHelper.doMultiGetRequest(serverUrls, null, true, 10000);

    ServerReloadTableControllerJobStatusResponse finalResponse =
        new ServerReloadTableControllerJobStatusResponse();
    finalResponse.setTotalServersQueried(serverUrls.size());
    finalResponse.setTotalServerCallsFailed(serviceResponse._failedResponseCount);
    boolean completed = true;

    for (Map.Entry<String, String> streamResponse : serviceResponse._httpResponses.entrySet()) {
      String responseString = streamResponse.getValue();
      try {
        ServerReloadTableControllerJobStatusResponse response =
            JsonUtils.stringToObject(responseString, ServerReloadTableControllerJobStatusResponse.class);
        if (!response.isCompleted()) {
          completed = false;
          break;
        }
      } catch (Exception e) {
        finalResponse.setTotalServerCallsFailed(finalResponse.getTotalServerCallsFailed() + 1);
      }
    }

    // this is true only when all the servers have completed the reloading
    finalResponse.setCompleted(completed);

    // Add ZK fields
    finalResponse.setMetadata(controllerJobZKMetadata);
    return finalResponse;
  }
}
