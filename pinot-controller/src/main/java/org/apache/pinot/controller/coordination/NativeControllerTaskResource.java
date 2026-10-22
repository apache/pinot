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
package org.apache.pinot.controller.coordination;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessControlUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Table-scoped experimental scheduling API. It exposes durable native states directly; SUCCEEDED means output
/// publication committed. Existing Helix task API resources and plugin activation are unchanged.
@Path("/tasks/native/{tableName}")
@Produces(MediaType.APPLICATION_JSON)
public final class NativeControllerTaskResource {
  private static final ObjectMapper REQUEST_JSON = JsonMapper.builder(JsonFactory.builder()
      .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
      .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      .enable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
      .enable(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES).build();
  private final NativeControllerTasks _tasks;
  private final AccessControlFactory _access;

  public NativeControllerTaskResource(NativeControllerTasks tasks, AccessControlFactory access) {
    _tasks = tasks;
    _access = access;
  }

  @PUT
  @Path("/schedules/{scheduleId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Authenticate(AccessType.UPDATE)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.CREATE_TASK, tableScopeParam = "tableName")
  public Response schedule(@PathParam("tableName") String tableName, @PathParam("scheduleId") String scheduleId,
      String body, @Context HttpHeaders headers) {
    String table = authorize(tableName, headers, AccessType.UPDATE, Actions.Cluster.CREATE_TASK);
    try {
      if (body == null || body.length() > 8192) {
        throw new IllegalArgumentException("Schedule request exceeds 8192 characters");
      }
      NativeControllerTasks.ScheduleRequest request;
      try {
        request = REQUEST_JSON.readValue(body, NativeControllerTasks.ScheduleRequest.class);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid schedule request", e);
      }
      return result(_tasks.schedule(table, UUID.fromString(scheduleId), request));
    } catch (IllegalArgumentException e) {
      throw error(400, e.getMessage());
    } catch (IOException e) {
      throw error(503, "Native task scheduling is unavailable");
    }
  }

  @GET
  @Authenticate(AccessType.READ)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_TASK, tableScopeParam = "tableName")
  public Response tasks(@PathParam("tableName") String tableName, @Context HttpHeaders headers) {
    String table = authorize(tableName, headers, AccessType.READ, Actions.Cluster.GET_TASK);
    try {
      var image = _tasks.snapshot();
      List<CatalogRecord> schedules = new ArrayList<>();
      List<CatalogRecord> tasks = new ArrayList<>();
      List<CatalogRecord> queues = new ArrayList<>();
      List<CatalogRecord> publications = new ArrayList<>();
      long round = -1;
      boolean exists = false;
      for (CatalogRecord record : image.records()) {
        switch (record.kind()) {
          case TABLE_CONFIG -> exists |= record.name().equals(table);
          case MEMBERSHIP_CONTROL -> round = MembershipProtocol.decodeControl(record.value()).round();
          case TASK_SCHEDULE -> {
            if (TaskProtocol.decodeSchedule(record.value()).input().queue().tableName().equals(table)) {
              schedules.add(record);
            }
          }
          case TASK_QUEUE -> {
            if (TaskProtocol.decodeQueue(record.value()).queue().tableName().equals(table)) {
              queues.add(record);
            }
          }
          case TASK -> {
            if (TaskProtocol.decodeTask(record.value()).input().queue().tableName().equals(table)) {
              tasks.add(record);
            }
          }
          default -> { }
        }
      }
      for (CatalogRecord record : image.records()) {
        if (record.kind() == RecordKind.TASK_PUBLICATION) {
          UUID taskId = TaskProtocol.decodePublication(record.value()).taskId();
          if (tasks.stream().anyMatch(task -> task.name().equals(taskId.toString()))) {
            publications.add(record);
          }
        }
      }
      if (!exists) {
        throw error(404, "Table not found");
      }
      return Response.ok(JsonUtils.objectToString(Map.of("round", round, "schedules", schedules, "tasks", tasks,
          "queues", queues, "publications", publications, "schedulerFailure",
          _tasks.failure() == null ? "" : "Native scheduler is unavailable")))
          .header(HttpHeaders.CACHE_CONTROL, "no-store")
          .header(NativeControllerResource.REVISION_HEADER,
              image.cursor().historyId() + ":" + image.cursor().index()).build();
    } catch (IOException e) {
      throw error(503, "Native task state is unavailable");
    }
  }

  @POST
  @Path("/{taskId}/cancel")
  @Authenticate(AccessType.UPDATE)
  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.STOP_TASK, tableScopeParam = "tableName")
  public Response cancel(@PathParam("tableName") String tableName, @PathParam("taskId") String taskId,
      @QueryParam("expectedVersion") long expectedVersion, @Context HttpHeaders headers) {
    String table = authorize(tableName, headers, AccessType.UPDATE, Actions.Cluster.STOP_TASK);
    try {
      return result(_tasks.cancel(table, UUID.fromString(taskId), expectedVersion));
    } catch (IllegalArgumentException e) {
      throw error(400, e.getMessage());
    } catch (IOException e) {
      throw error(503, "Native task cancellation is unavailable");
    }
  }

  private String authorize(String name, HttpHeaders headers, AccessType type, String action) {
    String table;
    try {
      table = DatabaseUtils.translateTableName(name, headers);
      if (TableNameBuilder.getTableTypeFromTableName(table) == TableType.REALTIME) {
        throw error(501, "Native tasks currently support OFFLINE tables");
      }
      String raw = TableNameBuilder.extractRawTableName(table);
      if (raw == null || raw.isBlank() || TableNameBuilder.getTableTypeFromTableName(raw) != null) {
        throw new IllegalArgumentException("Ambiguous table name");
      }
      table = TableNameBuilder.OFFLINE.tableNameWithType(table);
    } catch (IllegalArgumentException e) {
      throw error(400, "Invalid table name");
    }
    var access = _access.create();
    AccessControlUtils.validatePermission(table, type, headers, "/tasks/native/" + table, access);
    if (!access.hasAccess(headers, TargetType.CLUSTER, null, action)) {
      throw error(403, "Task permission denied");
    }
    return table;
  }

  private static Response result(Result result) throws IOException {
    int status = switch (result.status()) {
      case OK -> 200;
      case INVALID_REQUEST -> 400;
      case NOT_FOUND -> 404;
      case CONFLICT, FENCED, HISTORY_MISMATCH -> 409;
      default -> 503;
    };
    return Response.status(status).entity(JsonUtils.objectToString(result))
        .header(HttpHeaders.CACHE_CONTROL, "no-store").build();
  }

  private static WebApplicationException error(int status, String message) {
    return new WebApplicationException(Response.status(status).type(MediaType.APPLICATION_JSON_TYPE)
        .header(HttpHeaders.CACHE_CONTROL, "no-store").entity(Map.of("error", message)).build());
  }
}
