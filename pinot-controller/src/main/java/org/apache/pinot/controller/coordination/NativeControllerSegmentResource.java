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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.FileUploadDownloadClient.CustomHeaders;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessControlUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.NativeReplicaPlacementPolicy;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.DatabaseConflictException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Standard binary OFFLINE upload with explicit immutable publication semantics. Parses multipart only after
/// authorization and bounded admission; no legacy Helix upload resource or global multipart temp directory is used.
@Path("/v2/segments")
@Produces(MediaType.APPLICATION_JSON)
public final class NativeControllerSegmentResource {
  private final NativeControllerMetadata _metadata;
  private final NativeControllerTasks _tasks;
  private final AccessControlFactory _access;
  private final NativeSegmentUploadStorage _storage;
  private final Semaphore _requests = new Semaphore(2);

  public NativeControllerSegmentResource(NativeControllerMetadata metadata, NativeControllerTasks tasks,
      ControllerConf conf, AccessControlFactory access) throws IOException {
    _metadata = metadata;
    _tasks = tasks;
    _access = access;
    _storage = new NativeSegmentUploadStorage(conf);
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Authenticate(AccessType.CREATE)
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.UPLOAD_SEGMENT)
  public Response upload(InputStream body, @QueryParam("tableName") String tableName,
      @DefaultValue("OFFLINE") @QueryParam("tableType") String tableType,
      @DefaultValue("false") @QueryParam("allowRefresh") boolean allowRefresh,
      @DefaultValue("false") @QueryParam("enableParallelPushProtection") boolean parallelProtection,
      @Context HttpHeaders headers) {
    String table = tableName(tableName, tableType, headers);
    authorize(table, headers);
    String headerTable = headers.getHeaderString(CommonConstants.Controller.TABLE_NAME_HTTP_HEADER);
    if (headerTable != null && !table.equals(tableName(headerTable, tableType, headers))) {
      throw error(400, "Upload table header differs from tableName");
    }
    if (allowRefresh) {
      throw error(400, "Native uploads do not support refresh or replacement; use allowRefresh=false");
    }
    validateHeaders(headers);
    if (!_storage.enabled()) {
      throw error(503, "Native shared upload storage is not configured");
    }
    String length = headers.getHeaderString(HttpHeaders.CONTENT_LENGTH);
    try {
      if (length != null && (Long.parseLong(length) < 0 || Long.parseLong(length) > _storage.maxRequestBytes())) {
        throw error(413, "Upload exceeds its byte limit");
      }
    } catch (NumberFormatException e) {
      throw error(400, "Invalid request length");
    }
    if (!_requests.tryAcquire()) {
      throw error(503, "Native upload capacity is exhausted");
    }
    try {
      var snapshot = _metadata.snapshot();
      var tableRecord = snapshot.tables().get(table);
      if (tableRecord == null) {
        throw error(404, "Table not found");
      }
      var schemaRecord = snapshot.schemas().get(TableNameBuilder.extractRawTableName(table));
      if (schemaRecord == null) {
        throw error(409, "Table schema is unavailable");
      }
      TableConfig config = JsonUtils.jsonNodeToObject(tableRecord.value(), TableConfig.class);
      NativeReplicaPlacementPolicy.validateTable(config);
      validateUploadTable(config);
      Schema schema = JsonUtils.jsonNodeToObject(schemaRecord.value(), Schema.class);
      var history = snapshot.revision().historyId();
      var content = _storage.store(body, headers.getMediaType().getParameters().get("boundary"), history,
          tableRecord.objectId(), config, schema);
      // Immutable name/content CAS always supplies parallel push protection, regardless of the legacy flag.
      authorize(table, headers);
      // The replicated operation checks these exact versions and the current controller owner atomically.
      var result = _tasks.publish(content, new ObjectVersion(history, tableRecord.objectId(), tableRecord.version()),
          new ObjectVersion(history, schemaRecord.objectId(), schemaRecord.version()));
      int status = switch (result.status()) {
        case OK -> 200;
        case INVALID_REQUEST -> 400;
        case NOT_FOUND -> 404;
        case CONFLICT, FENCED, HISTORY_MISMATCH -> 409;
        default -> 503;
      };
      String message = status == 200 ? "Successfully uploaded segment: " + content.segmentName() + " of table: " + table
          : result.status().name();
      return Response.status(status).entity(JsonUtils.objectToString(Map.of("status", message,
          "segmentName", content.segmentName(), "segmentObjectId", content.segmentObjectId(),
          "published", status == 200))).header(HttpHeaders.CACHE_CONTROL, "no-store").build();
    } catch (WebApplicationException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      throw error(400, "Invalid or unsupported segment upload");
    } catch (IOException e) {
      return error(503, "Native upload publication is unavailable; retry the identical archive").getResponse();
    } finally {
      _requests.release();
    }
  }

  private static String tableName(String name, String type, HttpHeaders headers) {
    try {
      if (name == null || name.isBlank() || !"OFFLINE".equalsIgnoreCase(type)) {
        throw error(400, "An explicit OFFLINE tableName is required");
      }
      String translated = DatabaseUtils.translateTableName(name, headers);
      String raw = TableNameBuilder.extractRawTableName(translated);
      if (TableNameBuilder.getTableTypeFromTableName(translated) == TableType.REALTIME
          || raw == null || raw.isBlank() || TableNameBuilder.getTableTypeFromTableName(raw) != null) {
        throw error(400, "Invalid OFFLINE table name");
      }
      return TableNameBuilder.OFFLINE.tableNameWithType(translated);
    } catch (IllegalArgumentException | DatabaseConflictException e) {
      throw error(400, "Invalid table name");
    }
  }

  private void authorize(String table, HttpHeaders headers) {
    var access = _access.create();
    String raw = TableNameBuilder.extractRawTableName(table);
    AccessControlUtils.validatePermission(raw, AccessType.CREATE, headers, "/v2/segments", access);
    if (!access.hasAccess(headers, TargetType.TABLE, raw, Actions.Table.UPLOAD_SEGMENT)) {
      throw error(403, "Upload permission denied");
    }
  }

  private static void validateHeaders(HttpHeaders headers) {
    String uploadType = headers.getHeaderString(CustomHeaders.UPLOAD_TYPE);
    if (uploadType != null && !"SEGMENT".equalsIgnoreCase(uploadType)) {
      throw error(400, "Native upload supports binary SEGMENT requests only");
    }
    for (String header : List.of(CustomHeaders.REFRESH_ONLY, CustomHeaders.DOWNLOAD_URI,
        CustomHeaders.COPY_SEGMENT_TO_DEEP_STORE, CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER,
        CustomHeaders.CRYPTER, CommonConstants.Controller.INGESTION_DESCRIPTOR, HttpHeaders.IF_MATCH,
        HttpHeaders.IF_NONE_MATCH)) {
      if (headers.getHeaderString(header) != null) {
        throw error(400, "Unsupported native upload option");
      }
    }
  }

  // Explicitly reject persisted legacy refresh controls as well as modern controls.
  @SuppressWarnings({"deprecation", "removal"})
  private static void validateUploadTable(TableConfig config) {
    var batch = config.getIngestionConfig() == null ? null : config.getIngestionConfig().getBatchIngestionConfig();
    if (config.getQuotaConfig() != null || config.getValidationConfig().getCrypterClassName() != null
        || "REFRESH".equalsIgnoreCase(config.getValidationConfig().getSegmentPushType())
        || batch != null && ("REFRESH".equalsIgnoreCase(batch.getSegmentIngestionType())
        || batch.getConsistentDataPush())) {
      throw error(501, "Native upload does not yet enforce quotas, encryption, refresh or consistent batch push");
    }
  }

  private static WebApplicationException error(int status, String message) {
    return new WebApplicationException(message, Response.status(status).type(MediaType.APPLICATION_JSON_TYPE)
        .header(HttpHeaders.CACHE_CONTROL, "no-store").entity(Map.of("error", message)).build());
  }
}
