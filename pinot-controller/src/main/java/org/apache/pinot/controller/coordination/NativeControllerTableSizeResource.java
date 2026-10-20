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
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessControlUtils;
import org.apache.pinot.controller.api.access.AccessType;
import org.apache.pinot.controller.api.access.Authenticate;
import org.apache.pinot.controller.coordination.NativeControllerMetadata.Snapshot;
import org.apache.pinot.controller.util.TableSizeMetadataProvider.Replicas;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.controller.util.TableSizeReader.TableSizeDetails;
import org.apache.pinot.controller.util.TableSizeReader.TableSubTypeSizeDetails;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Normal table-size API backed by coherent native assignments and measured, identity-checked server reports.
/// Owns bounded HTTP fan-out resources. The runtime stops HTTP admission before closing this resource.
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public final class NativeControllerTableSizeResource implements AutoCloseable {
  private final NativeControllerMetadata _metadata;
  private final AccessControlFactory _accessControlFactory;
  private final ControllerMetrics _metrics;
  private final AuthProvider _serverAuth;
  private final int _timeoutMillis;
  private final ThreadPoolExecutor _executor;
  private final PoolingHttpClientConnectionManager _connections;
  private final NativeServerTableSizeCollector _collector;
  private final Semaphore _requests = new Semaphore(4);

  public NativeControllerTableSizeResource(NativeControllerMetadata metadata, ControllerConf config,
      AccessControlFactory accessControlFactory, ControllerMetrics metrics) {
    _metadata = metadata;
    _accessControlFactory = accessControlFactory;
    _metrics = metrics;
    _serverAuth = AuthProviderUtils.extractAuthProvider(config, ControllerConf.CONTROLLER_SERVER_ADMIN_AUTH_PREFIX);
    _timeoutMillis = Math.toIntExact(TimeUnit.SECONDS.toMillis(config.getServerAdminRequestTimeoutSeconds()));
    _executor = new ThreadPoolExecutor(4, 4, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(128), runnable -> {
      Thread thread = new Thread(runnable, "native-controller-server-observations");
      thread.setDaemon(true);
      return thread;
    });
    _collector = new NativeServerTableSizeCollector(config, _executor, _timeoutMillis);
    _connections = _collector.connections();
  }

  @GET
  @Path("/tables/{tableName}/size")
  @Authenticate(AccessType.READ)
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_SIZE)
  public Response tableSize(@PathParam("tableName") String tableName,
      @DefaultValue("true") @QueryParam("verbose") boolean verbose,
      @DefaultValue("true") @QueryParam("includeReplacedSegments") boolean includeReplacedSegments,
      @DefaultValue("false") @QueryParam("includeColumnCompressionStats") boolean includeColumnCompressionStats,
      @Context HttpHeaders headers) {
    String translated;
    try {
      translated = DatabaseUtils.translateTableName(tableName, headers);
    } catch (RuntimeException e) {
      throw failure(400, "Invalid database-qualified table name");
    }
    String raw = TableNameBuilder.extractRawTableName(translated);
    if (raw == null || raw.isBlank() || TableNameBuilder.getTableTypeFromTableName(raw) != null) {
      throw failure(400, "Native table names must contain at most one table type suffix");
    }
    authorize(raw, headers);
    if (TableNameBuilder.getTableTypeFromTableName(translated) == TableType.REALTIME
        || includeColumnCompressionStats) {
      throw failure(501, "Native size currently supports OFFLINE disk sizes without column compression statistics");
    }
    if (!_requests.tryAcquire()) {
      throw failure(503, "Native table-size read capacity is exhausted");
    }
    try {
      Snapshot snapshot = _metadata.snapshot();
      NativeTableSizeMetadataProvider provider = new NativeTableSizeMetadataProvider(snapshot, raw, _serverAuth);
      TableSizeDetails result;
      if (provider.empty()) {
        result = new TableSizeDetails(raw);
        result._offlineSegments = new TableSubTypeSizeDetails();
      } else {
        TableSizeReader reader = new TableSizeReader(_executor, _connections, _metrics, provider) {
          @Override
          protected Map<String, TableSizeInfo> readServerReports(Replicas replicas, String typedName, int timeoutMs) {
            return _collector.read(replicas, typedName, timeoutMs);
          }
        };
        result = reader.getTableSizeDetails(raw, _timeoutMillis, includeReplacedSegments);
      }
      if (result == null) {
        throw failure(404, "Table not found");
      }
      Snapshot validated = _metadata.snapshot();
      provider.validateCurrent(validated);
      authorize(raw, headers);
      if (!verbose && result._offlineSegments != null) {
        result._offlineSegments._segments = Map.of();
      }
      return Response.ok(JsonUtils.objectToString(result)).header(HttpHeaders.CACHE_CONTROL, "no-store")
          .header(NativeControllerResource.REVISION_HEADER,
              validated.revision().historyId() + ":" + validated.revision().index()).build();
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      throw failure(503, "Native table size is unavailable");
    } finally {
      _requests.release();
    }
  }

  private void authorize(String tableName, HttpHeaders headers) {
    AccessControl access = _accessControlFactory.create();
    String path = "/tables/" + tableName + "/size";
    AccessControlUtils.validatePermission(tableName, AccessType.READ, headers, path, access);
    if (!access.hasAccess(headers, TargetType.TABLE, tableName, Actions.Table.GET_SIZE)) {
      throw failure(403, "Permission denied");
    }
  }

  private static WebApplicationException failure(int status, String message) {
    return new WebApplicationException(Response.status(status).type(MediaType.APPLICATION_JSON_TYPE)
        .header(HttpHeaders.CACHE_CONTROL, "no-store").entity(Map.of("error", message)).build());
  }

  @Override
  public void close() throws IOException {
    _executor.shutdownNow();
    _collector.close();
  }
}
