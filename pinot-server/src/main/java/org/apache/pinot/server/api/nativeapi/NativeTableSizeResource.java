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
package org.apache.pinot.server.api.nativeapi;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
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
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.common.restlet.resources.NativeTableSizeIdentity;
import org.apache.pinot.common.restlet.resources.ResourceUtils;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.core.data.manager.offline.NativeImmutableSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.server.api.resources.TableSizeResource;
import org.apache.pinot.server.starter.NativeServerParticipant;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Read-only local size observations with provenance from the same pinned native handles as the measured bytes.
/// The controller must match these identities to committed assignments; an observation grants no serving authority.
@Path("/")
public final class NativeTableSizeResource {
  private static final int MAX_SEGMENTS = 4096;
  private static final int MAX_RESPONSE_BYTES = 2 * 1024 * 1024;
  private final Supplier<NativeServerParticipant> _participant;

  public NativeTableSizeResource(Supplier<NativeServerParticipant> participant) {
    _participant = participant;
  }

  @GET
  @Path("/tables/{tableName}/size")
  @Produces(MediaType.APPLICATION_JSON)
  public String getTableSize(@PathParam("tableName") String tableName,
      @DefaultValue("true") @QueryParam("detailed") boolean detailed,
      @DefaultValue("false") @QueryParam("includeCompressionStats") boolean includeCompressionStats,
      @DefaultValue("false") @QueryParam("includeColumnCompressionStats") boolean includeColumnCompressionStats,
      @Context HttpHeaders headers) {
    if (includeCompressionStats || includeColumnCompressionStats) {
      throw new WebApplicationException("Native compression statistics are not supported", Response.Status.BAD_REQUEST);
    }
    tableName = DatabaseUtils.translateTableName(tableName, headers);
    if (!TableNameBuilder.isOfflineTableResource(tableName)) {
      throw new WebApplicationException("Native size observations require an OFFLINE table",
          Response.Status.BAD_REQUEST);
    }
    NativeServerParticipant participant = _participant.get();
    if (participant == null) {
      throw unavailable();
    }
    MetadataRpc.Revision revision;
    try {
      revision = participant.observationRevision();
    } catch (IllegalStateException e) {
      throw unavailable();
    }
    MetadataRpc.ParticipantToken token = participant.participant();
    TableDataManager table = participant.dataManager().getTableDataManager(tableName);
    if (table == null) {
      throw new WebApplicationException("Table is not installed", Response.Status.NOT_FOUND);
    }
    List<SegmentDataManager> segments = table.acquireAllSegments();
    try {
      if (segments.size() > MAX_SEGMENTS) {
        throw tooLarge();
      }
      Map<String, Identity> identities = new HashMap<>();
      for (SegmentDataManager segment : segments) {
        if (!(segment instanceof NativeImmutableSegmentDataManager nativeSegment)) {
          throw unavailable();
        }
        Identity identity = nativeSegment.getInstallationIdentity();
        if (!identity.tableName().equals(tableName)
            || identities.putIfAbsent(segment.getSegmentName(), identity) != null) {
          throw unavailable();
        }
      }
      NativeTableSizeIdentity provenance;
      try {
        provenance = NativeTableSizeIdentity.of(token, revision, identities);
      } catch (IllegalArgumentException e) {
        throw unavailable();
      }
      TableSizeInfo sizes = TableSizeResource.readTableSize(table, segments, detailed,
          includeCompressionStats, includeColumnCompressionStats).setNativeMetadata(provenance);
      // A failed heartbeat, fencing, or shutdown during collection must not become a successful observation.
      try {
        participant.observationRevision();
      } catch (IllegalStateException e) {
        throw unavailable();
      }
      String response = ResourceUtils.convertToJsonString(sizes);
      if (response.getBytes(StandardCharsets.UTF_8).length > MAX_RESPONSE_BYTES) {
        throw tooLarge();
      }
      return response;
    } finally {
      segments.forEach(table::releaseSegment);
    }
  }

  private static WebApplicationException tooLarge() {
    return new WebApplicationException("Native size observation exceeds supported bounds",
        Response.Status.REQUEST_ENTITY_TOO_LARGE);
  }

  private static WebApplicationException unavailable() {
    return new WebApplicationException("Native size observation is unavailable", Response.Status.SERVICE_UNAVAILABLE);
  }
}
