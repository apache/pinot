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
package org.apache.pinot.coordination.ratis.transport;

import com.google.protobuf.Message;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.common.coordination.MetadataRpcClient;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;


/// Controller-side typed model adapters over the backend-independent [MetadataRpcClient]. Wire transport and channel
/// ownership are shared with normal workers; strict domain decoding stays inside the Ratis metadata module.
public final class MetadataGrpcClient extends MetadataRpcClient {
  public MetadataGrpcClient(String host, int port, Path certificateChain, Path privateKey, Path trustCertificates,
      long timeoutMillis)
      throws IOException {
    super(host, port, certificateChain, privateKey, trustCertificates, timeoutMillis);
  }

  public static MetadataRpc.InstallationReport encodeInstallationReport(
      OfflineInstallationProtocol.ReportInstallation report) {
    return InstallationRpcCodec.report(report);
  }

  public static OfflineInstallationProtocol.WorkResult decodeInstallationWork(
      MetadataRpc.InstallationWorkResult response)
      throws IOException {
    validateSize(response);
    try {
      return InstallationRpcCodec.work(response);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid installation work response", e);
    }
  }

  public static MembershipProtocol.Response decodeMembershipResponse(MetadataRpc.MembershipResponse response)
      throws IOException {
    if (response == null || response.getSerializedSize() > MAX_MEMBERSHIP_BYTES) {
      throw new IOException("Membership response exceeds the transport byte limit");
    }
    try {
      return MembershipRpcCodec.response(response);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid membership response", e);
    }
  }

  /// Decodes a bounded typed result without changing its original committed revision or durable request outcome.
  public static Result decodeResult(MetadataRpc.CommandResult response)
      throws IOException {
    validateSize(response);
    try {
      MetadataRpcCodec.rejectUnknown(response);
      if (!response.hasRevision() || response.getRevision().getIndex() < 0
          || response.getVersionsCount() > MetadataProtocol.MAX_MUTATIONS
          || response.hasClientEpoch() && response.getClientEpoch() <= 0) {
        throw new IllegalArgumentException("Invalid metadata result envelope");
      }
      MetadataProtocol.Status status = MetadataProtocol.Status.valueOf(response.getStatus().name());
      UUID history = UUID.fromString(response.getRevision().getHistoryId());
      List<ObjectVersion> versions = new ArrayList<>(response.getVersionsCount());
      Set<UUID> objects = new HashSet<>();
      for (MetadataRpc.ObjectVersion encoded : response.getVersionsList()) {
        ObjectVersion version = OfflineSegmentRpcCodec.version(encoded);
        if (!history.equals(version.historyId()) || !objects.add(version.objectId())) {
          throw new IllegalArgumentException("Result version history or object identity differs");
        }
        versions.add(version);
      }
      CatalogRecord record = response.hasRecord() ? MetadataRpcCodec.record(response.getRecord()) : null;
      if (status != MetadataProtocol.Status.OK && (response.hasClientEpoch() || !versions.isEmpty() || record != null)
          || record != null && record.lastModifiedIndex() > response.getRevision().getIndex()) {
        throw new IllegalArgumentException("Result payload differs from status or committed revision");
      }
      return new Result(status, new Revision(history, response.getRevision().getIndex()), response.getDetail(),
          response.hasClientEpoch() ? response.getClientEpoch() : null, versions, record);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid metadata result response", e);
    }
  }

  public static MetadataProtocol.CatalogRecord decodeRecord(MetadataRpc.CatalogRecord record)
      throws IOException {
    validateSize(record);
    try {
      return MetadataRpcCodec.record(record);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid metadata record response", e);
    }
  }

  public static MetadataProtocol.FeedCursor decodeCursor(MetadataRpc.Cursor cursor)
      throws IOException {
    validateSize(cursor);
    try {
      return MetadataRpcCodec.cursor(cursor);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid metadata cursor response", e);
    }
  }

  public static MetadataProtocol.ChangePage decodeChanges(MetadataRpc.ChangePage page)
      throws IOException {
    validateSize(page);
    try {
      return MetadataRpcCodec.changes(page);
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid metadata change response", e);
    }
  }

  private static void validateSize(Message response)
      throws IOException {
    if (response == null || response.getSerializedSize() > MAX_RESPONSE_BYTES) {
      throw new IOException("Metadata response exceeds the transport byte limit");
    }
  }
}
