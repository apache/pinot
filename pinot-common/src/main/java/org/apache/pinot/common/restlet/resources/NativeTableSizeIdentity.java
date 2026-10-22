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
package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;

import static com.google.common.base.Preconditions.checkArgument;


/// Provenance of measured native segment sizes. This observation grants no query or publication authority.
/// Controllers match every reported segment to a coherent catalog image before using its size.
public final class NativeTableSizeIdentity {
  private final UUID _historyId;
  private final String _participantId;
  private final UUID _incarnation;
  private final long _registrationEpoch;
  private final long _revision;
  private final Map<String, SegmentIdentity> _segments;

  @JsonCreator
  public NativeTableSizeIdentity(
      @JsonProperty("historyId") UUID historyId,
      @JsonProperty("participantId") String participantId,
      @JsonProperty("incarnation") UUID incarnation,
      @JsonProperty("registrationEpoch") long registrationEpoch,
      @JsonProperty("revision") long revision,
      @JsonProperty("segments") Map<String, SegmentIdentity> segments) {
    _historyId = historyId;
    _participantId = participantId;
    _incarnation = incarnation;
    _registrationEpoch = registrationEpoch;
    _revision = revision;
    _segments = segments == null ? null : Map.copyOf(segments);
  }

  /// Captures only identities from the same pinned segment handles used to compute the sizes.
  public static NativeTableSizeIdentity of(MetadataRpc.ParticipantToken participant, MetadataRpc.Revision revision,
      Map<String, Identity> identities) {
    checkArgument(participant.getHistoryId().equals(revision.getHistoryId()) && revision.getIndex() > 0,
        "Native size observation requires one history and a positive revision");
    Map<String, SegmentIdentity> segments = new HashMap<>();
    for (Map.Entry<String, Identity> entry : identities.entrySet()) {
      Identity identity = entry.getValue();
      checkArgument(entry.getKey().equals(identity.segmentName())
          && identity.historyId().toString().equals(participant.getHistoryId())
          && identity.server().participantId().equals(participant.getParticipantId())
          && identity.server().incarnation().toString().equals(participant.getIncarnation())
          && identity.server().registrationEpoch() == participant.getRegistrationEpoch(),
          "Native size observation does not match the installed process identity");
      segments.put(entry.getKey(), new SegmentIdentity(identity.table().objectId(), identity.table().version(),
          identity.schema().objectId(), identity.schema().version(), identity.content().objectId(),
          identity.content().version(), identity.contentSha256(), identity.assignment().objectId(),
          identity.assignment().version(), identity.materializationId()));
    }
    return new NativeTableSizeIdentity(UUID.fromString(participant.getHistoryId()), participant.getParticipantId(),
        UUID.fromString(participant.getIncarnation()), participant.getRegistrationEpoch(), revision.getIndex(),
        segments);
  }

  public UUID getHistoryId() {
    return _historyId;
  }

  public String getParticipantId() {
    return _participantId;
  }

  public UUID getIncarnation() {
    return _incarnation;
  }

  public long getRegistrationEpoch() {
    return _registrationEpoch;
  }

  public long getRevision() {
    return _revision;
  }

  public Map<String, SegmentIdentity> getSegments() {
    return _segments;
  }

  /// Exact installed content, configuration, and assignment identities for one measured segment.
  public static final class SegmentIdentity {
    private final UUID _tableObjectId;
    private final long _tableVersion;
    private final UUID _schemaObjectId;
    private final long _schemaVersion;
    private final UUID _segmentObjectId;
    private final long _contentVersion;
    private final String _contentSha256;
    private final UUID _assignmentObjectId;
    private final long _assignmentEpoch;
    private final UUID _materializationId;

    @JsonCreator
    public SegmentIdentity(
        @JsonProperty("tableObjectId") UUID tableObjectId,
        @JsonProperty("tableVersion") long tableVersion,
        @JsonProperty("schemaObjectId") UUID schemaObjectId,
        @JsonProperty("schemaVersion") long schemaVersion,
        @JsonProperty("segmentObjectId") UUID segmentObjectId,
        @JsonProperty("contentVersion") long contentVersion,
        @JsonProperty("contentSha256") String contentSha256,
        @JsonProperty("assignmentObjectId") UUID assignmentObjectId,
        @JsonProperty("assignmentEpoch") long assignmentEpoch,
        @JsonProperty("materializationId") UUID materializationId) {
      _tableObjectId = tableObjectId;
      _tableVersion = tableVersion;
      _schemaObjectId = schemaObjectId;
      _schemaVersion = schemaVersion;
      _segmentObjectId = segmentObjectId;
      _contentVersion = contentVersion;
      _contentSha256 = contentSha256;
      _assignmentObjectId = assignmentObjectId;
      _assignmentEpoch = assignmentEpoch;
      _materializationId = materializationId;
    }

    public UUID getTableObjectId() {
      return _tableObjectId;
    }

    public long getTableVersion() {
      return _tableVersion;
    }

    public UUID getSchemaObjectId() {
      return _schemaObjectId;
    }

    public long getSchemaVersion() {
      return _schemaVersion;
    }

    public UUID getSegmentObjectId() {
      return _segmentObjectId;
    }

    public long getContentVersion() {
      return _contentVersion;
    }

    public String getContentSha256() {
      return _contentSha256;
    }

    public UUID getAssignmentObjectId() {
      return _assignmentObjectId;
    }

    public long getAssignmentEpoch() {
      return _assignmentEpoch;
    }

    public UUID getMaterializationId() {
      return _materializationId;
    }
  }
}
