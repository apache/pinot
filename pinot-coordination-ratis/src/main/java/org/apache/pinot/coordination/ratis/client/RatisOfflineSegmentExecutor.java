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
package org.apache.pinot.coordination.ratis.client;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Installation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Mode;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Operation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Owner;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ReadGuard;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Request;
import org.apache.pinot.common.config.provider.TableDataManagerMetadata.SegmentSnapshot;
import org.apache.pinot.common.metadata.segment.SegmentLoadMetadata;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.Materialization;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.ReportInstallation;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.Work;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.WorkResult;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.LoaderDescriptor;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.TableShard;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;


/// Connects an authenticated, coherent own-target work read to the native segment installer and receipt publisher.
/// Local installation never constitutes controller success. The publisher must retain the exact durable request
/// identity across unknown outcomes; this adapter neither advances its stream nor acknowledges a failed response.
/// Instances can execute independent operations concurrently; the table manager serializes each segment's install.
public final class RatisOfflineSegmentExecutor {
  private final ParticipantToken _server;
  private final WorkSource _source;
  private final ReceiptPublisher _publisher;

  @FunctionalInterface
  public interface WorkSource {
    /// Return a fresh committed own-target capture authorized for this executor's SERVER session.
    WorkResult lookup(UUID operationId) throws Exception;
  }

  @FunctionalInterface
  public interface ReceiptPublisher {
    /// Publish through the authenticated SERVER lane, preserving the same request identity on an unknown outcome.
    Result publish(ReportInstallation installation) throws Exception;
  }

  /// A physical result and, only after installation, the authoritative receipt result. Neither grants serving rights.
  public record Execution(NativeOfflineSegmentAction.Result local, @Nullable Result receipt) {
  }

  public RatisOfflineSegmentExecutor(ParticipantToken server, WorkSource source, ReceiptPublisher publisher) {
    _server = Objects.requireNonNull(server, "server");
    if (server.role() != Role.SERVER || server.historyId() == null || server.incarnation() == null
        || server.registrationEpoch() <= 0 || server.participantId() == null || server.participantId().isBlank()) {
      throw new IllegalArgumentException("A complete SERVER registration is required");
    }
    _source = Objects.requireNonNull(source, "source");
    _publisher = Objects.requireNonNull(publisher, "publisher");
  }

  public Execution execute(UUID operationId, NativeOfflineSegmentAction.Executor installer)
      throws Exception {
    Objects.requireNonNull(installer, "installer");
    Work work = lookup(operationId);
    if (work == null) {
      return new Execution(new NativeOfflineSegmentAction.Result(NativeOfflineSegmentAction.Status.FENCED, null), null);
    }
    Identity identity = identity(work);
    Installation previous = installer.installed(identity.segmentName());
    Request request = new Request(work.reconcile() ? Mode.RECONCILE : work.initialLoad() ? Mode.LOAD : Mode.RELOAD,
        identity, snapshot(work),
        work.forceDownload(), previous == null ? null : previous.installationId());
    NativeOfflineSegmentAction.Result local = installer.execute(request, new ExecutionAuthority(operationId, work));
    if (local.status() != NativeOfflineSegmentAction.Status.INSTALLED) {
      return new Execution(local, null);
    }
    Installation installed = Objects.requireNonNull(local.installation(), "installed result");
    require(identity.equals(installed.identity()), "Installer returned a different installation identity");
    ReportInstallation report = new ReportInstallation(operationId, work.attempt(), work.owner(), work.target(),
        installed.installationId(), installed.manifestSha256());
    return new Execution(local, _publisher.publish(report));
  }

  @Nullable
  private Work lookup(UUID operationId)
      throws Exception {
    require(operationId != null, "Operation identity is required");
    WorkResult response = _source.lookup(operationId);
    if (response != null && (response.status() == Status.FENCED || response.status() == Status.NOT_FOUND
        || response.status() == Status.CONFLICT || response.status() == Status.NOT_READY
        || response.status() == Status.HISTORY_MISMATCH)) {
      return null;
    }
    require(response != null && response.status() == Status.OK && response.revision() != null
        && _server.historyId().equals(response.revision().historyId()) && response.work() != null,
        "Current installation work is unavailable: " + (response == null ? "null" : response.status()));
    Work work = response.work();
    require(operationId.equals(work.operationId()) && work.attempt() > 0 && work.target() != null
        && _server.equals(work.target().server()), "Installation work belongs to another operation or SERVER session");
    identity(work);
    return work;
  }

  private Identity identity(Work work)
      throws IOException {
    require(!work.initialLoad() || !work.reconcile(), "Installation work has conflicting action intents");
    Materialization target = work.target();
    Content content = work.content();
    OwnerToken owner = work.owner();
    require(owner != null && _server.historyId().equals(owner.historyId()) && owner.scope() instanceof TableShard
        && owner.participant() != null && _server.historyId().equals(owner.participant().historyId())
        && owner.participant().role() == Role.CONTROLLER, "Installation work has an invalid owner");
    require(content != null && content.tableName().equals(target.tableName())
        && content.tableObjectId().equals(target.tableObjectId()) && content.segmentName().equals(target.segmentName())
        && content.segmentObjectId().equals(target.segmentObjectId())
        && content.contentSha256().equals(target.contentSha256()), "Installation content identity differs");
    require(work.table() != null && work.schema() != null && !work.table().deleted() && !work.schema().deleted()
        && work.table().kind() == RecordKind.TABLE_CONFIG && work.schema().kind() == RecordKind.SCHEMA
        && target.tableName().equals(work.table().name()) && target.tableObjectId().equals(work.table().objectId())
        && target.compatibility() != null && target.compatibility().table() != null
        && target.compatibility().schema() != null, "Installation table/schema dependencies are absent");
    require(version(target.compatibility().table()).equals(new ObjectVersion(work.table().objectId(),
        work.table().version())) && version(target.compatibility().schema()).equals(
            new ObjectVersion(work.schema().objectId(), work.schema().version())),
        "Installation table/schema compatibility differs");
    return new Identity(_server.historyId(), target.tableName(), version(target.compatibility().table()),
        work.schema().name(), version(target.compatibility().schema()), target.segmentName(),
        new ObjectVersion(target.segmentObjectId(), 1), target.contentSha256(), participant(_server),
        version(target.assignment()), target.materializationId(), new Operation(work.operationId(), work.attempt(),
            new Owner(owner.layoutGeneration(), ((TableShard) owner.scope()).shard(), owner.ownerEpoch(),
                participant(owner.participant()))));
  }

  public static SegmentSnapshot snapshot(Work work)
      throws IOException {
    TableConfig table = JsonUtils.jsonNodeToObject(work.table().value(), TableConfig.class);
    Schema schema = JsonUtils.jsonNodeToObject(work.schema().value(), Schema.class);
    LoaderDescriptor loader = work.content().loader();
    SegmentLoadMetadata segment = new SegmentLoadMetadata(work.content().segmentName(), loader.crc(), loader.dataCrc(),
        loader.useDataCrc(), work.content().downloadUri(), loader.tier(), loader.crypterName(), loader.creationTimeMs(),
        loader.pushTimeMs(), loader.customConfigs(), true);
    return new SegmentSnapshot(table, schema, segment);
  }

  private ObjectVersion version(MetadataProtocol.ObjectVersion version)
      throws IOException {
    require(version != null && _server.historyId().equals(version.historyId()),
        "Installation dependency belongs to another history");
    return new ObjectVersion(version.objectId(), version.version());
  }

  private static Participant participant(ParticipantToken participant) {
    return new Participant(participant.participantId(),
        NativeOfflineSegmentAction.Role.valueOf(participant.role().name()),
        participant.incarnation(), participant.registrationEpoch());
  }

  /// A refresh invalidates the previous local capture before doing remote I/O. Final validation holds the read lock
  /// until pointer publication completes. Consensus independently revalidates the report against later global changes.
  private final class ExecutionAuthority implements NativeOfflineSegmentAction.Authority {
    private final UUID _operationId;
    private final Work _expected;
    private final ReentrantReadWriteLock _lock = new ReentrantReadWriteLock();
    @Nullable
    private Work _current;

    private ExecutionAuthority(UUID operationId, Work expected) {
      _operationId = operationId;
      _expected = expected;
      _current = expected;
    }

    @Override
    public void refresh()
        throws Exception {
      _lock.writeLock().lock();
      try {
        _current = null;
      } finally {
        _lock.writeLock().unlock();
      }
      Work current = lookup(_operationId);
      _lock.writeLock().lock();
      try {
        _current = current;
      } finally {
        _lock.writeLock().unlock();
      }
    }

    @Override
    public ReadGuard acquire(Identity expected)
        throws IOException {
      _lock.readLock().lock();
      boolean current;
      try {
        current = _current != null && expected.equals(identity(_current))
            && _current.initialLoad() == _expected.initialLoad()
            && _current.reconcile() == _expected.reconcile()
            && _current.forceDownload() == _expected.forceDownload();
      } catch (IOException | RuntimeException e) {
        _lock.readLock().unlock();
        throw e;
      }
      return new ReadGuard() {
        @Override
        public boolean isCurrent() {
          return current;
        }

        @Override
        public void close() {
          _lock.readLock().unlock();
        }
      };
    }
  }

  private static void require(boolean condition, String message)
      throws IOException {
    if (!condition) {
      throw new IOException(message);
    }
  }
}
