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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.AssignedOfflineSegmentReload;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.CompletionProof;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.OfflineSegmentReload;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Operation;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.OperationState;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Progress;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.Spec;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.State;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol.TargetMaterialization;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.Assignment;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.ClusterWork;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.HashVersion;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.Layout;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.Scope;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.TableShard;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.TablelessTask;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;


/// Closed controller operation vocabulary and typed feed projections. Mutating requests derive owner identity from
/// authenticated controller policy; full owner tokens are output values, never caller-selected mutation authority.
final class ControllerRpcCodec {
  private ControllerRpcCodec() {
  }

  static MetadataProtocol.NativeOperationCommand command(MetadataRpc.CommandRequest request, String clientId,
      ParticipantIdentity identity)
      throws IOException {
    MetadataRpcCodec.rejectUnknown(request);
    require(request.hasControllerOperation() && identity.role() == Role.CONTROLLER,
        "A controller operation and authenticated controller participant are required");
    UUID history = UUID.fromString(request.getHistoryId());
    MetadataRpc.ControllerOperationRequest envelope = request.getControllerOperation();
    require(envelope.getClientEpoch() > 0 && envelope.getSequence() > 0, "Invalid operation request identity");
    identifier(envelope.getStreamId());
    Operation operation = switch (envelope.getOperationCase()) {
      case SUBMIT -> {
        MetadataRpc.SubmitOperation submit = envelope.getSubmit();
        require(submit.hasInput() && submit.getInput().hasOfflineSegmentReload(),
            "Submit requires bare reload input; authoritative dependencies are derived by consensus");
        yield new OperationProtocol.Submit(UUID.fromString(submit.getOperationId()), spec(submit.getInput()));
      }
      case CLAIM -> {
        MetadataRpc.ClaimOperation claim = envelope.getClaim();
        require(claim.hasOwner() && claim.getExpectedVersion() > 0, "Claim owner and version required");
        yield new OperationProtocol.Claim(UUID.fromString(claim.getOperationId()), claim.getExpectedVersion(),
            owner(history, claim.getOwner(), identity), dependencies(claim.getDependenciesList()));
      }
      case CHECKPOINT -> {
        MetadataRpc.CheckpointOperation checkpoint = envelope.getCheckpoint();
        require(checkpoint.hasOwner() && checkpoint.hasProgress() && checkpoint.getExpectedVersion() > 0
            && checkpoint.getAttempt() > 0, "Checkpoint owner, attempt, version and progress required");
        yield new OperationProtocol.Checkpoint(UUID.fromString(checkpoint.getOperationId()),
            checkpoint.getExpectedVersion(), owner(history, checkpoint.getOwner(), identity), checkpoint.getAttempt(),
            progress(checkpoint.getProgress()));
      }
      case COMPLETE -> {
        MetadataRpc.CompleteOperation complete = envelope.getComplete();
        require(complete.hasOwner() && complete.hasResult() && complete.getExpectedVersion() > 0
            && complete.getAttempt() > 0 && complete.getEffectsCount() < MetadataProtocol.MAX_MUTATIONS,
            "Completion owner, attempt, version and bounded effects required");
        State terminal = state(complete.getTerminal());
        require(terminal == State.SUCCEEDED || terminal == State.FAILED, "Invalid completion state");
        List<Mutation> effects = new ArrayList<>();
        for (MetadataRpc.Mutation effect : complete.getEffectsList()) {
          require(effect.getKind() == MetadataRpc.RecordKind.TABLE_CONFIG
              && effect.getAction() == MetadataRpc.Mutation.Action.UPDATE, "Only table updates are operation effects");
          effects.add(MetadataRpcCodec.mutation(effect));
        }
        yield new OperationProtocol.Complete(UUID.fromString(complete.getOperationId()), complete.getExpectedVersion(),
            owner(history, complete.getOwner(), identity), complete.getAttempt(), terminal,
            progress(complete.getResult()), effects);
      }
      case CANCEL -> {
        MetadataRpc.CancelOperation cancel = envelope.getCancel();
        require(cancel.hasOwner() && cancel.getExpectedVersion() > 0, "Cancellation owner and version required");
        detail(cancel.getDetail());
        yield new OperationProtocol.Cancel(UUID.fromString(cancel.getOperationId()), cancel.getExpectedVersion(),
            owner(history, cancel.getOwner(), identity), cancel.getDetail());
      }
      case COMPLETE_INSTALLED -> {
        MetadataRpc.CompleteInstalledOperation complete = envelope.getCompleteInstalled();
        require(complete.hasOwner() && complete.getExpectedVersion() > 0 && complete.getAttempt() > 0,
            "Installed completion owner, attempt and version required");
        yield new OperationProtocol.CompleteInstalled(UUID.fromString(complete.getOperationId()),
            complete.getExpectedVersion(), owner(history, complete.getOwner(), identity), complete.getAttempt());
      }
      default -> throw new IllegalArgumentException("A typed controller operation is required");
    };
    MetadataProtocol.NativeOperationCommand command = new MetadataProtocol.NativeOperationCommand(history, clientId,
        envelope.getClientEpoch(), envelope.getStreamId(), envelope.getSequence(), operation);
    // Protobuf and the replicated encoding have independent size bounds; reject expansion before consensus append.
    MetadataProtocol.encode(command);
    return command;
  }

  private static OwnerToken owner(UUID history, MetadataRpc.ClaimedOwner owner, ParticipantIdentity identity) {
    require(owner.hasScope() && owner.hasSession() && owner.getLayoutGeneration() > 0 && owner.getOwnerEpoch() > 0
        && owner.getSession().getRegistrationEpoch() > 0, "Complete claimed owner required");
    return new OwnerToken(history, owner.getLayoutGeneration(), scope(owner.getScope()), owner.getOwnerEpoch(),
        new ParticipantToken(history, identity.participantId(), Role.CONTROLLER,
            UUID.fromString(owner.getSession().getIncarnation()), owner.getSession().getRegistrationEpoch()));
  }

  static MetadataRpc.OwnershipLayout layout(Layout layout) {
    return MetadataRpc.OwnershipLayout.newBuilder().setGeneration(layout.generation())
        .setTableShards(layout.tableShards())
        .setHashVersion(MetadataRpc.OwnershipHashVersion.valueOf(layout.hashVersion().name())).build();
  }

  static Layout layout(MetadataRpc.OwnershipLayout layout) {
    MetadataRpcCodec.rejectUnknown(layout);
    require(layout.getGeneration() == 1 && layout.getTableShards() == 24, "Unsupported ownership layout");
    return new Layout(layout.getGeneration(), layout.getTableShards(),
        HashVersion.valueOf(layout.getHashVersion().name()));
  }

  static MetadataRpc.OwnershipAssignment assignment(Assignment assignment) {
    MetadataRpc.OwnershipAssignment.Builder result = MetadataRpc.OwnershipAssignment.newBuilder()
        .setLayoutGeneration(assignment.layoutGeneration()).setScope(scope(assignment.scope()))
        .setOwnerEpoch(assignment.ownerEpoch());
    if (assignment.owner() != null) {
      result.setOwner(MembershipRpcCodec.participant(assignment.owner()));
    }
    return result.build();
  }

  static Assignment assignment(MetadataRpc.OwnershipAssignment assignment) {
    MetadataRpcCodec.rejectUnknown(assignment);
    require(assignment.getLayoutGeneration() == 1 && assignment.hasScope() && assignment.getOwnerEpoch() > 0,
        "Invalid ownership assignment");
    ParticipantToken owner = assignment.hasOwner() ? participant(assignment.getOwner(), Role.CONTROLLER) : null;
    return new Assignment(assignment.getLayoutGeneration(), scope(assignment.getScope()), assignment.getOwnerEpoch(),
        owner);
  }

  private static MetadataRpc.OwnershipScope scope(Scope scope) {
    MetadataRpc.OwnershipScope.Builder result = MetadataRpc.OwnershipScope.newBuilder();
    return switch (scope) {
      case TableShard table -> result.setTableShard(table.shard()).build();
      case ClusterWork work -> result.setClusterWork(work.name()).build();
      case TablelessTask task -> result.setTablelessTask(task.taskType()).build();
    };
  }

  private static Scope scope(MetadataRpc.OwnershipScope scope) {
    return switch (scope.getScopeCase()) {
      case TABLE_SHARD -> {
        require(scope.getTableShard() >= 0 && scope.getTableShard() < 24, "Invalid table shard");
        yield new TableShard(scope.getTableShard());
      }
      case CLUSTER_WORK -> {
        identifier(scope.getClusterWork());
        yield new ClusterWork(scope.getClusterWork());
      }
      case TABLELESS_TASK -> {
        identifier(scope.getTablelessTask());
        yield new TablelessTask(scope.getTablelessTask());
      }
      default -> throw new IllegalArgumentException("Typed ownership scope required");
    };
  }

  static MetadataRpc.OwnerToken owner(OwnerToken owner) {
    return MetadataRpc.OwnerToken.newBuilder().setHistoryId(owner.historyId().toString())
        .setLayoutGeneration(owner.layoutGeneration()).setScope(scope(owner.scope())).setOwnerEpoch(owner.ownerEpoch())
        .setParticipant(MembershipRpcCodec.participant(owner.participant())).build();
  }

  static OwnerToken owner(MetadataRpc.OwnerToken owner) {
    require(owner.hasScope() && owner.hasParticipant() && owner.getLayoutGeneration() == 1 && owner.getOwnerEpoch() > 0,
        "Invalid stored owner token");
    UUID history = UUID.fromString(owner.getHistoryId());
    ParticipantToken participant = participant(owner.getParticipant(), Role.CONTROLLER);
    require(history.equals(participant.historyId()), "Owner participant history differs");
    return new OwnerToken(history, owner.getLayoutGeneration(), scope(owner.getScope()), owner.getOwnerEpoch(),
        participant);
  }

  static MetadataRpc.ControllerOperationState operation(OperationState operation) {
    MetadataRpc.ControllerOperationState.Builder result = MetadataRpc.ControllerOperationState.newBuilder()
        .setHistoryId(operation.historyId().toString()).setOperationId(operation.operationId().toString())
        .setInput(spec(operation.input())).setStatus(MetadataRpc.OperationStatus.valueOf(operation.status().name()))
        .setAttempt(operation.attempt()).setProgress(progress(operation.progress()));
    if (operation.owner() != null) {
      result.setOwner(owner(operation.owner()));
    }
    for (ObjectVersion dependency : operation.dependencies()) {
      result.addDependencies(version(dependency));
    }
    if (operation.result() != null) {
      result.setResult(progress(operation.result()));
    }
    if (operation.completionProof() != null) {
      result.setCompletionProof(MetadataRpc.InstallationCompletionProof.newBuilder().addAllInstallations(
          operation.completionProof().installations().stream().map(ControllerRpcCodec::version).toList()));
    }
    return result.build();
  }

  static OperationState operation(MetadataRpc.ControllerOperationState operation) {
    MetadataRpcCodec.rejectUnknown(operation);
    require(operation.hasInput() && operation.hasProgress() && operation.getAttempt() >= 0,
        "Operation input and progress required");
    UUID history = UUID.fromString(operation.getHistoryId());
    State status = state(operation.getStatus());
    Spec input = spec(operation.getInput());
    OwnerToken owner = operation.hasOwner() ? owner(operation.getOwner()) : null;
    List<ObjectVersion> dependencies = dependencies(operation.getDependenciesList());
    require(owner == null || history.equals(owner.historyId()), "Operation owner history differs");
    for (ObjectVersion dependency : dependencies) {
      require(history.equals(dependency.historyId()), "Operation dependency history differs");
    }
    if (input instanceof AssignedOfflineSegmentReload assigned) {
      for (ObjectVersion dependency : assigned.dependencies()) {
        require(history.equals(dependency.historyId()), "Assigned input dependency history differs");
      }
    }
    for (TargetMaterialization target : reload(input).targets()) {
      require(history.equals(target.server().historyId()), "Operation target history differs");
    }
    boolean accepted = status == State.ACCEPTED;
    boolean terminal = status == State.SUCCEEDED || status == State.FAILED || status == State.CANCELLED;
    require(accepted ? owner == null && operation.getAttempt() == 0 && dependencies.isEmpty()
        : owner != null && operation.getAttempt() > 0, "Operation owner or attempt does not match state");
    require(terminal == operation.hasResult(), "Operation result does not match terminal state");
    Progress progress = progress(operation.getProgress());
    Progress result = operation.hasResult() ? progress(operation.getResult()) : null;
    int targets = reload(input).targets().size();
    require(progress.completedTargets() + progress.failedTargets() <= targets,
        "Operation progress exceeds target count");
    require(!terminal || progress.equals(result), "Operation result and progress differ");
    require(status != State.SUCCEEDED || progress.completedTargets() == targets && progress.failedTargets() == 0,
        "Successful operation requires every target");
    CompletionProof proof = operation.hasCompletionProof()
        ? new CompletionProof(dependencies(operation.getCompletionProof().getInstallationsList())) : null;
    require(proof == null || status == State.SUCCEEDED && !proof.installations().isEmpty(),
        "Installation proof requires successful completion");
    return new OperationState(history, UUID.fromString(operation.getOperationId()), input, status,
        operation.getAttempt(), owner, dependencies, progress, result, proof);
  }

  private static MetadataRpc.OperationSpec spec(Spec spec) {
    return switch (spec) {
      case AssignedOfflineSegmentReload assigned -> MetadataRpc.OperationSpec.newBuilder()
          .setAssignedOfflineSegmentReload(MetadataRpc.AssignedOfflineSegmentReload.newBuilder()
              .setReload(spec(assigned.reload()).getOfflineSegmentReload())
              .addAllDependencies(assigned.dependencies().stream().map(ControllerRpcCodec::version).toList())).build();
      case OfflineSegmentReload reload -> {
        MetadataRpc.OfflineSegmentReload.Builder value = MetadataRpc.OfflineSegmentReload.newBuilder()
            .setTableName(reload.tableName()).setTableObjectId(reload.tableObjectId().toString())
            .setSegmentName(reload.segmentName()).setSegmentObjectId(reload.segmentObjectId().toString())
            .setContentSha256(reload.contentSha256()).setForceDownload(reload.forceDownload())
            .setInitialLoad(reload.initialLoad()).setReconcile(reload.reconcile())
            .setSubmittedAtMs(reload.submittedAtMs());
        for (TargetMaterialization target : reload.targets()) {
          value.addTargets(MetadataRpc.TargetMaterialization.newBuilder()
              .setServer(MembershipRpcCodec.participant(target.server())).setAssignmentEpoch(target.assignmentEpoch())
              .setMaterializationId(target.materializationId().toString()));
        }
        yield MetadataRpc.OperationSpec.newBuilder().setOfflineSegmentReload(value).build();
      }
    };
  }

  private static Spec spec(MetadataRpc.OperationSpec spec) {
    if (spec.hasAssignedOfflineSegmentReload()) {
      MetadataRpc.AssignedOfflineSegmentReload assigned = spec.getAssignedOfflineSegmentReload();
      require(assigned.hasReload() && assigned.getDependenciesCount() > 0,
          "Assigned reload requires authoritative dependencies");
      return new AssignedOfflineSegmentReload((OfflineSegmentReload) spec(MetadataRpc.OperationSpec.newBuilder()
          .setOfflineSegmentReload(assigned.getReload()).build()), dependencies(assigned.getDependenciesList()));
    }
    require(spec.hasOfflineSegmentReload(), "Typed offline reload input required");
    MetadataRpc.OfflineSegmentReload reload = spec.getOfflineSegmentReload();
    identifier(reload.getTableName());
    identifier(reload.getSegmentName());
    require(reload.getContentSha256().matches("[0-9a-f]{64}") && reload.getTargetsCount() > 0
        && reload.getTargetsCount() <= OperationProtocol.MAX_TARGETS && reload.getSubmittedAtMs() >= 0
        && !(reload.getInitialLoad() && reload.getReconcile()), "Invalid content identity, target bound or intent");
    List<TargetMaterialization> targets = new ArrayList<>();
    Set<String> servers = new HashSet<>();
    Set<UUID> materializations = new HashSet<>();
    for (MetadataRpc.TargetMaterialization target : reload.getTargetsList()) {
      require(target.hasServer() && target.getAssignmentEpoch() > 0, "Reload target identity required");
      ParticipantToken server = participant(target.getServer(), Role.SERVER);
      UUID materialization = UUID.fromString(target.getMaterializationId());
      require(servers.add(server.participantId()) && materializations.add(materialization), "Duplicate reload target");
      targets.add(new TargetMaterialization(server, target.getAssignmentEpoch(), materialization));
    }
    return new OfflineSegmentReload(reload.getTableName(), UUID.fromString(reload.getTableObjectId()),
        reload.getSegmentName(), UUID.fromString(reload.getSegmentObjectId()), reload.getContentSha256(), targets,
        reload.getForceDownload(), reload.getInitialLoad(), reload.getReconcile(), reload.getSubmittedAtMs());
  }

  private static OfflineSegmentReload reload(Spec spec) {
    return switch (spec) {
      case OfflineSegmentReload reload -> reload;
      case AssignedOfflineSegmentReload assigned -> assigned.reload();
    };
  }

  private static ParticipantToken participant(MetadataRpc.ParticipantToken value, Role role) {
    ParticipantToken token = MembershipRpcCodec.participant(value);
    identifier(token.participantId());
    require(token.role() == role && token.registrationEpoch() > 0, "Invalid participant role or registration");
    return token;
  }

  private static List<ObjectVersion> dependencies(List<MetadataRpc.ObjectVersion> values) {
    require(values.size() <= OperationProtocol.MAX_DEPENDENCIES, "Too many operation dependencies");
    List<ObjectVersion> result = new ArrayList<>();
    Set<UUID> ids = new HashSet<>();
    for (MetadataRpc.ObjectVersion value : values) {
      UUID id = UUID.fromString(value.getObjectId());
      require(value.getVersion() > 0 && ids.add(id), "Invalid or duplicate dependency version");
      result.add(new ObjectVersion(UUID.fromString(value.getHistoryId()), id, value.getVersion()));
    }
    return List.copyOf(result);
  }

  private static MetadataRpc.ObjectVersion version(ObjectVersion version) {
    return MetadataRpc.ObjectVersion.newBuilder().setHistoryId(version.historyId().toString())
        .setObjectId(version.objectId().toString()).setVersion(version.version()).build();
  }

  private static MetadataRpc.OperationProgress progress(Progress progress) {
    return MetadataRpc.OperationProgress.newBuilder().setCompletedTargets(progress.completedTargets())
        .setFailedTargets(progress.failedTargets()).setDetail(progress.detail()).build();
  }

  private static Progress progress(MetadataRpc.OperationProgress progress) {
    require(progress.getCompletedTargets() >= 0 && progress.getFailedTargets() >= 0
        && progress.getCompletedTargets() <= OperationProtocol.MAX_TARGETS - progress.getFailedTargets(),
        "Invalid operation progress counts");
    detail(progress.getDetail());
    return new Progress(progress.getCompletedTargets(), progress.getFailedTargets(), progress.getDetail());
  }

  private static State state(MetadataRpc.OperationStatus status) {
    return State.valueOf(status.name());
  }

  private static void detail(String detail) {
    require(detail.length() <= OperationProtocol.MAX_DETAIL_LENGTH, "Operation detail exceeds supported bound");
  }

  private static void identifier(String value) {
    require(!value.isBlank() && value.length() <= 1024 && value.chars().noneMatch(Character::isISOControl),
        "Invalid controller identifier");
  }

  private static void require(boolean valid, String message) {
    if (!valid) {
      throw new IllegalArgumentException(message);
    }
  }
}
