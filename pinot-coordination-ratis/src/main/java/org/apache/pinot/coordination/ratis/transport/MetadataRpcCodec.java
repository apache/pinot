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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Action;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangePage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangeTransaction;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Command;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedCursor;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Query;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol;
import org.apache.pinot.coordination.ratis.metadata.OperationProtocol;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.spi.utils.JsonUtils;


/// Converts a closed protobuf command vocabulary to the deterministic catalog protocol. JSON appears only in typed
/// Pinot schema/table values; it is strictly parsed without coercing exact decimal plugin configuration values.
final class MetadataRpcCodec {
  private static final ObjectMapper JSON = JsonMapper.builder(JsonFactory.builder()
          .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
          .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(100)
              .maxStringLength(MetadataProtocol.MAX_COMMAND_BYTES).build()).build())
      .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS).build();

  private MetadataRpcCodec() {
  }

  static Command command(MetadataRpc.CommandRequest request, String clientId)
      throws IOException {
    rejectUnknown(request);
    UUID history = UUID.fromString(request.getHistoryId());
    return switch (request.getCommandCase()) {
      case REGISTER -> new MetadataProtocol.RegisterClient(history, clientId,
          UUID.fromString(request.getRegister().getIncarnation()), request.getRegister().getExpectedEpoch());
      case RETIRE -> new MetadataProtocol.RetireClient(history, clientId, request.getRetire().getClientEpoch());
      case ACKNOWLEDGE -> new MetadataProtocol.Acknowledge(history, clientId,
          request.getAcknowledge().getClientEpoch(), request.getAcknowledge().getStreamId(),
          request.getAcknowledge().getThroughSequence());
      case TRANSACTION -> {
        MetadataRpc.Transaction transaction = request.getTransaction();
        if (transaction.getMutationsCount() > MetadataProtocol.MAX_MUTATIONS) {
          throw new IllegalArgumentException("Too many mutations");
        }
        List<Mutation> mutations = new ArrayList<>();
        for (MetadataRpc.Mutation mutation : transaction.getMutationsList()) {
          mutations.add(mutation(mutation));
        }
        yield new MetadataProtocol.Transaction(history, clientId, transaction.getClientEpoch(),
            transaction.getStreamId(), transaction.getSequence(), mutations);
      }
      default -> throw new IllegalArgumentException("A typed command is required");
    };
  }

  static Mutation mutation(MetadataRpc.Mutation mutation)
      throws IOException {
    rejectUnknown(mutation);
    RecordKind kind = kind(mutation.getKind());
    if (kind != RecordKind.CLUSTER_CONFIG && kind != RecordKind.SCHEMA && kind != RecordKind.TABLE_CONFIG) {
      throw new IllegalArgumentException("Coordination records require typed coordination commands");
    }
    if (!mutation.hasExpected()) {
      throw new IllegalArgumentException("Expected object version is required");
    }
    Action action = switch (mutation.getAction()) {
      case CREATE -> Action.CREATE;
      case UPDATE -> Action.UPDATE;
      case DELETE -> Action.DELETE;
      default -> throw new IllegalArgumentException("Unknown mutation action");
    };
    JsonNode value = mutation.hasValue() ? value(kind, mutation.getValue()) : null;
    return new Mutation(action, kind, mutation.getName(), new ObjectVersion(
        UUID.fromString(mutation.getExpected().getHistoryId()),
        UUID.fromString(mutation.getExpected().getObjectId()), mutation.getExpected().getVersion()), value);
  }

  static Query query(MetadataRpc.ReadRequest request, String clientId) {
    rejectUnknown(request);
    UUID history = UUID.fromString(request.getHistoryId());
    return switch (request.getQueryCase()) {
      case RECORD -> new MetadataProtocol.GetRecord(history, kind(request.getRecord().getKind()),
          request.getRecord().getName());
      case OUTCOME -> new MetadataProtocol.GetOutcome(history, clientId, request.getOutcome().getClientEpoch(),
          request.getOutcome().getStreamId(), request.getOutcome().getSequence());
      case REVISION -> new MetadataProtocol.GetRevision(history);
      default -> throw new IllegalArgumentException("A typed query is required");
    };
  }

  static MetadataRpc.CommandResult result(Result result)
      throws IOException {
    MetadataRpc.CommandResult.Builder builder = MetadataRpc.CommandResult.newBuilder()
        .setStatus(MetadataRpc.ResultStatus.valueOf(result.status().name()))
        .setRevision(MetadataRpc.Revision.newBuilder().setHistoryId(result.revision().historyId().toString())
            .setIndex(result.revision().index())).setDetail(result.detail());
    if (result.clientEpoch() != null) {
      builder.setClientEpoch(result.clientEpoch());
    }
    for (ObjectVersion version : result.versions()) {
      builder.addVersions(MetadataRpc.ObjectVersion.newBuilder().setHistoryId(version.historyId().toString())
          .setObjectId(version.objectId().toString()).setVersion(version.version()));
    }
    if (result.record() != null) {
      builder.setRecord(record(result.record()));
    }
    return builder.build();
  }

  static MetadataRpc.CatalogRecord record(CatalogRecord record)
      throws IOException {
    MetadataRpc.CatalogRecord.Builder builder = MetadataRpc.CatalogRecord.newBuilder()
        .setKind(MetadataRpc.RecordKind.valueOf(record.kind().name())).setName(record.name())
        .setObjectId(record.objectId().toString()).setVersion(record.version()).setSchemaVersion(record.schemaVersion())
        .setLastModifiedIndex(record.lastModifiedIndex()).setDeleted(record.deleted());
    if (record.value() != null) {
      MetadataRpc.CatalogValue.Builder value = MetadataRpc.CatalogValue.newBuilder();
      switch (record.kind()) {
        case CLUSTER_CONFIG -> {
          MetadataRpc.ClusterConfig.Builder config = MetadataRpc.ClusterConfig.newBuilder();
          record.value().properties().forEach(entry -> config.putProperties(entry.getKey(), entry.getValue().asText()));
          value.setClusterConfig(config);
        }
        case SCHEMA -> value.setSchemaJson(ByteString.copyFrom(JsonUtils.objectToBytes(record.value())));
        case TABLE_CONFIG -> value.setTableConfigJson(ByteString.copyFrom(JsonUtils.objectToBytes(record.value())));
        case MEMBERSHIP_CONTROL -> value.setMembershipControl(
            MembershipRpcCodec.control(MembershipProtocol.decodeControl(record.value())));
        case PARTICIPANT -> value.setParticipant(
            MembershipRpcCodec.participant(MembershipProtocol.decodeParticipant(record.value())));
        case OWNER_LAYOUT -> value.setOwnerLayout(
            ControllerRpcCodec.layout(OwnershipProtocol.decodeLayout(record.value())));
        case OWNER_ASSIGNMENT -> value.setOwnerAssignment(
            ControllerRpcCodec.assignment(OwnershipProtocol.decodeAssignment(record.value())));
        case CONTROLLER_OPERATION -> value.setControllerOperation(
            ControllerRpcCodec.operation(OperationProtocol.decodeOperation(record.value())));
        case OFFLINE_SEGMENT_CONTENT -> value.setOfflineSegmentContent(
            OfflineSegmentRpcCodec.content(OfflineSegmentProtocol.decodeContent(record.value())));
        case OFFLINE_REPLICA_ASSIGNMENT -> value.setOfflineReplicaAssignment(
            OfflineSegmentRpcCodec.assignment(OfflineSegmentProtocol.decodeAssignment(record.value())));
        case OFFLINE_INSTALLATION -> value.setOfflineInstallation(
            InstallationRpcCodec.receipt(OfflineInstallationProtocol.decodeReceipt(record.value())));
        case TASK_QUEUE -> value.setTaskQueue(TaskRpcCodec.queue(TaskProtocol.decodeQueue(record.value())));
        case TASK -> value.setTask(TaskRpcCodec.task(TaskProtocol.decodeTask(record.value())));
        case TASK_ATTEMPT -> value.setTaskAttempt(TaskRpcCodec.attempt(TaskProtocol.decodeAttempt(record.value())));
        case TASK_SCHEDULE -> value.setTaskSchedule(TaskRpcCodec.schedule(TaskProtocol.decodeSchedule(record.value())));
        case TASK_PUBLICATION -> value.setTaskPublication(
            TaskRpcCodec.publication(TaskProtocol.decodePublication(record.value())));
        default -> throw new IOException("Unsupported catalog record kind");
      }
      builder.setValue(value);
    }
    return builder.build();
  }

  static RecordKind kind(MetadataRpc.RecordKind kind) {
    return switch (kind) {
      case CLUSTER_CONFIG -> RecordKind.CLUSTER_CONFIG;
      case SCHEMA -> RecordKind.SCHEMA;
      case TABLE_CONFIG -> RecordKind.TABLE_CONFIG;
      case MEMBERSHIP_CONTROL -> RecordKind.MEMBERSHIP_CONTROL;
      case PARTICIPANT -> RecordKind.PARTICIPANT;
      case OWNER_LAYOUT -> RecordKind.OWNER_LAYOUT;
      case OWNER_ASSIGNMENT -> RecordKind.OWNER_ASSIGNMENT;
      case CONTROLLER_OPERATION -> RecordKind.CONTROLLER_OPERATION;
      case OFFLINE_SEGMENT_CONTENT -> RecordKind.OFFLINE_SEGMENT_CONTENT;
      case OFFLINE_REPLICA_ASSIGNMENT -> RecordKind.OFFLINE_REPLICA_ASSIGNMENT;
      case OFFLINE_INSTALLATION -> RecordKind.OFFLINE_INSTALLATION;
      case TASK_QUEUE -> RecordKind.TASK_QUEUE;
      case TASK -> RecordKind.TASK;
      case TASK_ATTEMPT -> RecordKind.TASK_ATTEMPT;
      case TASK_SCHEDULE -> RecordKind.TASK_SCHEDULE;
      case TASK_PUBLICATION -> RecordKind.TASK_PUBLICATION;
      default -> throw new IllegalArgumentException("Unknown catalog record kind");
    };
  }

  static MetadataRpc.Cursor cursor(FeedCursor cursor) {
    return MetadataRpc.Cursor.newBuilder().setHistoryId(cursor.historyId().toString()).setIndex(cursor.index())
        .setScopeId(cursor.scopeId()).build();
  }

  static FeedCursor cursor(MetadataRpc.Cursor cursor) {
    rejectUnknown(cursor);
    if (cursor.getIndex() < 0 || !cursor.getScopeId().matches("[a-f0-9]{64}")) {
      throw new IllegalArgumentException("Invalid metadata feed cursor");
    }
    return new FeedCursor(UUID.fromString(cursor.getHistoryId()), cursor.getIndex(), cursor.getScopeId());
  }

  static CatalogRecord record(MetadataRpc.CatalogRecord record)
      throws IOException {
    rejectUnknown(record);
    RecordKind kind = kind(record.getKind());
    if (record.getName().isBlank() || record.getVersion() < 1 || record.getSchemaVersion() != 1
        || record.getLastModifiedIndex() < 1 || record.getDeleted() == record.hasValue()) {
      throw new IllegalArgumentException("Invalid catalog record");
    }
    return new CatalogRecord(kind, record.getName(), UUID.fromString(record.getObjectId()), record.getVersion(),
        record.getSchemaVersion(), record.getLastModifiedIndex(), record.getDeleted(),
        record.hasValue() ? value(kind, record.getValue()) : null);
  }

  static ChangePage changes(MetadataRpc.ChangePage page)
      throws IOException {
    rejectUnknown(page);
    if (!page.hasFrom() || !page.hasNext() || page.getRetentionFloor() < 0
        || page.getTransactionsCount() > MetadataProtocol.MAX_CHANGE_TRANSACTIONS) {
      throw new IllegalArgumentException("Invalid metadata change page");
    }
    List<ChangeTransaction> transactions = new ArrayList<>();
    int recordCount = 0;
    for (MetadataRpc.ChangeTransaction transaction : page.getTransactionsList()) {
      if (transaction.getPreviousIndex() < 0 || transaction.getIndex() <= transaction.getPreviousIndex()) {
        throw new IllegalArgumentException("Invalid metadata change transaction");
      }
      recordCount += transaction.getRecordsCount();
      if (recordCount > MetadataProtocol.MAX_CHANGE_RECORDS) {
        throw new IllegalArgumentException("Metadata page record limit exceeded");
      }
      List<CatalogRecord> records = new ArrayList<>();
      for (MetadataRpc.CatalogRecord record : transaction.getRecordsList()) {
        records.add(record(record));
      }
      transactions.add(new ChangeTransaction(transaction.getPreviousIndex(), transaction.getIndex(), records));
    }
    return new ChangePage(MetadataProtocol.Status.valueOf(page.getStatus().name()), cursor(page.getFrom()),
        cursor(page.getNext()), page.getRetentionFloor(), transactions, page.getHasMore());
  }

  private static JsonNode value(RecordKind kind, MetadataRpc.CatalogValue value)
      throws IOException {
    switch (value.getValueCase()) {
      case CLUSTER_CONFIG:
        if (kind == RecordKind.CLUSTER_CONFIG) {
          return JsonUtils.objectToJsonNode(value.getClusterConfig().getPropertiesMap());
        }
        break;
      case SCHEMA_JSON:
        if (kind == RecordKind.SCHEMA) {
          return object(value.getSchemaJson());
        }
        break;
      case TABLE_CONFIG_JSON:
        if (kind == RecordKind.TABLE_CONFIG) {
          return object(value.getTableConfigJson());
        }
        break;
      case MEMBERSHIP_CONTROL:
        if (kind == RecordKind.MEMBERSHIP_CONTROL) {
          return typedValue(MembershipRpcCodec.control(value.getMembershipControl()));
        }
        break;
      case PARTICIPANT:
        if (kind == RecordKind.PARTICIPANT) {
          return typedValue(MembershipRpcCodec.participant(value.getParticipant()));
        }
        break;
      case OWNER_LAYOUT:
        if (kind == RecordKind.OWNER_LAYOUT) {
          return typedValue(ControllerRpcCodec.layout(value.getOwnerLayout()));
        }
        break;
      case OWNER_ASSIGNMENT:
        if (kind == RecordKind.OWNER_ASSIGNMENT) {
          return typedValue(ControllerRpcCodec.assignment(value.getOwnerAssignment()));
        }
        break;
      case CONTROLLER_OPERATION:
        if (kind == RecordKind.CONTROLLER_OPERATION) {
          return typedValue(ControllerRpcCodec.operation(value.getControllerOperation()));
        }
        break;
      case OFFLINE_SEGMENT_CONTENT:
        if (kind == RecordKind.OFFLINE_SEGMENT_CONTENT) {
          return typedValue(OfflineSegmentRpcCodec.content(value.getOfflineSegmentContent()));
        }
        break;
      case OFFLINE_REPLICA_ASSIGNMENT:
        if (kind == RecordKind.OFFLINE_REPLICA_ASSIGNMENT) {
          return typedValue(OfflineSegmentRpcCodec.assignment(value.getOfflineReplicaAssignment()));
        }
        break;
      case OFFLINE_INSTALLATION:
        if (kind == RecordKind.OFFLINE_INSTALLATION) {
          return typedValue(InstallationRpcCodec.receipt(value.getOfflineInstallation()));
        }
        break;
      case TASK_QUEUE:
        if (kind == RecordKind.TASK_QUEUE) {
          return typedValue(TaskRpcCodec.queue(value.getTaskQueue()));
        }
        break;
      case TASK:
        if (kind == RecordKind.TASK) {
          return typedValue(TaskRpcCodec.task(value.getTask()));
        }
        break;
      case TASK_ATTEMPT:
        if (kind == RecordKind.TASK_ATTEMPT) {
          return typedValue(TaskRpcCodec.attempt(value.getTaskAttempt()));
        }
        break;
      case TASK_SCHEDULE:
        if (kind == RecordKind.TASK_SCHEDULE) {
          return typedValue(TaskRpcCodec.schedule(value.getTaskSchedule()));
        }
        break;
      case TASK_PUBLICATION:
        if (kind == RecordKind.TASK_PUBLICATION) {
          return typedValue(TaskRpcCodec.publication(value.getTaskPublication()));
        }
        break;
      default:
        break;
    }
    throw new IllegalArgumentException("Catalog value does not match its declared record kind");
  }

  private static JsonNode typedValue(Object value)
      throws IOException {
    // Parse the serialized representation just like catalog query responses, including integer node widths.
    return JSON.readTree(JsonUtils.objectToBytes(value));
  }

  private static JsonNode object(ByteString bytes)
      throws IOException {
    if (bytes.isEmpty() || bytes.size() > MetadataProtocol.MAX_COMMAND_BYTES) {
      throw new IllegalArgumentException("Invalid catalog value length");
    }
    JsonNode node = JSON.readTree(bytes.toByteArray());
    if (node == null || !node.isObject()) {
      throw new IllegalArgumentException("Catalog value must be an object");
    }
    return node;
  }

  static void rejectUnknown(Message message) {
    if (!message.getUnknownFields().asMap().isEmpty()) {
      throw new IllegalArgumentException("Unsupported metadata protocol field");
    }
    for (Map.Entry<FieldDescriptor, Object> field : message.getAllFields().entrySet()) {
      if (field.getValue() instanceof Message nested) {
        rejectUnknown(nested);
      } else if (field.getValue() instanceof List<?> values) {
        for (Object value : values) {
          if (value instanceof Message nested) {
            rejectUnknown(nested);
          }
        }
      }
    }
  }
}
