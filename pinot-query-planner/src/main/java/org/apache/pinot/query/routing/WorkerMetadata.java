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
package org.apache.pinot.query.routing;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/// `WorkerMetadata` is used to send worker-level info about how to execute a stage on a particular worker.
///
/// It contains information specific to a single worker within a stage, such as:
///
/// - the underlying segments this particular worker needs to execute.
/// - the mailbox info required to construct data transfer linkages.
/// - the partition mechanism of the data being execute on this worker.
///
/// The segment maps are held as plain objects: they are only encoded for the wire in [QueryPlanSerDeUtils] when a
/// request is built for the server that runs the worker, so the planner never pays for encoding on the compile path.
///
/// On a server, a segment map that arrived in the legacy JSON encoding is kept as the raw JSON and only parsed on first
/// access, so that each worker parses its own list on its own thread when its leaf stage is compiled, rather than
/// every worker of a stage being parsed one after another while the request is deserialized.
///
/// Thread-safety: the raw JSON is set while the request is deserialized, before the instance is handed to a worker.
/// The parsed maps are published through `volatile` fields; two threads racing on the first access may both parse the
/// JSON, which is harmless since they produce equal maps.
///
/// TODO: WorkerMetadata now doesn't have info directly about how to construct the mailboxes. instead it rely on
/// MailboxSendNode and MailboxReceiveNode to derive the info during runtime. this should changed to plan time soon.
public class WorkerMetadata {
  /// Custom-property keys under which brokers that predate the proto segment list encoding ship the segment maps as
  /// JSON strings. Still written (when the proto encoding is disabled) and read by [QueryPlanSerDeUtils] so that mixed
  /// broker/server versions keep working; never present in [#getCustomProperties()] of a decoded instance.
  public static final String TABLE_SEGMENTS_MAP_KEY = "tableSegmentsMap";
  public static final String LOGICAL_TABLE_SEGMENTS_MAP_KEY = "logicalTableSegmentsMap";

  private static final TypeReference<Map<String, List<String>>> SEGMENTS_MAP_TYPE = new TypeReference<>() {
  };

  private final int _workerId;
  private final Map<Integer, MailboxInfos> _mailboxInfosMap;
  private final Map<String, String> _customProperties;
  @Nullable
  private volatile Map<String, List<String>> _tableSegmentsMap;
  @Nullable
  private volatile Map<String, List<String>> _logicalTableSegmentsMap;
  /// The legacy JSON encoding of [#_tableSegmentsMap] as received from the broker, parsed on first access.
  @Nullable
  private String _tableSegmentsMapJson;
  /// The legacy JSON encoding of [#_logicalTableSegmentsMap] as received from the broker, parsed on first access.
  @Nullable
  private String _logicalTableSegmentsMapJson;

  public WorkerMetadata(int workerId, Map<Integer, MailboxInfos> mailboxInfosMap) {
    this(workerId, mailboxInfosMap, new HashMap<>());
  }

  public WorkerMetadata(int workerId, Map<Integer, MailboxInfos> mailboxInfosMap,
      Map<String, String> customProperties) {
    _workerId = workerId;
    _mailboxInfosMap = mailboxInfosMap;
    _customProperties = customProperties;
  }

  public int getWorkerId() {
    return _workerId;
  }

  public Map<Integer, MailboxInfos> getMailboxInfosMap() {
    return _mailboxInfosMap;
  }

  public Map<String, String> getCustomProperties() {
    return _customProperties;
  }

  /// Segments to scan keyed by table type (`OFFLINE` / `REALTIME`), or `null` for a worker that scans no physical
  /// table (intermediate stage, or a logical-table leaf).
  @Nullable
  public Map<String, List<String>> getTableSegmentsMap() {
    Map<String, List<String>> tableSegmentsMap = _tableSegmentsMap;
    if (tableSegmentsMap == null && _tableSegmentsMapJson != null) {
      tableSegmentsMap = decodeSegmentsMapJson(_tableSegmentsMapJson);
      _tableSegmentsMap = tableSegmentsMap;
    }
    return tableSegmentsMap;
  }

  /// Stores `tableSegmentsMap` by reference, and it is only encoded for the wire when the query is dispatched, so the
  /// caller must not mutate it (or its lists) once the plan is built.
  public void setTableSegmentsMap(Map<String, List<String>> tableSegmentsMap) {
    _tableSegmentsMap = tableSegmentsMap;
  }

  /// Stores the legacy JSON encoding of the table segments map, to be parsed by [#getTableSegmentsMap] on first access.
  void setTableSegmentsMapJson(String tableSegmentsMapJson) {
    _tableSegmentsMapJson = tableSegmentsMapJson;
  }

  /// Segments to scan keyed by physical table name (with type suffix), or `null` for a worker that scans no logical
  /// table.
  @Nullable
  public Map<String, List<String>> getLogicalTableSegmentsMap() {
    Map<String, List<String>> logicalTableSegmentsMap = _logicalTableSegmentsMap;
    if (logicalTableSegmentsMap == null && _logicalTableSegmentsMapJson != null) {
      logicalTableSegmentsMap = decodeSegmentsMapJson(_logicalTableSegmentsMapJson);
      _logicalTableSegmentsMap = logicalTableSegmentsMap;
    }
    return logicalTableSegmentsMap;
  }

  /// Stores `logicalTableSegmentsMap` by reference, with the same no-mutation contract as [#setTableSegmentsMap].
  public void setLogicalTableSegmentsMap(Map<String, List<String>> logicalTableSegmentsMap) {
    _logicalTableSegmentsMap = logicalTableSegmentsMap;
  }

  /// Stores the legacy JSON encoding of the logical table segments map, to be parsed by
  /// [#getLogicalTableSegmentsMap] on first access.
  void setLogicalTableSegmentsMapJson(String logicalTableSegmentsMapJson) {
    _logicalTableSegmentsMapJson = logicalTableSegmentsMapJson;
  }

  /// A leaf-stage worker carries a (possibly empty) segment map, parsed or not; an intermediate-stage worker carries
  /// none.
  public boolean isLeafStageWorker() {
    return _tableSegmentsMap != null || _logicalTableSegmentsMap != null || _tableSegmentsMapJson != null
        || _logicalTableSegmentsMapJson != null;
  }

  private static Map<String, List<String>> decodeSegmentsMapJson(String segmentsMapJson) {
    try {
      return JsonUtils.stringToObject(segmentsMapJson, SEGMENTS_MAP_TYPE);
    } catch (IOException e) {
      throw new RuntimeException("Unable to deserialize segments map: " + segmentsMapJson, e);
    }
  }
}
