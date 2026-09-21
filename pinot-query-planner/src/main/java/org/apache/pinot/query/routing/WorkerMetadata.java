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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


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
/// TODO: WorkerMetadata now doesn't have info directly about how to construct the mailboxes. instead it rely on
/// MailboxSendNode and MailboxReceiveNode to derive the info during runtime. this should changed to plan time soon.
public class WorkerMetadata {
  /// Custom-property keys under which brokers that predate the proto segment list encoding ship the segment maps as
  /// JSON strings. Still written (when the proto encoding is disabled) and read by [QueryPlanSerDeUtils] so that mixed
  /// broker/server versions keep working; never present in [#getCustomProperties()] of a decoded instance.
  public static final String TABLE_SEGMENTS_MAP_KEY = "tableSegmentsMap";
  public static final String LOGICAL_TABLE_SEGMENTS_MAP_KEY = "logicalTableSegmentsMap";

  private final int _workerId;
  private final Map<Integer, MailboxInfos> _mailboxInfosMap;
  private final Map<String, String> _customProperties;
  @Nullable
  private Map<String, List<String>> _tableSegmentsMap;
  @Nullable
  private Map<String, List<String>> _logicalTableSegmentsMap;

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
    return _tableSegmentsMap;
  }

  /// Stores `tableSegmentsMap` by reference, and it is only encoded for the wire when the query is dispatched, so the
  /// caller must not mutate it (or its lists) once the plan is built.
  public void setTableSegmentsMap(Map<String, List<String>> tableSegmentsMap) {
    _tableSegmentsMap = tableSegmentsMap;
  }

  /// Segments to scan keyed by physical table name (with type suffix), or `null` for a worker that scans no logical
  /// table.
  @Nullable
  public Map<String, List<String>> getLogicalTableSegmentsMap() {
    return _logicalTableSegmentsMap;
  }

  /// Stores `logicalTableSegmentsMap` by reference, with the same no-mutation contract as [#setTableSegmentsMap].
  public void setLogicalTableSegmentsMap(Map<String, List<String>> logicalTableSegmentsMap) {
    _logicalTableSegmentsMap = logicalTableSegmentsMap;
  }

  /// A leaf-stage worker carries a (possibly empty) segment map; an intermediate-stage worker carries none.
  public boolean isLeafStageWorker() {
    return _tableSegmentsMap != null || _logicalTableSegmentsMap != null;
  }
}
