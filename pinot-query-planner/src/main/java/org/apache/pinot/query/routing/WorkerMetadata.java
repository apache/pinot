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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * {@code WorkerMetadata} is used to send worker-level info about how to execute a stage on a particular worker.
 *
 * <p>It contains information specific to a single worker within a stage, such as:</p>
 * <ul>
 *   <li>the underlying segments this particular worker needs to execute.</li>
 *   <li>the mailbox info required to construct data transfer linkages.</li>
 *   <li>the partition mechanism of the data being execute on this worker.</li>
 * </ul>
 *
 * TODO: WorkerMetadata now doesn't have info directly about how to construct the mailboxes. instead it rely on
 * MailboxSendNode and MailboxReceiveNode to derive the info during runtime. this should changed to plan time soon.
 */
public class WorkerMetadata {
  public static final String TABLE_SEGMENTS_MAP_KEY = "tableSegmentsMap";

  private final int _workerId;
  private final Map<Integer, List<MailboxInfo>> _mailboxInfosMap;
  private final Map<String, String> _customProperties;

  public WorkerMetadata(int workerId, Map<Integer, List<MailboxInfo>> mailboxInfosMap) {
    _workerId = workerId;
    _mailboxInfosMap = mailboxInfosMap;
    _customProperties = new HashMap<>();
  }

  public WorkerMetadata(int workerId, Map<Integer, List<MailboxInfo>> mailboxInfosMap,
      Map<String, String> customProperties) {
    _workerId = workerId;
    _mailboxInfosMap = mailboxInfosMap;
    _customProperties = customProperties;
  }

  public int getWorkerId() {
    return _workerId;
  }

  public Map<Integer, List<MailboxInfo>> getMailboxInfosMap() {
    return _mailboxInfosMap;
  }

  public Map<String, String> getCustomProperties() {
    return _customProperties;
  }

  @Nullable
  public Map<String, List<String>> getTableSegmentsMap() {
    String tableSegmentsMapStr = _customProperties.get(TABLE_SEGMENTS_MAP_KEY);
    if (tableSegmentsMapStr != null) {
      try {
        return JsonUtils.stringToObject(tableSegmentsMapStr, new TypeReference<Map<String, List<String>>>() {
        });
      } catch (IOException e) {
        throw new RuntimeException("Unable to deserialize table segments map: " + tableSegmentsMapStr, e);
      }
    } else {
      return null;
    }
  }

  public boolean isLeafStageWorker() {
    return _customProperties.containsKey(TABLE_SEGMENTS_MAP_KEY);
  }

  public void setTableSegmentsMap(Map<String, List<String>> tableSegmentsMap) {
    String tableSegmentsMapStr;
    try {
      tableSegmentsMapStr = JsonUtils.objectToString(tableSegmentsMap);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Unable to serialize table segments map: " + tableSegmentsMap, e);
    }
    _customProperties.put(TABLE_SEGMENTS_MAP_KEY, tableSegmentsMapStr);
  }
}
