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
  private final VirtualServerAddress _virtualServerAddress;
  private final Map<Integer, MailboxMetadata> _mailBoxInfosMap;
  private final Map<String, String> _customProperties;

  private WorkerMetadata(VirtualServerAddress virtualServerAddress, Map<Integer, MailboxMetadata> mailBoxInfosMap,
      Map<String, String> customProperties) {
    _virtualServerAddress = virtualServerAddress;
    _mailBoxInfosMap = mailBoxInfosMap;
    _customProperties = customProperties;
  }

  public VirtualServerAddress getVirtualServerAddress() {
    return _virtualServerAddress;
  }

  public Map<Integer, MailboxMetadata> getMailBoxInfosMap() {
    return _mailBoxInfosMap;
  }

  public Map<String, String> getCustomProperties() {
    return _customProperties;
  }

  public static class Builder {
    public static final String TABLE_SEGMENTS_MAP_KEY = "tableSegmentsMap";
    private VirtualServerAddress _virtualServerAddress;
    private Map<Integer, MailboxMetadata> _mailBoxInfosMap;
    private Map<String, String> _customProperties;

    public Builder() {
      _mailBoxInfosMap = new HashMap<>();
      _customProperties = new HashMap<>();
    }

    public Builder setVirtualServerAddress(VirtualServerAddress virtualServerAddress) {
      _virtualServerAddress = virtualServerAddress;
      return this;
    }

    public Builder putAllMailBoxInfosMap(Map<Integer, MailboxMetadata> mailBoxInfosMap) {
      _mailBoxInfosMap.putAll(mailBoxInfosMap);
      return this;
    }

    public Builder addMailBoxInfoMap(Integer planFragmentId, MailboxMetadata mailBoxMetadata) {
      _mailBoxInfosMap.put(planFragmentId, mailBoxMetadata);
      return this;
    }

    public Builder addTableSegmentsMap(Map<String, List<String>> tableSegmentsMap) {
      try {
        String tableSegmentsMapStr = JsonUtils.objectToString(tableSegmentsMap);
        _customProperties.put(TABLE_SEGMENTS_MAP_KEY, tableSegmentsMapStr);
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Unable to serialize table segments map", e);
      }
      return this;
    }

    public WorkerMetadata build() {
      return new WorkerMetadata(_virtualServerAddress, _mailBoxInfosMap, _customProperties);
    }

    public void putAllCustomProperties(Map<String, String> customPropertyMap) {
      _customProperties.putAll(customPropertyMap);
    }
  }

  public static Map<String, List<String>> getTableSegmentsMap(WorkerMetadata workerMetadata) {
    String tableSegmentKeyStr = workerMetadata.getCustomProperties().get(Builder.TABLE_SEGMENTS_MAP_KEY);
    if (tableSegmentKeyStr != null) {
      try {
        return JsonUtils.stringToObject(tableSegmentKeyStr, new TypeReference<Map<String, List<String>>>() {
        });
      } catch (IOException e) {
        throw new RuntimeException("Unable to deserialize table segments map", e);
      }
    } else {
      return null;
    }
  }
}
