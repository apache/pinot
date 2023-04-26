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
 * The {@code WorkerMetadata} info contains the information for executing a particular worker on a particular stage.
 *
 * <p>It contains information about:</p>
 * <ul>
 *   <li>the tables it is suppose to scan (including real-time and offline portions separately)</li>
 *   <li>the underlying segments a stage requires to execute upon if it is a leaf stage worker</li>
 *   <li>the partition mechanism of the data being execute on this worker</li>
 * </ul>
 *
 * <p>WorkerMetadata should allow {@link org.apache.pinot.query.planner.stage.MailboxSendNode} and
 * {@link org.apache.pinot.query.planner.stage.MailboxReceiveNode} to derive the appropriate set of mailboxes to be
 * created when executing the worker.</p>
 */
public class WorkerMetadata {
  private final VirtualServerAddress _virtualServerAddress;
  private final Map<String, String> _customProperties;

  private WorkerMetadata(VirtualServerAddress virtualServerAddress, Map<String, String> customProperties) {
    _virtualServerAddress = virtualServerAddress;
    _customProperties = customProperties;
  }

  public VirtualServerAddress getVirtualServerAddress() {
    return _virtualServerAddress;
  }

  public Map<String, String> getCustomProperties() {
    return _customProperties;
  }

  public static class Builder {
    public static final String TABLE_SEGMENTS_MAP_KEY = "tableSegmentsMap";
    private VirtualServerAddress _virtualServerAddress;
    private Map<String, String> _customProperties;

    public Builder() {
      _customProperties = new HashMap<>();
    }

    public Builder setVirtualServerAddress(VirtualServerAddress virtualServerAddress) {
      _virtualServerAddress = virtualServerAddress;
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
      return new WorkerMetadata(_virtualServerAddress, _customProperties);
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
