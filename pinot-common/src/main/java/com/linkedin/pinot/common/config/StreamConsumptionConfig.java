/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.config;

import java.io.IOException;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamConsumptionConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamConsumptionConfig.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String _streamPartitionAssignmentStrategy;

  public String getStreamPartitionAssignmentStrategy() {
    return _streamPartitionAssignmentStrategy;
  }

  public void setStreamPartitionAssignmentStrategy(String streamPartitionAssignmentStrategy) {
    _streamPartitionAssignmentStrategy = streamPartitionAssignmentStrategy;
  }

  @Override
  public String toString() {
    try {
      return OBJECT_MAPPER.writeValueAsString(this);
    } catch (IOException e) {
      return e.toString();
    }
  }

}
