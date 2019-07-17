/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.apache.pinot.thirdeye.detection.formatter;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;


public class DetectionConfigFormatter implements ConfigFormatter<DetectionConfigDTO> {
  private static final String ATTR_ID = "id";
  private static final String ATTR_CREATED_BY = "createdBy";
  private static final String ATTR_UPDATED_BY = "updatedBy";
  private static final String ATTR_NAME = "name";
  private static final String ATTR_DESCRIPTION = "description";
  private static final String ATTR_LAST_TIMESTAMP = "lastTimestamp";
  private static final String ATTR_YAML = "yaml";
  private static final String ATTR_METRIC_URNS = "metricUrns";

  @Override
  public Map<String, Object> format(DetectionConfigDTO config) {
    Map<String, Object> output = new HashMap<>();
    output.put(ATTR_ID, config.getId());
    output.put(ATTR_CREATED_BY, config.getCreatedBy());
    output.put(ATTR_UPDATED_BY, config.getUpdatedBy());
    output.put(ATTR_NAME, config.getName());
    output.put(ATTR_DESCRIPTION, config.getDescription());
    output.put(ATTR_YAML, config.getYaml());
    output.put(ATTR_LAST_TIMESTAMP, config.getLastTimestamp());

    return output;
  }
}
