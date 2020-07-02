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
package org.apache.pinot.hadoop.io;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Json-based FieldExtractor converts record into {@link JsonNode} and extracts fields from it.
 */
public class JsonBasedFieldExtractor implements FieldExtractor<Object> {
  private Set<String> _fields;
  private Map<String, Object> _reuse = new HashMap<>();

  @Override
  public void init(Configuration conf, Set<String> fields) {
    _fields = fields;
    _reuse = new HashMap<>(HashUtil.getHashMapCapacity(fields.size()));
  }

  @Override
  public Map<String, Object> extractFields(Object record) {
    _reuse.clear();
    JsonNode jsonNode = JsonUtils.objectToJsonNode(record);
    for (String field : _fields) {
      _reuse.put(field, jsonNode.get(field));
    }
    return _reuse;
  }
}
