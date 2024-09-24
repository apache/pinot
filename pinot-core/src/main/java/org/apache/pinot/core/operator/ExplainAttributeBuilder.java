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
package org.apache.pinot.core.operator;

import java.util.Map;
import org.apache.pinot.common.proto.Plan;


/**
 * A builder used to build attributes for an explain node without having to deal with the protobuf API.
 */
public class ExplainAttributeBuilder {
  private final Map<String, Plan.ExplainNode.AttributeValue> _attributes = new java.util.HashMap<>();

  public ExplainAttributeBuilder putString(String key, String value) {
    _attributes.put(key, Plan.ExplainNode.AttributeValue.newBuilder().setString(value).build());
    return this;
  }

  public ExplainAttributeBuilder putLong(String key, long value) {
    _attributes.put(key, Plan.ExplainNode.AttributeValue.newBuilder().setLong(value).build());
    return this;
  }

  public ExplainAttributeBuilder putLongIdempotent(String key, long value) {
    _attributes.put(key, Plan.ExplainNode.AttributeValue.newBuilder()
        .setLong(value)
        .setMergeType(Plan.ExplainNode.AttributeValue.MergeType.IDEMPOTENT)
        .build());
    return this;
  }

  public ExplainAttributeBuilder putBool(String key, boolean value) {
    _attributes.put(key, Plan.ExplainNode.AttributeValue.newBuilder().setBool(value).build());
    return this;
  }

  public ExplainAttributeBuilder putStringList(String key, Iterable<String> value) {
    Plan.ExplainNode.AttributeValue attValue = Plan.ExplainNode.AttributeValue
        .newBuilder()
        .setStringList(
            Plan.ExplainNode.StringList.newBuilder()
                .addAllValues(value)
                .build())
        .build();
    _attributes.put(key, attValue);
    return this;
  }

  public ExplainAttributeBuilder putAttribute(String key, Plan.ExplainNode.AttributeValue value) {
    _attributes.put(key, value);
    return this;
  }

  public Map<String, Plan.ExplainNode.AttributeValue> build() {
    return _attributes;
  }
}
