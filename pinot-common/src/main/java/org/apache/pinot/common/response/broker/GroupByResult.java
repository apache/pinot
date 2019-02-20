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
package org.apache.pinot.common.response.broker;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.io.Serializable;
import java.util.List;


@JsonPropertyOrder({"value", "group"})
public class GroupByResult {

  private Serializable _value;
  private List<String> _group;

  public GroupByResult() {
  }

  @JsonProperty("value")
  public Serializable getValue() {
    return _value;
  }

  @JsonProperty("value")
  public void setValue(Serializable value) {
    _value = value;
  }

  @JsonProperty("group")
  public List<String> getGroup() {
    return _group;
  }

  @JsonProperty("group")
  public void setGroup(List<String> group) {
    _group = group;
  }

  @Override
  public String toString() {
    return _group + ": " + _value.toString();
  }
}
