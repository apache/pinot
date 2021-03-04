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
package org.apache.pinot.compat.tests;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;


@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = SegmentOp.class, name = "segmentOp"),
    @JsonSubTypes.Type(value = TableOp.class, name = "tableOp"),
    @JsonSubTypes.Type(value = QueryOp.class, name = "queryOp"),
    @JsonSubTypes.Type(value = StreamOp.class, name = "streamOp")
})
public abstract class BaseOp {
  enum OpType {
    TABLE_OP,
    SEGMENT_OP,
    QUERY_OP,
    STREAM_OP,
  }

  private String _name;
  private final OpType _opType;
  private String _description = "No description provided";

  protected BaseOp(OpType opType) {
    _opType = opType;
  }

  public String getName() {
    return _name;
  }

  public void setName(String name) {
    _name = name;
  }

  public void setDescription(String description) {
    _description = description;
  }

  public String getDescription() {
    return _description;
  }

  public  boolean run(int generationNumber) {
    System.out.println("Running OpType " + _opType.toString() + ": " + getDescription());
    return runOp(generationNumber);
  }

  abstract boolean runOp(int generationNumber);
}
