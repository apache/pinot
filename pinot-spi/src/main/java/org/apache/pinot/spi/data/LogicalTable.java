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
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.pinot.spi.utils.JsonUtils;


@JsonIgnoreProperties(ignoreUnknown = true)
public class LogicalTable {

  public static final String TABLE_NAME_KEY = "tableName";
  public static final String PHYSICAL_TABLE_NAMES_KEY = "physicalTableNames";

  private String _tableName;
  private List<String> _physicalTableNames;

  public static LogicalTable fromFile(File logicalTableFile)
      throws IOException {
    return JsonUtils.fileToObject(logicalTableFile, LogicalTable.class);
  }

  public static LogicalTable fromString(String logicalTableString)
      throws IOException {
    return JsonUtils.stringToObject(logicalTableString, LogicalTable.class);
  }

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public List<String> getPhysicalTableNames() {
    return _physicalTableNames;
  }

  public void setPhysicalTableNames(List<String> physicalTableNames) {
    _physicalTableNames = physicalTableNames;
  }

  private ObjectNode toJsonObject() {
    ObjectNode node = JsonUtils.newObjectNode().put("tableName", _tableName);
    ArrayNode arrayNode = JsonUtils.newArrayNode();
    for (String physicalTableName : _physicalTableNames) {
      arrayNode.add(physicalTableName);
    }
    node.set("physicalTableNames", arrayNode);
    return node;
  }

  /**
   * Returns a single-line json string representation of the schema.
   */
  public String toSingleLineJsonString() {
    return toJsonObject().toString();
  }

  /**
   * Returns a pretty json string representation of the schema.
   */
  public String toPrettyJsonString() {
    try {
      return JsonUtils.objectToPrettyString(toJsonObject());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    LogicalTable that = (LogicalTable) object;
    return Objects.equals(getTableName(), that.getTableName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTableName());
  }

  @Override
  public String toString() {
    return "LogicalTable{" + "_tableName='" + _tableName + '\'' + ", _physicalTableNames=" + _physicalTableNames + '}';
  }
}
