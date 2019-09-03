/*
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

package org.apache.pinot.thirdeye.dashboard.views;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GenericResponse {

  Info summary;

  ResponseSchema schema;
  List<String[]> responseData;
  Map<String, List<Integer>> keyToRowIdMapping;

  public Info getSummary() {
    return summary;
  }

  public void setSummary(Info summary) {
    this.summary = summary;
  }

  public ResponseSchema getSchema() {
    return schema;
  }

  public void setSchema(ResponseSchema schema) {
    this.schema = schema;
  }

  public List<String[]> getResponseData() {
    return responseData;
  }

  public void setResponseData(List<String[]> responseData) {
    this.responseData = responseData;
  }

  public Map<String, List<Integer>> getKeyToRowIdMapping() {
    return keyToRowIdMapping;
  }

  public void setKeyToRowIdMapping(Map<String, List<Integer>> keyToRowIdMapping) {
    this.keyToRowIdMapping = keyToRowIdMapping;
  }

  public static class ResponseSchema {

    Map<String, Integer> columnsToIndexMapping = new LinkedHashMap<>();

    public ResponseSchema() {

    }

    public ResponseSchema(String[] columns) {
      for (int i = 0; i < columns.length; i++) {
        add(columns[i], i);
      }
    }

    public Map<String, Integer> getColumnsToIndexMapping() {
      return columnsToIndexMapping;
    }

    public void add(String columnName, Integer index) {
      columnsToIndexMapping.put(columnName, index);
    }
  }

  public static class Info {

    private Map<String, String> simpleFields = new LinkedHashMap<>();
    private Map<String, List<String>> listFields = new LinkedHashMap<>();
    private Map<String, Map<String, String>> mapFields = new LinkedHashMap<>();

    public Map<String, String> getSimpleFields() {
      return simpleFields;
    }

    public Map<String, List<String>> getListFields() {
      return listFields;
    }

    public Map<String, Map<String, String>> getMapFields() {
      return mapFields;
    }

    public void addSimpleField(String name, String value) {
      simpleFields.put(name, value);
    }

    public void setListField(String group, List<String> values) {
      for (String value : values) {
        addListField(group, value);
      }
    }

    public void setListField(String group, String[] values) {
      for (String value : values) {
        addListField(group, value);
      }
    }

    public void addListField(String group, String value) {
      if (!listFields.containsKey(group)) {
        listFields.put(group, new ArrayList<String>());
      }
      listFields.get(group).add(value);
    }

    public void addMapField(String group, String name, String value) {
      if (!mapFields.containsKey(group)) {
        mapFields.put(group, new LinkedHashMap<String, String>());
      }
      mapFields.get(group).put(name, value);
    }
  }

  public static void main(String[] args) {
    GenericResponse heatMapResponse = new GenericResponse();
    ResponseSchema schema = new ResponseSchema();
    String[] columns = new String[] {
        "dimensionName", "dimValue", "m1.baseline", "m1.current", "m2.baseline", "m2.current"
    };
    for (int i = 0; i < columns.length; i++) {
      String column = columns[i];
      schema.add(column, i);
    }
    Info summary = new Info();
    summary.addSimpleField("baselineStart", "20160101");
    summary.addSimpleField("baselineEnd", "20160107");
    summary.addSimpleField("currentStart", "20160108");
    summary.addSimpleField("currentEnd", "20160105");
    summary.setListField("metrics", new String[] {
        "m1", "m2"
    });
    summary.setListField("dimensions", new String[] {
        "d1", "d2", "d3"
    });
    summary.addMapField("m1", "baselineTotal", "1000000");
    summary.addMapField("m2", "currentTotal", "1000090");

  }
}
