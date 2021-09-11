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
package org.apache.pinot.common.metadata;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;


public class MetadataUtils {
  private MetadataUtils() {
  }

  public static boolean comparisonZNRecords(ZNRecord record1, ZNRecord record2) {
    // Compare Id;
    if (!record1.getId().equals(record2.getId())) {
      return false;
    }
    // Compare Simple fields;
    Map<String, String> simpleFieldMap1 = record1.getSimpleFields();
    Map<String, String> simpleFieldMap2 = record2.getSimpleFields();
    if (simpleFieldMap1.size() != simpleFieldMap2.size()) {
      return false;
    }
    for (String key : simpleFieldMap1.keySet()) {
      if (simpleFieldMap1.get(key) != null) {
        if (!simpleFieldMap1.get(key).equals(simpleFieldMap2.get(key))) {
          return false;
        }
      } else {
        if (simpleFieldMap2.get(key) != null) {
          return false;
        }
      }
    }
    // Compare List fields;
    Map<String, List<String>> listFieldMap1 = record1.getListFields();
    Map<String, List<String>> listFieldMap2 = record2.getListFields();
    if (listFieldMap1.size() != listFieldMap2.size()) {
      return false;
    }
    for (String key : listFieldMap1.keySet()) {
      if (listFieldMap1.get(key) != null) {
        List<String> list1 = listFieldMap1.get(key);
        List<String> list2 = listFieldMap2.get(key);
        if (list2 == null) {
          return false;
        } else {
          if (list1.size() == list2.size()) {
            String[] list1Array = list1.toArray(new String[0]);
            Arrays.sort(list1Array);
            String[] list2Array = list2.toArray(new String[0]);
            Arrays.sort(list2Array);
            for (int i = 0; i < list1.size(); ++i) {
              if (list1Array[i] == null && list2Array[i] == null) {
                continue;
              }
              if (list1Array[i] == null || list2Array[i] == null) {
                return false;
              }
              if (!list1Array[i].equals(list2Array[i])) {
                return false;
              }
            }
          } else {
            return false;
          }
        }
      } else {
        if (listFieldMap2.get(key) != null) {
          return false;
        }
      }
    }
    // Compare Map fields;
    Map<String, Map<String, String>> mapFieldMap1 = record1.getMapFields();
    Map<String, Map<String, String>> mapFieldMap2 = record2.getMapFields();
    if (mapFieldMap1.size() != mapFieldMap2.size()) {
      return false;
    }
    for (String key : mapFieldMap1.keySet()) {
      if (!mapFieldMap2.containsKey(key)) {
        return false;
      }
      Map<String, String> map1 = mapFieldMap1.get(key);
      Map<String, String> map2 = mapFieldMap2.get(key);
      if (map1 == null && map2 == null) {
        continue;
      }
      if (map1 == null || map2 == null) {
        return false;
      }
      for (String mapKey : map1.keySet()) {
        if (!map2.containsKey(mapKey)) {
          return false;
        }
        String value1 = map1.get(mapKey);
        String value2 = map2.get(mapKey);
        if (value1 == null && value2 == null) {
          continue;
        }
        if (value1 == null || value2 == null) {
          return false;
        }
        if (!value1.equals(value2)) {
          return false;
        }
      }
    }
    return true;
  }
}
