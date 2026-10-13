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
package org.apache.pinot.common.function.scalar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.annotations.ScalarFunction;

public class MapFunctions {
    private MapFunctions() { }

    public static final ObjectMapper oM = new ObjectMapper();

    /**
     * Internal Helper Function to convert Json String Values to Java Maps
     *
     * @param jsonString
     * @return converted Java Map from JSON String
     */
    public static LinkedHashMap stringToMap(String jsonString) {
        // input validation for ObjectMapper.readValue();
        if (jsonString == null) {
            return null;
        }

        LinkedHashMap<Object, Object> convertedMap = null;
        try {
            // object --> java map with objectMapper
            convertedMap =
                oM.readValue(jsonString, new TypeReference<LinkedHashMap<Object, Object>>() { });
        } catch (Exception e) {
            throw new RuntimeException("Unknown issue: " + jsonString);
        }

        return convertedMap;
    }

    /**
     * Scalar listMapEntries UDF that lists all key-value entries from a String representation of JSON
     * Map
     *
     * @param jsonString
     * @return string array of all key-value pairs from JSON map in data
     */
    @ScalarFunction
    public static String[] listMapEntries(String jsonString) {
        // handle empty string case
        if (jsonString == null) {
            return new String[0];
        }

        // convert string input to equivalent java map
        LinkedHashMap convertedMap = (LinkedHashMap) stringToMap(jsonString);

        // add map key elements to list
        List<Map.Entry<Object, Object>> keyList = new ArrayList<>(convertedMap.size());
        keyList.addAll(convertedMap.keySet());

        // convert raw map entry list to list of string key-value pair representations
        List<String> stringMapEntriesList = new ArrayList<String>(convertedMap.size());
        String currentEntryString = "";
        for (Object keyObj : keyList) {
            if (convertedMap.get(keyObj) != null) {
                currentEntryString += keyObj.toString() + " = " + convertedMap.get(keyObj).toString();
            } else {
                currentEntryString += keyObj.toString() + " = ";
            }
            stringMapEntriesList.add(currentEntryString);
            currentEntryString = "";
        }

        // convert string map entries list to string array and return
        return Arrays.copyOf(
            stringMapEntriesList.toArray(), stringMapEntriesList.size(), String[].class);
    }

    /**
     * Scalar listKeys UDF that lists all keys from a String representation of JSON Map
     *
     * @param jsonString
     * @return string array of all keys from JSON map in data
     */
    @ScalarFunction
    public static String[] listKeys(String jsonString) {
        // handle empty string case
        if (jsonString == null) {
            return new String[0];
        }

        // convert string input to equivalent java map
        LinkedHashMap convertedMap = (LinkedHashMap) stringToMap(jsonString);

        // add map key elements to list
        List<Map.Entry<Object, Object>> keyList = new ArrayList<>(convertedMap.size());
        keyList.addAll(convertedMap.keySet());

        // convert object list to string list
        List<String> stringKeysList = new ArrayList<String>(convertedMap.size());
        for (Object key : keyList) {
            stringKeysList.add(key.toString());
        }

        // convert string keys list to string array and return
        return Arrays.copyOf(stringKeysList.toArray(), stringKeysList.size(), String[].class);
    }

    /**
     * Scalar listValues UDF that lists all values from a String representation of JSON Map
     *
     * @param jsonString
     * @return string array of all values from JSON map in data
     */
    @ScalarFunction
    public static String[] listValues(String jsonString) {
        // handle empty string case
        if (jsonString == null) {
            return new String[0];
        }

        // convert string input to equivalent java map
        LinkedHashMap convertedMap = (LinkedHashMap) stringToMap(jsonString);

        // add map key elements to list
        List<Map.Entry<Object, Object>> keyList = new ArrayList<>(convertedMap.size());
        keyList.addAll(convertedMap.keySet());

        // convert object list to string list
        List<String> stringValuesList = new ArrayList<String>(convertedMap.size());
        for (Object key : keyList) {
            if (convertedMap.get(key) == null) {
                stringValuesList.add("null");
                continue;
            }
            stringValuesList.add(convertedMap.get(key).toString());
        }

        // convert string keys list to string array and return
        return Arrays.copyOf(stringValuesList.toArray(), stringValuesList.size(), String[].class);
    }

    /**
     * Scalar mapSize UDF that returns size/number of entries of the Map represented by a JSON Map
     * String
     *
     * @param jsonString
     * @return size of JSON map in data
     */
    @ScalarFunction
    public static int mapSize(String jsonString) {
        // handle empty string case
        if (jsonString == null) {
            return 0;
        }

        // convert string input to equivalent java map
        Map convertedMap = stringToMap(jsonString);

        // handle both cases for where returned map is null or is valid but empty
        if (convertedMap == null || convertedMap.entrySet().isEmpty()) {
            return 0;
        }

        // return number of entries extracted
        return convertedMap.entrySet().size();
    }

    /**
     * Internal Testing Function getValueFromKey that returns given key's corresponding value for
     * given JSON Map Object
     *
     * @param object
     * @param key
     * @return corresponding value for given key from JSON map in data
     */
    public static Object getValueFromKey(Object object, Object key) {
        // handle empty string case
        if (object == null) {
            return null;
        }

        // convert json object input to equivalent java map
        Map convertedMap = stringToMap((String) object);

        // handle both cases for where returned map is null or is valid but empty
        if (convertedMap == null || convertedMap.entrySet().isEmpty()) {
            return null;
        }

        // return key's value in map. returns null if key not found.
        return convertedMap.get(key);
    }
}
