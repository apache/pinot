package org.apache.pinot.common.function.scalar;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pinot.spi.annotations.ScalarFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MapFunctions {
    private MapFunctions() {
    }

    /**
     * Internal Helper Function to convert Json String Values to Java Maps
     * @param jsonString
     * @return converted Java Map from JSON String
     */
    public static Map stringToMap(String jsonString) {
        //input validation for ObjectMapper.readValue();
        if (jsonString == null) {
            return null;
        }

        Map<Object, Object> convertedMap = null;
        try {
            //object --> java map with objectMapper
            convertedMap = new ObjectMapper().readValue(jsonString,
                    new TypeReference<Map<Object, Object>>() { });
        } catch (Exception e) {
            throw new RuntimeException("Unknown issue: " + jsonString);
        }

        return convertedMap;
    }

    /**
     * Scalar listKeys UDF that lists all keys from a String representation of JSON Map
     * @param jsonString
     * @return string array of all keys from JSON map in data
     */
    @ScalarFunction
    public static String[] listKeys(String jsonString) {
        //handle empty string case
        if (jsonString == null) {
            return new String[]{};
        }

        //convert string input to equivalent java map
        Map convertedMap = stringToMap(jsonString);

        //add map keys to an object list
        List<Object> mapList = new ArrayList<>(convertedMap.size());
        mapList.addAll(convertedMap.keySet());

        //convert object list to string list
        List<String> stringKeysList = new ArrayList<String>(convertedMap.size());
        for (Object key: mapList) {
            stringKeysList.add(key.toString());
        }

        //convert string keys list to string array and return
        return Arrays.copyOf(stringKeysList.toArray(), stringKeysList.size(), String[].class);
    }

}

