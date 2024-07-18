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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;

public class MapFunctionsTest {

    @Test
    public void testRegMapSameTypes() throws JsonProcessingException {
        // test with all same type keys and values (Strings)
        String jsonString =
            "{"
                + "  \"id\": \"7044885078\","
                + "  \"type\": \"CreateEvent\","
                + "  \"public\": \"true\","
                + "  \"created_at\": \"2018-01-01T11:12:53Z\","
                + "    \"date\": \"November\","
                + "    \"occasion\": \"Anniversary\","
                + "    \"emotion\": \"Happiness\""
                + "}";

        // check individual elements correctly recorded
        assertEquals(MapFunctions.getValueFromKey(jsonString, "id"), "7044885078");
        assertEquals(MapFunctions.getValueFromKey(jsonString, "type"), "CreateEvent");
        assertEquals(MapFunctions.getValueFromKey(jsonString, "public"), "true");
        assertEquals(MapFunctions.getValueFromKey(jsonString, "date"), "November");
        assertEquals(MapFunctions.getValueFromKey(jsonString, "created_at"), "2018-01-01T11:12:53Z");
        assertEquals(MapFunctions.getValueFromKey(jsonString, "occasion"), "Anniversary");
        assertEquals(MapFunctions.getValueFromKey(jsonString, "emotion"), "Happiness");

        // check mapEntries list
        assertEquals(
            MapFunctions.listMapEntries(jsonString),
            new String[] {
                "id = 7044885078",
                "type = CreateEvent",
                "public = true",
                "created_at = 2018-01-01T11:12:53Z",
                "date = November",
                "occasion = Anniversary",
                "emotion = Happiness"
            });

        // check key, value lists and nPairs functions
        assertEquals(
            MapFunctions.listKeys(jsonString),
            new String[] {"id", "type", "public", "created_at", "date", "occasion", "emotion"});
        assertEquals(
            MapFunctions.listValues(jsonString),
            new String[] {
                "7044885078",
                "CreateEvent",
                "true",
                "2018-01-01T11:12:53Z",
                "November",
                "Anniversary",
                "Happiness"
            });
    }

    @Test
    public void testRegMapDiffTypes1() throws JsonProcessingException {
        // test with different type values (number, bool, array, string, null)
        String jsonString =
            "{"
                + "  \"id\": 704488,"
                + "  \"type\": false,"
                + "  \"public\": 4.0,"
                + "  \"created_at\": \"2018-01-01T11:12:53Z\","
                + "    \"date\": \"n\","
                + "    \"occasion\":  [80, 85, 90, 95, 100],"
                + "    \"extra\": null,"
                + "    \"emotion\": [\"happy\",\"sad\",\"angry\"]"
                + "}";

        // check individual elements correctly recorded
        assertEquals(MapFunctions.getValueFromKey(jsonString, "id"), 704488);
        assertEquals(MapFunctions.getValueFromKey(jsonString, "type"), false);
        assertEquals(MapFunctions.getValueFromKey(jsonString, "public"), 4.0);
        assertEquals(MapFunctions.getValueFromKey(jsonString, "created_at"), "2018-01-01T11:12:53Z");
        assertEquals(MapFunctions.getValueFromKey(jsonString, "date"), "n");
        assertEquals(
            MapFunctions.getValueFromKey(jsonString, "occasion"),
            new ArrayList<>(Arrays.asList(new Integer[] {80, 85, 90, 95, 100})));
        assertEquals(
            MapFunctions.getValueFromKey(jsonString, "occasion"),
            new ArrayList<>(Arrays.asList(new Object[] {80, 85, 90, 95, 100})));
        assertNull(MapFunctions.getValueFromKey(jsonString, "extra"));
        assertEquals(
            MapFunctions.getValueFromKey(jsonString, "emotion"),
            new ArrayList<>(Arrays.asList(new String[] {"happy", "sad", "angry"})));
        assertEquals(
            MapFunctions.getValueFromKey(jsonString, "emotion"),
            new ArrayList<>(Arrays.asList(new Object[] {"happy", "sad", "angry"})));

        // check mapEntries list
        assertEquals(
            MapFunctions.listMapEntries(jsonString),
            new String[] {
                "id = 704488",
                "type = false",
                "public = 4.0",
                "created_at = 2018-01-01T11:12:53Z",
                "date = n",
                "occasion = [80, 85, 90, 95, 100]",
                "extra = ",
                "emotion = [happy, sad, angry]"
            });

        // check key, value lists and nPairs functions
        assertEquals(
            MapFunctions.listKeys(jsonString),
            new String[] {
                "id", "type", "public", "created_at", "date", "occasion", "extra", "emotion"
            });
        assertEquals(
            MapFunctions.listValues(jsonString),
            new String[] {
                "704488",
                "false",
                "4.0",
                "2018-01-01T11:12:53Z",
                "n",
                "[80, 85, 90, 95, 100]",
                "null",
                "[happy, sad, angry]"
            });
    }

    @Test
    public void testRegMapDiffTypes2() throws JsonProcessingException {
        // test different value types, emphasis on the case of "subjects" value
        String jsonString =
            "{\n"
                + "    \"name\": \"Pete\",\n"
                + "    \"age\": 24,\n"
                + "    \"subjects\": [\n"
                + "        {\n"
                + "            \"name\": \"maths\",\n"
                + "            \"homework_grades\": [80, 85, 90, 95, 100],\n"
                + "            \"grade\": \"A\"\n"
                + "        },\n"
                + "        {\n"
                + "            \"name\": \"english\",\n"
                + "            \"homework_grades\": [60, 65, 70, 85, 90],\n"
                + "            \"grade\": \"B\"\n"
                + "        }\n"
                + "    ]\n"
                + "}";

        // check individual elements correctly recorded
        assertEquals(MapFunctions.getValueFromKey(jsonString, "name"), "Pete");
        assertEquals(MapFunctions.getValueFromKey(jsonString, "age"), 24);
        String correctSubjectContents =
            "[{\"name\":\"maths\", \"homework_grades\":[80, 85, 90, 95, 100], \"grade\":"
                + "\"A\"}, {\"name\":\"english\", \"homework_grades\":[60, 65, 70, 85, 90], \"grade\":\"B\"}]";
        ObjectMapper oM = new ObjectMapper();
        Object[] correctSubjectValArray = oM.readValue(correctSubjectContents, Object[].class);
        System.out.println("New: " + new ArrayList<>(Arrays.asList(correctSubjectValArray)));
        assertEquals(
            MapFunctions.getValueFromKey(jsonString, "subjects"),
            new ArrayList<>(Arrays.asList(correctSubjectValArray)));

        // check mapEntries list
        assertEquals(
            MapFunctions.listMapEntries(jsonString),
            new String[] {
                "name = Pete",
                "age = 24",
                "subjects = [{name=maths, homework_grades=[80, 85, 90, 95, 100], grade=A}, {name=english, "
                    + "homework_grades=[60, 65, 70, 85, 90], grade=B}]"
            });

        // check key, value lists and nPairs functions
        assertEquals(MapFunctions.listKeys(jsonString), new String[] {"name", "age", "subjects"});
        assertEquals(
            MapFunctions.listValues(jsonString),
            new String[] {
                "Pete",
                "24",
                "[{name=maths, homework_grades=[80, 85, 90, 95, 100], grade=A}, "
                    + "{name=english, homework_grades=[60, 65, 70, 85, 90], grade=B}]"
            });
    }

    @Test
    public void testSingle() throws JsonProcessingException {
        // test with different type values (number, bool, array, string, null)
        String jsonString = "{" + "  \"id\": 704488" + "}";

        // check individual elements correctly recorded
        assertEquals(MapFunctions.getValueFromKey(jsonString, "id"), 704488);

        // check mapEntries list
        assertEquals(
            MapFunctions.listMapEntries(jsonString),
            new String[] {
                "id = 704488",
            });

        // check key, value lists and nPairs functions
        assertEquals(MapFunctions.listKeys(jsonString), new String[] {"id"});
        assertEquals(MapFunctions.listValues(jsonString), new String[] {"704488"});
    }

    @Test
    public void testEmpty() throws JsonProcessingException {
        // test with empty JSON object
        String jsonString = "{" + "}";

        assertEquals(MapFunctions.listMapEntries(jsonString), new String[] {});
        assertEquals(MapFunctions.listKeys(jsonString), new String[] {});
        assertEquals(MapFunctions.listValues(jsonString), new String[] {});
        assertEquals(MapFunctions.getValueFromKey(jsonString, "blah"), null);
    }

    @Test
    public void testNull() throws JsonProcessingException {

        // test with empty JSON object
        String jsonString = null;

        assertEquals(MapFunctions.listMapEntries(jsonString), new String[] {});
        assertEquals(MapFunctions.listKeys(jsonString), new String[] {});
        assertEquals(MapFunctions.listValues(jsonString), new String[] {});
        assertEquals(MapFunctions.getValueFromKey(jsonString, "blah"), null);
    }
}
