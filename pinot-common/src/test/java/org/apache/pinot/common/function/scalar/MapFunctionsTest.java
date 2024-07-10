package org.apache.pinot.common.function.scalar;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class MapFunctionsTest {

    @Test
    public void testRegMapSameTypes()
            throws JsonProcessingException {

        // CHECKSTYLE:OFF
        // @formatter:off

        //test with all same type keys and values (Strings)
        String jsonString = "{" +
                "  \"id\": \"7044885078\"," +
                "  \"type\": \"CreateEvent\"," +
                "  \"public\": \"true\"," +
                "  \"created_at\": \"2018-01-01T11:12:53Z\"," +
                "    \"date\": \"November\"," +
                "    \"occasion\": \"Anniversary\"," +
                "    \"emotion\": \"Happiness\"" +
                "}";

        // @formatter:on
        // CHECKSTYLE:ON

        assertEquals(MapFunctions.listKeys(jsonString), new String[]{"id", "type", "public", "created_at",
                "date", "occasion", "emotion"});
    }

    @Test
    public void testRegMapDiffTypes1()
            throws JsonProcessingException {

        // CHECKSTYLE:OFF
        // @formatter:off

        //test with different type values (number, bool, array, string, null)
        String jsonString = "{" +
                "  \"id\": 704488," +
                "  \"type\": false," +
                "  \"public\": 4.0," +
                "  \"created_at\": \"2018-01-01T11:12:53Z\"," +
                "    \"date\": \"n\"," +
                "    \"occasion\":  [80, 85, 90, 95, 100]," +
                "    \"extra\": null," +
                "    \"emotion\": [\"happy\",\"sad\",\"angry\"]" +
                "}";

        // @formatter:on
        // CHECKSTYLE:ON

        assertEquals(MapFunctions.listKeys(jsonString), new String[]{"id", "type", "public", "created_at", "date",
                "occasion", "extra", "emotion"});
    }

    @Test
    public void testRegMapDiffTypes2()
            throws JsonProcessingException {

        // CHECKSTYLE:OFF
        // @formatter:off

        //test different value types, emphasis on the case of "subjects" value
        String jsonString = "{\n" +
                "    \"name\": \"Pete\",\n" +
                "    \"age\": 24,\n" +
                "    \"subjects\": [\n" +
                "        {\n" +
                "            \"name\": \"maths\",\n" +
                "            \"homework_grades\": [80, 85, 90, 95, 100],\n" +
                "            \"grade\": \"A\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"english\",\n" +
                "            \"homework_grades\": [60, 65, 70, 85, 90],\n" +
                "            \"grade\": \"B\"\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        // @formatter:on
        // CHECKSTYLE:ON

        assertEquals(MapFunctions.listKeys(jsonString), new String[]{"name", "age", "subjects"});
    }


    @Test
    public void testEmpty()
            throws JsonProcessingException {

        // CHECKSTYLE:OFF
        // @formatter:off

        //test with empty JSON object
        String jsonString = "{" + "}";

        // @formatter:on
        // CHECKSTYLE:ON

        assertEquals(MapFunctions.listKeys(jsonString), new String[]{});
    }

    @Test
    public void testNull()
            throws JsonProcessingException {

        // CHECKSTYLE:OFF
        // @formatter:off

        //test with empty JSON object
        String jsonString = null;

        // @formatter:on
        // CHECKSTYLE:ON

        assertEquals(MapFunctions.listKeys(jsonString), new String[]{});
    }
}
