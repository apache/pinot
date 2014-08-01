package com.linkedin.pinot.query.client.request;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.testng.AssertJUnit;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;
import com.linkedin.pinot.common.query.request.Query;


public class TestQuery {

  public static String _queryString1;
  public static String _queryString2;
  public static String _queryString3;
  public static JSONObject _queryJsonObject1;
  public static JSONObject _queryJsonObject2;
  public static JSONObject _queryJsonObject3;

  @BeforeClass
  public static void setup() throws JSONException {

    _queryString2 =
        "{" + "  "
            + "    \"source\": midas.jymbii,\n"
            + "    \"aggregations\": ["
            + "        {"
            + "            \"aggregationType\": sum,\n"
            + "            \"params\": {"
            + "                \"column\": met_impressionCount,\n"
            + "            },"
            + "        },"
            + "    ]"
            + "}";

    _queryString1 =
        "{" + "  "
            + "    \"source\": midas.jymbii,\n"
            + "    \"timeInterval\": {"
            + "        \"startTime\": 2014-06-20,\n"
            + "        \"endTime\": 2014-06-30,\n"
            + "    },"
            + "    \"timeGranularity\": \"1D\",\n"
            + "    \"filters\": {"
            + "        \"values\": [],"
            + "        \"column\": null,"
            + "        \"operator\": \"and\","
            + "        \"nestedFilter\": ["
            + "            {"
            + "                \"values\": [\"m\"],"
            + "                \"column\": dim_memberGender,"
            + "                \"operator\": \"equality\","
            + "                \"nestedFilter\": [],"
            + "            },"
            + "            {"
            + "                \"values\": [\"23\"],"
            + "                \"column\": dim_memberIndustry,"
            + "                \"operator\": \"equality\","
            + "                \"nestedFilter\": [],"
            + "            },"
            + "        ]"
            + "    },"
            + "    \"selections\": {"
            + "        \"columns\": \"dim_memberGender,dim_memberIndustry\",\n"
            + "        \"sorts\": \"dim_memberGender\",\n"
            + "        \"offset\": 0,\n"
            + "        \"size\": 10,\n"
            + "    },"
            + "    \"groupBy\": {"
            + "        \"columns\": \"dim_memberGender,dim_memberIndustry\",\n"
            + "        \"top\": \"10\",\n"
            + "    },"
            + "    \"aggregations\": ["
            + "        {"
            + "            \"aggregationType\": sum,\n"
            + "            \"params\": {"
            + "                \"column\": met_impressionCount,\n"
            + "            },"
            + "        },"
            + "        {"
            + "            \"aggregationType\": count,\n"
            + "            \"params\": {"
            + "            },"
            + "        },"
            + "    ]"
            + "}";

    //String req = "{\"bql\":\"select sum(met_impressionCount) where dim_memberIndustry in (102, 100)\"}";
    _queryString3 =
        "{" + "  "
            + "    \"source\": midas.jymbii,\n"
            + "    \"filters\": {"
            + "        \"values\": [],"
            + "        \"column\": null,"
            + "        \"operator\": \"or\","
            + "        \"nestedFilter\": ["
            + "            {"
            + "                \"values\": [\"100\"],"
            + "                \"column\": dim_memberIndustry,"
            + "                \"operator\": \"equality\","
            + "                \"nestedFilter\": [],"
            + "            },"
            + "            {"
            + "                \"values\": [\"102\"],"
            + "                \"column\": dim_memberIndustry,"
            + "                \"operator\": \"equality\","
            + "                \"nestedFilter\": [],"
            + "            },"
            + "        ]"
            + "    },"
            + "    \"aggregations\": ["
            + "        {"
            + "            \"aggregationType\": sum,\n"
            + "            \"params\": {"
            + "                \"column\": met_impressionCount,\n"
            + "            },"
            + "        },"
            + "        {"
            + "            \"aggregationType\": count,\n"
            + "            \"params\": {"
            + "            },"
            + "        },"
            + "    ]"
            + "}";

    _queryString3 =
        "{" + "  "
            + "    \"source\": midas.jymbii,\n"
            + "    \"filters\": {"
            + "        \"values\": [\"102,100\"],"
            + "        \"column\": dim_memberIndustry,"
            + "        \"operator\": \"equality\","
            + "        \"nestedFilter\": [],"
            + "    },"
            + "    \"aggregations\": ["
            + "        {"
            + "            \"aggregationType\": sum,\n"
            + "            \"params\": {"
            + "                \"column\": met_impressionCount,\n"
            + "            },"
            + "        },"
            + "        {"
            + "            \"aggregationType\": count,\n"
            + "            \"params\": {"
            + "            },"
            + "        },"
            + "    ]"
            + "}";

    _queryJsonObject1 = new JSONObject(_queryString1);
    _queryJsonObject2 = new JSONObject(_queryString2);
    _queryJsonObject3 = new JSONObject(_queryString3);

  }

  @Test
  public void testQueryFromJson1() throws JSONException {

    Query query = Query.fromJson(_queryJsonObject1);

    // Assertion on query type
    AssertJUnit.assertTrue(query.getQueryType().hasAggregation());
    AssertJUnit.assertTrue(query.getQueryType().hasFilter());
    AssertJUnit.assertTrue(query.getQueryType().hasGroupBy());
    AssertJUnit.assertTrue(query.getQueryType().hasSelection());

    // Assertion on source, resource, table
    AssertJUnit.assertEquals("midas", query.getResourceName());
    AssertJUnit.assertEquals("jymbii", query.getTableName());
    AssertJUnit.assertEquals("midas.jymbii", query.getSourceName());

    // Assertion on filters
    AssertJUnit.assertEquals("AND", query.getFilterQuery().getOperator().toString());
    AssertJUnit.assertEquals(2, query.getFilterQuery().getNestedFilterConditions().size());
    AssertJUnit.assertEquals("dim_memberGender", query.getFilterQuery().getNestedFilterConditions().get(0).getColumn()
        .toString());
    AssertJUnit.assertEquals("EQUALITY", query.getFilterQuery().getNestedFilterConditions().get(0).getOperator().toString());
    AssertJUnit.assertEquals(1, query.getFilterQuery().getNestedFilterConditions().get(0).getValue().size());
    AssertJUnit.assertEquals("m", query.getFilterQuery().getNestedFilterConditions().get(0).getValue().get(0));
    AssertJUnit.assertEquals("dim_memberIndustry", query.getFilterQuery().getNestedFilterConditions().get(1).getColumn()
        .toString());
    AssertJUnit.assertEquals("EQUALITY", query.getFilterQuery().getNestedFilterConditions().get(1).getOperator().toString());
    AssertJUnit.assertEquals(1, query.getFilterQuery().getNestedFilterConditions().get(1).getValue().size());
    AssertJUnit.assertEquals("23", query.getFilterQuery().getNestedFilterConditions().get(1).getValue().get(0));

    // Assertion on selections
    AssertJUnit.assertEquals(0, query.getSelections().getOffset());
    AssertJUnit.assertEquals(10, query.getSelections().getSize());
    AssertJUnit.assertEquals(2, query.getSelections().getSelectionColumns().length);
    AssertJUnit.assertEquals("dim_memberGender", query.getSelections().getSelectionColumns()[0]);
    AssertJUnit.assertEquals("dim_memberIndustry", query.getSelections().getSelectionColumns()[1]);
    AssertJUnit.assertEquals(1, query.getSelections().getSelectionSortSequence().length);
    AssertJUnit.assertEquals("dim_memberGender", query.getSelections().getSelectionSortSequence()[0].getColumn());
    AssertJUnit.assertTrue(query.getSelections().getSelectionSortSequence()[0].isAsc());

    // Assertion on Time Granularity
    AssertJUnit.assertEquals(86400000, query.getTimeGranularity().getMillis());

    // Assertion on Time Interval
    AssertJUnit.assertEquals(1403247600000L, query.getTimeInterval().getStartMillis());
    AssertJUnit.assertEquals(1403247600000L + (10 * 86400000), query.getTimeInterval().getEndMillis());

    // Assertion on groupby
    AssertJUnit.assertEquals(2, query.getGroupBy().getColumns().size());
    AssertJUnit.assertEquals("dim_memberGender", query.getGroupBy().getColumns().get(0));
    AssertJUnit.assertEquals("dim_memberIndustry", query.getGroupBy().getColumns().get(1));
    AssertJUnit.assertEquals(10, query.getGroupBy().getTop());
  }

  @Test
  public void testQueryFromJson2() throws JSONException {

    Query query = Query.fromJson(_queryJsonObject2);

    // Assertion on query type
    AssertJUnit.assertTrue(query.getQueryType().hasAggregation());
    AssertJUnit.assertFalse(query.getQueryType().hasFilter());
    AssertJUnit.assertFalse(query.getQueryType().hasGroupBy());
    AssertJUnit.assertFalse(query.getQueryType().hasSelection());
  }

  @Test
  public void testQueryFromJson3() throws JSONException {

    Query query = Query.fromJson(_queryJsonObject3);

    // Assertion on query type
    AssertJUnit.assertTrue(query.getQueryType().hasAggregation());
    AssertJUnit.assertTrue(query.getQueryType().hasFilter());
    AssertJUnit.assertFalse(query.getQueryType().hasGroupBy());
    AssertJUnit.assertFalse(query.getQueryType().hasSelection());
  }
}
