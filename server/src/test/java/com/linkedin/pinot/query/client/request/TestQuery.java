package com.linkedin.pinot.query.client.request;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.linkedin.pinot.query.aggregation.AggregationFunction;
import com.linkedin.pinot.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.query.request.Query;


public class TestQuery {

  public static String _queryString1;
  public static String _queryString2;
  public static JSONObject _queryJsonObject1;
  public static JSONObject _queryJsonObject2;

  @BeforeClass
  public static void setup() {

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
    _queryJsonObject1 = new JSONObject(_queryString1);
    _queryJsonObject2 = new JSONObject(_queryString2);

  }

  @Test
  public void testQueryFromJson1() {

    Query query = Query.fromJson(_queryJsonObject1);
    List<AggregationFunction> aggregationFunctions = new ArrayList<AggregationFunction>();
    for (int i = 0; i < query.getAggregationsInfo().size(); ++i) {
      aggregationFunctions.add(AggregationFunctionFactory.get(query.getAggregationsInfo().get(i)));
    }

    // Assertion on query type
    Assert.assertTrue(query.getQueryType().hasAggregation());
    Assert.assertTrue(query.getQueryType().hasFilter());
    Assert.assertTrue(query.getQueryType().hasGroupBy());
    Assert.assertTrue(query.getQueryType().hasSelection());

    // Assertion on source, resource, table
    Assert.assertEquals("midas", query.getResourceName());
    Assert.assertEquals("jymbii", query.getTableName());
    Assert.assertEquals("midas.jymbii", query.getSourceName());

    // Assertion on filters
    Assert.assertEquals("AND", query.getFilterQuery().getOperator().toString());
    Assert.assertEquals(2, query.getFilterQuery().getNestedFilterConditions().size());
    Assert.assertEquals("dim_memberGender", query.getFilterQuery().getNestedFilterConditions().get(0).getColumn()
        .toString());
    Assert.assertEquals("EQUALITY", query.getFilterQuery().getNestedFilterConditions().get(0).getOperator().toString());
    Assert.assertEquals(1, query.getFilterQuery().getNestedFilterConditions().get(0).getValue().size());
    Assert.assertEquals("m", query.getFilterQuery().getNestedFilterConditions().get(0).getValue().get(0));
    Assert.assertEquals("dim_memberIndustry", query.getFilterQuery().getNestedFilterConditions().get(1).getColumn()
        .toString());
    Assert.assertEquals("EQUALITY", query.getFilterQuery().getNestedFilterConditions().get(1).getOperator().toString());
    Assert.assertEquals(1, query.getFilterQuery().getNestedFilterConditions().get(1).getValue().size());
    Assert.assertEquals("23", query.getFilterQuery().getNestedFilterConditions().get(1).getValue().get(0));

    // Assertion on selections
    Assert.assertEquals(0, query.getSelections().getOffset());
    Assert.assertEquals(10, query.getSelections().getSize());
    Assert.assertEquals(2, query.getSelections().getSelectionColumns().length);
    Assert.assertEquals("dim_memberGender", query.getSelections().getSelectionColumns()[0]);
    Assert.assertEquals("dim_memberIndustry", query.getSelections().getSelectionColumns()[1]);
    Assert.assertEquals(1, query.getSelections().getSelectionSortSequence().length);
    Assert.assertEquals("dim_memberGender", query.getSelections().getSelectionSortSequence()[0].getColumn());
    Assert.assertTrue(query.getSelections().getSelectionSortSequence()[0].isAsc());

    // Assertion on Time Granularity
    Assert.assertEquals(86400000, query.getTimeGranularity().getMillis());

    // Assertion on Time Interval
    Assert.assertEquals(1403247600000L, query.getTimeInterval().getStartMillis());
    Assert.assertEquals(1403247600000L + (10 * 86400000), query.getTimeInterval().getEndMillis());

    // Assertion on groupby
    Assert.assertEquals(2, query.getGroupBy().getColumns().size());
    Assert.assertEquals("dim_memberGender", query.getGroupBy().getColumns().get(0));
    Assert.assertEquals("dim_memberIndustry", query.getGroupBy().getColumns().get(1));
    Assert.assertEquals(10, query.getGroupBy().getTop());
  }
  
  @Test
  public void testQueryFromJson2() {

    Query query = Query.fromJson(_queryJsonObject2);
    List<AggregationFunction> aggregationFunctions = new ArrayList<AggregationFunction>();
    for (int i = 0; i < query.getAggregationsInfo().size(); ++i) {
      aggregationFunctions.add(AggregationFunctionFactory.get(query.getAggregationsInfo().get(i)));
    }

    // Assertion on query type
    Assert.assertTrue(query.getQueryType().hasAggregation());
    Assert.assertFalse(query.getQueryType().hasFilter());
    Assert.assertFalse(query.getQueryType().hasGroupBy());
    Assert.assertFalse(query.getQueryType().hasSelection());
  }
}
