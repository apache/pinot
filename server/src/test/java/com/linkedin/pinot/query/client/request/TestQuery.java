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

  public static String _queryString;
  public static JSONObject _queryJsonObject;

  @BeforeClass
  public static void setup() {
    _queryString =
        "{" + "  "
            + "    \"queryType\": Complex,\n"
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
            + "        \"selectionColumn\": \"dim_memberGender,dim_memberIndustry\",\n"
            + "        \"orderByColumn\": \"dim_memberGender\",\n"
            + "        \"startDocId\": 0,\n"
            + "        \"endDocId\": 10,\n"
            + "    },"
            + "    \"groupBy\": {"
            + "        \"groupByColumns\": \"dim_memberGender,dim_memberIndustry\",\n"
            + "        \"groupByOrder\": \"top\",\n"
            + "        \"limit\": 10,\n"
            + "    },"
            + "    \"aggregations\": ["
            + "        {"
            + "            \"function\": sum,\n"
            + "            \"params\": {"
            + "                \"column\": met_impressionCount,\n"
            + "            },"
            + "        },"
            + "        {"
            + "            \"function\": count,\n"
            + "            \"params\": {"
            + "            },"
            + "        },"
            + "    ]"
            + "}";

    _queryJsonObject = new JSONObject(_queryString);
  }

  @Test
  public void testQueryFromJson() {

    Query query = Query.fromJson(_queryJsonObject);
    List<AggregationFunction> aggregationFunctions = new ArrayList<AggregationFunction>();
    for (int i = 0; i < query.getAggregationJSONArray().length(); ++i) {
      aggregationFunctions.add(AggregationFunctionFactory.get(query.getAggregationJSONArray().getJSONObject(i)));
    }

    // Assertion on query type
    Assert.assertEquals("Complex", query.getQueryType().toString());

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
    Assert.assertEquals(0, query.getSelections().getStartDocId());
    Assert.assertEquals(10, query.getSelections().getEndDocId());
    Assert.assertEquals(2, query.getSelections().getSelectionColumns().length);
    Assert.assertEquals("dim_memberGender", query.getSelections().getSelectionColumns()[0]);
    Assert.assertEquals("dim_memberIndustry", query.getSelections().getSelectionColumns()[1]);
    Assert.assertEquals(1, query.getSelections().getOrderBySequence().length);
    Assert.assertEquals("dim_memberGender", query.getSelections().getOrderBySequence()[0]);

    // Assertion on Time Granularity
    Assert.assertEquals(86400000, query.getTimeGranularity().getMillis());

    // Assertion on Time Interval
    Assert.assertEquals(1403247600000L, query.getTimeInterval().getStartMillis());
    Assert.assertEquals(1403247600000L + 10 * 86400000, query.getTimeInterval().getEndMillis());

    // Assertion on groupby
    Assert.assertEquals(2, query.getGroupBy().getColumns().length);
    Assert.assertEquals("dim_memberGender", query.getGroupBy().getColumns()[0]);
    Assert.assertEquals("dim_memberIndustry", query.getGroupBy().getColumns()[1]);
    Assert.assertEquals("top", query.getGroupBy().getOrderBy().toString());
    Assert.assertEquals(10, query.getGroupBy().getLimit());
  }
}
