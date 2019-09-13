package org.apache.pinot.queries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InterSegmentSelectionSingleValueQueriesTest extends BaseSingleValueQueriesTest {
  @Test
  public void testSelectStar() {
    String query = "SELECT * FROM testTable";

    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    Map<String, String> expectedColumnTypes = new HashMap<>();
    expectedColumnTypes.put("column1", "INT");
    expectedColumnTypes.put("column3", "INT");
    expectedColumnTypes.put("column5", "STRING");
    expectedColumnTypes.put("column6", "INT");
    expectedColumnTypes.put("column7", "INT");
    expectedColumnTypes.put("column9", "INT");
    expectedColumnTypes.put("column11", "STRING");
    expectedColumnTypes.put("column12", "STRING");
    expectedColumnTypes.put("column17", "INT");
    expectedColumnTypes.put("column18", "INT");
    expectedColumnTypes.put("daysSinceEpoch", "INT");

    List<String> expectedColumns  = new ArrayList<>();
    expectedColumns.add("column1");
    expectedColumns.add("column3");
    expectedColumns.add("column5");
    expectedColumns.add("column6");
    expectedColumns.add("column7");
    expectedColumns.add("column9");
    expectedColumns.add("column11");
    expectedColumns.add("column12");
    expectedColumns.add("column17");
    expectedColumns.add("column18");
    expectedColumns.add("daysSinceEpoch");
    verifyBrokerResponse(brokerResponse, 40L, 120000L, expectedColumns, expectedColumnTypes);
  }

  @Test
  public void testSelectColumns() {
    String query = "SELECT column1, column5 FROM testTable";
    BrokerResponseNative brokerResponse = getBrokerResponseForQuery(query);
    Map<String, String> expectedColumnTypes = new HashMap<>();
    expectedColumnTypes.put("column1", "INT");
    expectedColumnTypes.put("column5", "STRING");
    List<String> expectedColumns  = new ArrayList<>();
    expectedColumns.add("column1");
    expectedColumns.add("column5");
    verifyBrokerResponse(brokerResponse, 40L, 120000L, expectedColumns, expectedColumnTypes);
  }

  private void verifyBrokerResponse(BrokerResponseNative brokerResponse, long expectedNumDocsScanned, long expectedTotalDocNum, List<String> expectedColumns,
      Map<String, String> expectedColumnTypes) {
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), expectedNumDocsScanned);
    Assert.assertEquals(brokerResponse.getTotalDocs(), expectedTotalDocNum);
    Assert.assertEquals(brokerResponse.getSelectionResults().getColumns().size(), expectedColumns.size());
    for(String column : expectedColumns) {
      Assert.assertTrue(brokerResponse.getSelectionResults().getColumns().contains(column));
    }
    Assert.assertEquals(brokerResponse.getSelectionResults().getColumnTypes(), expectedColumnTypes);
  }
}
