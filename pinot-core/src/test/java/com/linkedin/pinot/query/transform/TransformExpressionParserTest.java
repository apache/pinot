/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.query.transform;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for TransformExpression parsing
 */
public class TransformExpressionParserTest {

  /**
   * This test ensures that both column names and expressions can be parsed and populated into
   * broker request correctly.
   */
  @Test
  public void testGroupBy() {

    String[] expectedGroupByColumns = new String[]{"col1", "col2", "col3"};
    String[] expectedGroupByExpressions = new String[]{"udf1(col2)", "udf2(udf3(col1, col2), col3)"};

    String query =
        "select col1 from myTable group by " + StringUtil.join(",", expectedGroupByColumns) + "," + StringUtil.join(",",
            expectedGroupByExpressions);

    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(query);

    List<String> actualGroupByColumns = brokerRequest.getGroupBy().getColumns();
    for (int i = 0; i < actualGroupByColumns.size(); i++) {
      Assert.assertEquals(actualGroupByColumns.get(i), expectedGroupByColumns[i]);
    }

    // Group by expression contains columns as well as expressions.
    List<String> actualGroupByExpressions = brokerRequest.getGroupBy().getExpressions();
    for (int i = 0; i < expectedGroupByColumns.length; i++) {
      Assert.assertEquals(actualGroupByExpressions.get(i), expectedGroupByColumns[i]);
    }

    for (int i = 0; i < expectedGroupByExpressions.length; i++) {
      int expressionIndex = i + expectedGroupByColumns.length;
      Assert.assertEquals(actualGroupByExpressions.get(expressionIndex),
          expectedGroupByExpressions[i]);
    }
  }
}
