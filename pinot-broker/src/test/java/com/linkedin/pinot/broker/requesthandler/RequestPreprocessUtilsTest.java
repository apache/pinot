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
package com.linkedin.pinot.broker.requesthandler;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class RequestPreprocessUtilsTest {
  private static final Pql2Compiler COMPILER = new Pql2Compiler();

  @Test(dataProvider = "testAttachValueInUDF")
  public void testAttachValueInUDF(String queryWithoutValueIn, String queryWithValueIn) {
    BrokerRequest actual = COMPILER.compileToBrokerRequest(queryWithoutValueIn);
    RequestPreprocessUtils.attachValueInUDF(actual);
    BrokerRequest expected = COMPILER.compileToBrokerRequest(queryWithValueIn);
    RequestPreprocessUtils.attachValueInUDF(expected);

    Assert.assertEquals(actual.getQueryOptions(), expected.getQueryOptions());
    Assert.assertEquals(actual.getGroupBy().getColumns(), expected.getGroupBy().getColumns());
    Assert.assertEquals(actual.getGroupBy().getExpressions(), expected.getGroupBy().getExpressions());
  }

  @DataProvider(name = "testAttachValueInUDF")
  public Object[][] testAttachValueInUDF() {
    return new Object[][]{
        {
            "SELECT COUNT(*) FROM table WHERE column IN (1, 2, 3) GROUP BY column OPTION(attachValueInForColumns='column')",
            "SELECT COUNT(*) FROM table WHERE column IN (1, 2, 3) GROUP BY ValueIn(column, 1, 2, 3)"
        },
        {
            "SELECT COUNT(*) FROM table WHERE column IN (1, 2, 3) GROUP BY column OPTION(attachValueInForColumns='column')",
            "SELECT COUNT(*) FROM table WHERE column IN (1, 2, 3) GROUP BY ValueIn(column, 1, 2, 3) OPTION(attachValueInForColumns='column')"
        },
        {
            "SELECT COUNT(*) FROM table WHERE column IN (1, 2, 3) GROUP BY column OPTION(attachValueInForColumns='column', foo='bar')",
            "SELECT COUNT(*) FROM table WHERE column IN (1, 2, 3) GROUP BY ValueIn(column, 1, 2, 3) OPTION(foo='bar')"
        },
        {
            "SELECT COUNT(*) FROM table WHERE column IN ('a', 'b', 'c') GROUP BY column OPTION(attachValueInForColumns='column')",
            "SELECT COUNT(*) FROM table WHERE column IN ('a', 'b', 'c') GROUP BY ValueIn(column, 'a', 'b', 'c')"
        },
        {
            "SELECT COUNT(*) FROM table WHERE column1 IN (1, 2, 3) AND column2 IN ('a', 'b', 'c') GROUP BY column1, column2 OPTION(attachValueInForColumns='column1, column2')",
            "SELECT COUNT(*) FROM table WHERE column1 IN (1, 2, 3) AND column2 IN ('a', 'b', 'c') GROUP BY ValueIn(column1, 1, 2, 3), ValueIn(column2, 'a', 'b', 'c')"
        },
        {
            "SELECT COUNT(*) FROM table WHERE column1 IN (1, 2, 3) AND column2 IN ('a', 'b', 'c') GROUP BY column1, column2 OPTION(attachValueInForColumns='column2, column1')",
            "SELECT COUNT(*) FROM table WHERE column1 IN (1, 2, 3) AND column2 IN ('a', 'b', 'c') GROUP BY ValueIn(column1, 1, 2, 3), ValueIn(column2, 'a', 'b', 'c')"
        },
        {
            "SELECT COUNT(*) FROM table WHERE column1 IN (1, 2, 3) AND column2 IN ('a', 'b', 'c') GROUP BY column2, column1 OPTION(attachValueInForColumns='column1, column2')",
            "SELECT COUNT(*) FROM table WHERE column1 IN (1, 2, 3) AND column2 IN ('a', 'b', 'c') GROUP BY ValueIn(column2, 'a', 'b', 'c'), ValueIn(column1, 1, 2, 3)"
        },
        {
            "SELECT COUNT(*) FROM table WHERE column1 IN (1, 2, 3) AND column2 IN ('a', 'b', 'c') GROUP BY column1, column2 OPTION(attachValueInForColumns='column2')",
            "SELECT COUNT(*) FROM table WHERE column1 IN (1, 2, 3) AND column2 IN ('a', 'b', 'c') GROUP BY column1, ValueIn(column2, 'a', 'b', 'c')"
        }
    };
  }

  @Test(dataProvider = "testAttachValueInUDFException", expectedExceptions = RuntimeException.class)
  public void testAttachValueInUDFException(String illegalQuery) {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(illegalQuery);
    RequestPreprocessUtils.attachValueInUDF(brokerRequest);
  }

  @DataProvider(name = "testAttachValueInUDFException")
  public Object[][] testAttachValueInUDFException() {
    return new Object[][] {
        {
            "SELECT COUNT(*) FROM table GROUP BY column OPTION(attachValueInForColumns='column')"
        },
        {
            "SELECT COUNT(*) FROM table WHERE column IN (1, 2) OPTION(attachValueInForColumns='column')"
        },
        {
            "SELECT COUNT(*) FROM table WHERE column NOT IN (1, 2) GROUP BY column OPTION(attachValueInForColumns='column')"
        },
        {
            "SELECT COUNT(*) FROM table WHERE column IN (1, 2) OR column IN (2, 3) GROUP BY column OPTION(attachValueInForColumns='column')"
        }
    };
  }
}