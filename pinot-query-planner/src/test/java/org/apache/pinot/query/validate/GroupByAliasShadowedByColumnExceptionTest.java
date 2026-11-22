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
package org.apache.pinot.query.validate;

import javax.annotation.Nullable;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;


/// Checks when the validation fails with [GroupByAliasShadowedByColumnException], the exception that makes the
/// controller retry a query with `preferSelectAliasInGroupBy`.
public class GroupByAliasShadowedByColumnExceptionTest extends QueryEnvironmentTestBase {

  @DataProvider
  public Object[][] queries() {
    return new Object[][]{
        {"alias of another column, resolved to the column", "SELECT col2 AS col1, COUNT(*) FROM a GROUP BY col1",
            GroupByAliasShadowedByColumnException.class},
        {"same, with the option set the query is valid",
            "SET preferSelectAliasInGroupBy=true; SELECT col2 AS col1, COUNT(*) FROM a GROUP BY col1", null},
        {"option set, the query fails for another reason",
            "SET preferSelectAliasInGroupBy=true; SELECT col2 AS col1, col3, COUNT(*) FROM a GROUP BY col1",
            QueryException.class},
        {"no alias matches the group by name", "SELECT col2, COUNT(*) FROM a GROUP BY col1", QueryException.class},
        {"the query fails before the group by is validated",
            "SELECT col2 AS col1, COUNT(*) FROM a WHERE unknownColumn = 1 GROUP BY col1", QueryException.class},
        {"shadowed alias in a valid query",
            "SELECT CASE WHEN col1 = 'foo' THEN 'bar' ELSE col1 END AS col1, COUNT(*) FROM a GROUP BY col1", null},
    };
  }

  @Test(dataProvider = "queries")
  public void testValidationException(String description, String sql,
      @Nullable Class<? extends QueryException> expectedException) {
    try (QueryEnvironment.CompiledQuery ignored = _queryEnvironment.compile(sql)) {
      assertNull(expectedException, description + ": the query must fail");
    } catch (QueryException e) {
      if (expectedException == null) {
        fail(description + ": the query must compile", e);
      }
      assertSame(e.getClass(), expectedException, description);
      if (expectedException == GroupByAliasShadowedByColumnException.class) {
        assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_VALIDATION, description);
      }
    }
  }
}
