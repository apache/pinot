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

import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class PinotTypeCoercionImplTest extends QueryEnvironmentTestBase {

  @DataProvider(name = "implicitCastCharToIntInWhere")
  protected Object[][] provideQueriesWithWhere() {
    return new Object[][]{
        // VARCHAR BINARY_COMPARISON INTEGER
        new Object[]{"SELECT * FROM a where col1 > 5"},
        new Object[]{"SELECT * FROM a where col1 >= 5"},
        new Object[]{"SELECT * FROM a where col1 < 5"},
        new Object[]{"SELECT * FROM a where col1 <= 5"},
        new Object[]{"SELECT * FROM a where col1 = 5"},
        new Object[]{"SELECT * FROM a where col1 != 5"},


        // INTEGER BINARY_COMPARISON VARCHAR
        new Object[]{"SELECT * FROM a where 5 > col1"},
        new Object[]{"SELECT * FROM a where 5 >= col1"},
        new Object[]{"SELECT * FROM a where 5 < col1"},
        new Object[]{"SELECT * FROM a where 5 <= col1"},
        new Object[]{"SELECT * FROM a where 5 = col1"},
        new Object[]{"SELECT * FROM a where 5 != col1"},

        // (VARCHAR ARITHMETIC_OP INTEGER)
        new Object[]{"SELECT * FROM a where col1 + 1 > 5"},
        new Object[]{"SELECT * FROM a where col1 - 1 > 5"},
        new Object[]{"SELECT * FROM a where col1 * 1 > 5"},
        new Object[]{"SELECT * FROM a where col1 / 1 > 5"},

        // (INTEGER ARITHMETIC_OP VARCHAR)
        new Object[]{"SELECT * FROM a where 5 > col1 + 1"},
        new Object[]{"SELECT * FROM a where 5 > col1 - 1"},
        new Object[]{"SELECT * FROM a where 5 > col1 * 1"},
        new Object[]{"SELECT * FROM a where 5 > col1 / 1"},

        // Constants
        new Object[]{"SELECT * FROM a where '10' > 5"},
        new Object[]{"SELECT * FROM a where '10' >= 5"},
        new Object[]{"SELECT * FROM a where '10' < 5"},
        new Object[]{"SELECT * FROM a where '10' <= 5"},
        new Object[]{"SELECT * FROM a where '10' = 5"},
        new Object[]{"SELECT * FROM a where '10' != 5"},

        // BETWEEN
        new Object[]{"SELECT * FROM a where col1 BETWEEN 5 AND 8"},
        new Object[]{"SELECT * FROM a where 5 BETWEEN " + "col1 AND 8"},
        new Object[]{"SELECT * FROM a where 5 BETWEEN 8 AND col1"},
    };
  }

  @Test(dataProvider = "implicitCastCharToIntInWhere", expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Cannot apply .+ to arguments of type .*")
  public void testImplicitCastCharToIntInWhere(String query) {
    _queryEnvironment.planQuery(query);
  }

  @DataProvider(name = "implicitCastCharToIntInProject")
  protected Object[][] provideQueriesWithProject() {
    return new Object[][]{
        // VARCHAR BINARY_COMPARISON INTEGER
        new Object[]{"SELECT col1 > 5 FROM a"},
        new Object[]{"SELECT col1 >= 5 FROM a"},
        new Object[]{"SELECT col1 < 5 FROM a"},
        new Object[]{"SELECT col1 <= 5 FROM a"},
        new Object[]{"SELECT col1 < 5 FROM a"},
        new Object[]{"SELECT col1 != 5 FROM a"},

        // INTEGER BINARY_COMPARISON VARCHAR
        new Object[]{"SELECT 5 > col1 FROM a"},
        new Object[]{"SELECT 5 >= col1 FROM a"},
        new Object[]{"SELECT 5 < col1 FROM a"},
        new Object[]{"SELECT 5 <= col1 FROM a"},
        new Object[]{"SELECT 5 = col1 FROM a"},
        new Object[]{"SELECT 5 != col1 FROM a"},

        // (VARCHAR ARITHMETIC_OP INTEGER)
        new Object[]{"SELECT col1 + 1 > 5 FROM a"},
        new Object[]{"SELECT col1 - 1 > 5 FROM a"},
        new Object[]{"SELECT col1 * 1 > 5 FROM a"},
        new Object[]{"SELECT col1 / 1 > 5 FROM a"},

        // (INTEGER ARITHMETIC_OP VARCHAR)
        new Object[]{"SELECT 5 > col1 + 1 FROM a"},
        new Object[]{"SELECT 5 > col1 - 1 FROM a"},
        new Object[]{"SELECT 5 > col1 * 1 FROM a"},
        new Object[]{"SELECT 5 > col1 / 1 FROM a"},
    };
  }

  @Test(dataProvider = "implicitCastCharToIntInProject", expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Cannot apply .+ to arguments of type .*")
  public void testImplicitCastCharToIntInProject(String query) {
    _queryEnvironment.planQuery(query);
  }

  @DataProvider(name = "explicitCastCharToIntInWhere")
  protected Object[][] provideQueriesWithExplicitInWhere() {
    return new Object[][]{
        // VARCHAR BINARY_COMPARISON INTEGER
        new Object[]{"SELECT * FROM a where CAST(col1 as INT) > 5"},
        new Object[]{"SELECT * FROM a where CAST(col1 as INT) >= 5"},
        new Object[]{"SELECT * FROM a where CAST(col1 as INT) < 5"},
        new Object[]{"SELECT * FROM a where CAST(col1 as INT) <= 5"},
        new Object[]{"SELECT * FROM a where CAST(col1 as INT) = 5"},
        new Object[]{"SELECT * FROM a where CAST(col1 as INT) != 5"},

        // INTEGER BINARY_COMPARISON VARCHAR
        new Object[]{"SELECT * FROM a where 5 > CAST(col1 as INT)"},
        new Object[]{"SELECT * FROM a where 5 >= CAST(col1 as INT)"},
        new Object[]{"SELECT * FROM a where 5 < CAST(col1 as INT)"},
        new Object[]{"SELECT * FROM a where 5 <= CAST(col1 as INT)"},
        new Object[]{"SELECT * FROM a where 5 = CAST(col1 as INT)"},

        // (VARCHAR ARITHMETIC_OP INTEGER)
        new Object[]{"SELECT * FROM a where CAST(col1 as INT) + 1 > 5"},
        new Object[]{"SELECT * FROM a where CAST(col1 as INT) - 1 > 5"},
        new Object[]{"SELECT * FROM a where CAST(col1 as INT) * 1 > 5"},
        new Object[]{"SELECT * FROM a where CAST(col1 as INT) / 1 > 5"},

        // (INTEGER ARITHMETIC_OP VARCHAR)
        new Object[]{"SELECT * FROM a where 5 > CAST(col1 as INT) + 1"},
        new Object[]{"SELECT * FROM a where 5 > CAST(col1 as INT) - 1"},
        new Object[]{"SELECT * FROM a where 5 > CAST(col1 as INT) * 1"},
        new Object[]{"SELECT* FROM a where 5 > CAST(col1 as INT) / 1"},

        // Constants
        new Object[]{"SELECT * FROM a where CAST('10' as INT) > 5"},
        new Object[]{"SELECT * FROM a where CAST('10' as INT) >= 5"},
        new Object[]{"SELECT * FROM a where CAST('10' as INT) < 5"},
        new Object[]{"SELECT * FROM a where CAST('10' as INT) <= 5"},
        new Object[]{"SELECT * FROM a where CAST('10' as INT) = 5"},
        new Object[]{"SELECT * FROM a where CAST('10' as INT) != 5"},

        // BETWEEN
        new Object[]{"SELECT * FROM a where CAST(col1 as INT) BETWEEN 5 AND 8"},
        new Object[]{"SELECT * FROM a where 5 BETWEEN CAST(col1 as INT) AND 8"},
        new Object[]{"SELECT * FROM a where 5 BETWEEN 8 AND CAST(col1 as INT)"},

        // ALLOW CAST of INT -> VARCHAR
        new Object[]{"SELECT * FROM a where col1 > CAST(5 AS VARCHAR)"},
        new Object[]{"SELECT * FROM a where col1 BETWEEN CAST(5 AS VARCHAR) AND CAST(8 AS VARCHAR)"},

    };
  }

  @Test(dataProvider = "explicitCastCharToIntInWhere")
  public void testExplicitCastCharToIntInWhere(String query) {
    Assert.assertNotNull(_queryEnvironment.planQuery(query));
  }

  @DataProvider(name = "explicitCastCharToIntInProject")
  protected Object[][] provideQueriesWithExplicitInProject() {
    return new Object[][]{
        // VARCHAR BINARY_COMPARISON INTEGER
        new Object[]{"SELECT CAST(col1 as INT) > 5 FROM a"},
        new Object[]{"SELECT CAST(col1 as INT) >= 5 FROM a"},
        new Object[]{"SELECT CAST(col1 as INT) < 5 FROM a"},
        new Object[]{"SELECT CAST(col1 as INT) <= 5 FROM a"},
        new Object[]{"SELECT CAST(col1 as INT) = 5 FROM a"},
        new Object[]{"SELECT CAST(col1 as INT) != 5 FROM a"},

        // INTEGER BINARY_COMPARISON VARCHAR
        new Object[]{"SELECT 5 > CAST(col1 as INT) FROM a"},
        new Object[]{"SELECT 5 >= CAST(col1 as INT) FROM a"},
        new Object[]{"SELECT 5 < CAST(col1 as INT) FROM a"},
        new Object[]{"SELECT 5 <= CAST(col1 as INT) FROM a"},
        new Object[]{"SELECT 5 = CAST(col1 as INT) FROM a"},
        new Object[]{"SELECT 5 != CAST(col1 as INT) FROM a"},

        // (VARCHAR ARITHMETIC_OP INTEGER)
        new Object[]{"SELECT CAST(col1 as INT) + 1 > 5 FROM a"},
        new Object[]{"SELECT CAST(col1 as INT) - 1 > 5 FROM a"},
        new Object[]{"SELECT CAST(col1 as INT) * 1 > 5 FROM a"},
        new Object[]{"SELECT CAST(col1 as INT) / 1 > 5 FROM a"},

        // (INTEGER ARITHMETIC_OP VARCHAR)
        new Object[]{"SELECT 5 > CAST(col1 as INT) + 1 FROM a"},
        new Object[]{"SELECT 5 > CAST(col1 as INT) - 1 FROM a"},
        new Object[]{"SELECT 5 > CAST(col1 as INT) * 1 FROM a"},
        new Object[]{"SELECT 5 > CAST(col1 as INT) / 1 FROM a"},

        //Allow Cast of INT -> VARCHAR
        new Object[]{"SELECT col1 > CAST(5 AS VARCHAR) FROM a"},
    };
  }

  @Test(dataProvider = "explicitCastCharToIntInProject")
  public void testExplicitCastCharToIntInProject(String query) {
    Assert.assertNotNull(_queryEnvironment.planQuery(query));
  }

  @DataProvider(name = "implicitCastCharToIntOnClause")
  protected Object[][] provideQueriesWithOnClause() {
    return new Object[][]{
        new Object[]{"SELECT a.col1, c.col3 from a join c ON a.col1 = c.col3"},
        new Object[]{"SELECT a.col1, c.col3 from a join c ON a.col1 != c.col3"},
        new Object[]{"SELECT a.col1, c.col3 from a join c ON a.col1 > c.col3"},
        new Object[]{"SELECT a.col1, c.col3 from a join c ON a.col1 >= c.col3"},
        new Object[]{"SELECT a.col1, c.col3 from a join c ON a.col1 <= c.col3"},
        new Object[]{"SELECT a.col1, c.col3 from a join c ON a.col1 < c.col3"},
    };
  }

  @Test(dataProvider = "implicitCastCharToIntOnClause", expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Cannot apply .+ to arguments of type .*")
  public void testImplicitCastCharToIntOnClause(String query) {
    _queryEnvironment.planQuery(query);
  }

  @DataProvider(name = "explicitCastCharToIntOnClause")
  protected Object[][] provideQueriesWithExplicitCastOnClause() {
    return new Object[][]{
        new Object[]{"SELECT a.col1, c.col3 from a join c ON CAST(a.col1 as INT) = c.col3"},
        new Object[]{"SELECT a.col1, c.col3 from a join c ON CAST(a.col1 as INT) != c.col3"},
        new Object[]{"SELECT a.col1, c.col3 from a join c ON CAST(a.col1 as INT) > c.col3"},
        new Object[]{"SELECT a.col1, c.col3 from a join c ON CAST(a.col1 as INT) >= c.col3"},
        new Object[]{"SELECT a.col1, c.col3 from a join c ON CAST(a.col1 as INT) <= c.col3"},
        new Object[]{"SELECT a.col1, c.col3 from a join c ON CAST(a.col1 as INT) < c.col3"},

        //Allow Cast of INT -> VARCHAR
        new Object[]{"SELECT a.col1, c.col3 from a join c ON a.col1 < CAST(c.col3 as VARCHAR)"},

    };
  }

  @Test(dataProvider = "explicitCastCharToIntOnClause")
  public void testExplicitCastCharToIntOnClause(String query) {
    Assert.assertNotNull(_queryEnvironment.planQuery(query));
  }
}
