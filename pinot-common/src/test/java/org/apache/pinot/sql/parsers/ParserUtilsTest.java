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
package org.apache.pinot.sql.parsers;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ParserUtilsTest {

  @Test
  public void testRemoveExcessiveWhiteSpace() {

    testRemoveExcessiveWhiteSpace(
      "SELECT * FROM mytable " + " ".repeat(20000),
      "SELECT * FROM mytable"
    );

    testRemoveExcessiveWhiteSpace(
      "SELECT * FROM " + " ".repeat(20000) + " mytable",
      "SELECT * FROM mytable"
    );

    testRemoveExcessiveWhiteSpace(
      "SELECT * " + " ".repeat(20000) + "FROM mytable " + " ".repeat(20000),
      "SELECT * FROM mytable"
    );

    testRemoveExcessiveWhiteSpace(
        "SELECT * FROM mytable" + " ".repeat(20000) + " options(a=b)" + " ".repeat(20000),
      "SELECT * FROM mytable options(a=b)"
    );
  }

  private void testRemoveExcessiveWhiteSpace(
      String sqlWithExcessiveWhitespace,
      String expectedSqlAfterSanitization
  ) {
    String sanitizedSql = ParserUtils.sanitizeSqlForParsing(sqlWithExcessiveWhitespace);
    Assert.assertEquals(sanitizedSql, expectedSqlAfterSanitization);
  }
}
