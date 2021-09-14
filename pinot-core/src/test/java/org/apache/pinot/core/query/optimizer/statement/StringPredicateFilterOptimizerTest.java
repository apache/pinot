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
package org.apache.pinot.core.query.optimizer.statement;

import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.annotations.Test;


public class StringPredicateFilterOptimizerTest {
  private static final QueryOptimizer OPTIMIZER = new QueryOptimizer();
  private static final CalciteSqlCompiler SQL_COMPILER = new CalciteSqlCompiler();
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("testTable")
      .addSingleValueDimension("intColumn1", FieldSpec.DataType.INT)
      .addSingleValueDimension("intColumn2", FieldSpec.DataType.INT)
      .addSingleValueDimension("strColumn1", FieldSpec.DataType.STRING)
      .addSingleValueDimension("strColumn2", FieldSpec.DataType.STRING)
      .addSingleValueDimension("strColumn3", FieldSpec.DataType.STRING).build();
  private static final TableConfig TABLE_CONFIG_WITHOUT_INDEX = null;

  @Test
  public void testReplaceMinusWithCompare() {
    // 'WHERE strColumn1=strColumn2' gets replaced with 'strcmp(strColumn1, strColumn2) = 0'
    TestHelper.assertEqualsQuery("SELECT * FROM testTable WHERE strColumn1=strColumn2",
        "SELECT * FROM testTable WHERE strcmp(strColumn1,strColumn2) = 0", TABLE_CONFIG_WITHOUT_INDEX, SCHEMA);

    // 'WHERE strColumn1>strColumn2' gets replaced with 'strcmp(strColumn1, strColumn2) > 0'
    TestHelper.assertEqualsQuery("SELECT * FROM testTable WHERE strColumn1>strColumn2",
        "SELECT * FROM testTable WHERE strcmp(strColumn1,strColumn2) > 0", TABLE_CONFIG_WITHOUT_INDEX, SCHEMA);

    // 'HAVING strColumn1=strColumn2' gets replaced with 'strcmp(strColumn1, strColumn2) = 0'
    TestHelper.assertEqualsQuery("SELECT strColumn1, strColumn2 FROM testTable HAVING strColumn1=strColumn2",
        "SELECT strColumn1, strColumn2 FROM testTable HAVING strcmp(strColumn1,strColumn2)=0",
        TABLE_CONFIG_WITHOUT_INDEX, SCHEMA);

    // 'HAVING strColumn1=strColumn2' gets replaced with 'strcmp(strColumn1, strColumn2) < 0'
    TestHelper.assertEqualsQuery("SELECT strColumn1, strColumn2 FROM testTable HAVING strColumn1<strColumn2",
        "SELECT strColumn1, strColumn2 FROM testTable HAVING strcmp(strColumn1,strColumn2)<0",
        TABLE_CONFIG_WITHOUT_INDEX, SCHEMA);

    // 'WHERE strColumn1=strColumn2 AND strColumn1=strColumn3' gets replaced with 'strcmp(strColumn1, strColumn2) = 0
    // AND strcmp(strColumn1, strColumn3) = 0'
    TestHelper.assertEqualsQuery("SELECT * FROM testTable WHERE strColumn1=strColumn2 OR strColumn1=strColumn3",
        "SELECT * FROM testTable WHERE strcmp(strColumn1,strColumn2) = 0 OR strcmp(strColumn1,strColumn3) = 0",
        TABLE_CONFIG_WITHOUT_INDEX, SCHEMA);
  }
}
