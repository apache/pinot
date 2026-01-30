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
package org.apache.pinot.sql.parsers.dml;

import java.util.Map;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class DataManipulationStatementParserTest {
  @Test
  public void testDeleteSegmentStatement() {
    String sql = "DELETE FROM mytable WHERE $segmentName = 'mySegmentName'";
    SqlNodeAndOptions nodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);

    DataManipulationStatement statement = DataManipulationStatementParser.parse(nodeAndOptions);
    assertTrue(statement instanceof DeleteSegmentStatement);
    assertEquals(statement.getExecutionType(), DataManipulationStatement.ExecutionType.MINION);
    AdhocTaskConfig config = statement.generateAdhocTaskConfig();
    assertEquals(config.getTableName(), "mytable");
    assertEquals(config.getTaskType(), "SegmentDeletionTask");
    assertEquals(config.getTaskConfigs(),
        Map.of("operator", "=", "segmentName", "'mySegmentName'"));
  }

  @Test
  public void testInvalidDeleteSegmentStatementMissingWhereClause() {
    String sql = "DELETE FROM mytable";
    SqlNodeAndOptions nodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    try {
      DataManipulationStatementParser.parse(nodeAndOptions);
      fail("expected exception was not thrown");
    } catch (IllegalStateException ex) {
      assertEquals(ex.getMessage(), "missing WHERE clause");
    }
  }

  @Test
  public void testInvalidDeleteSegmentStatementMissingSegmentName() {
    String sql = "DELETE FROM mytable WHERE a = b";
    SqlNodeAndOptions nodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);
    try {
      DataManipulationStatementParser.parse(nodeAndOptions);
      fail("expected exception was not thrown");
    } catch (IllegalStateException ex) {
      assertEquals(ex.getMessage(), "missing $segmentName in WHERE clause");
    }
  }

  @Test
  public void testInsertIntoFromFileStatement() {
    String sql = "INSERT INTO \"baseballStats\"\n"
        + "FROM FILE 's3://my-bucket/path/to/data/';\n"
        + "SET taskName = 'myTask-1';\n"
        + "SET \"input.fs.className\" = 'org.apache.pinot.plugin.filesystem.S3PinotFS';\n"
        + "SET \"input.fs.prop.accessKey\" = 'my-access-key';\n"
        + "SET \"input.fs.prop.secretKey\" = 'my-secret-key';\n"
        + "SET \"input.fs.prop.region\" = 'us-west-2';";
    SqlNodeAndOptions nodeAndOptions = CalciteSqlParser.compileToSqlNodeAndOptions(sql);

    DataManipulationStatement statement = DataManipulationStatementParser.parse(nodeAndOptions);
    assertTrue(statement instanceof InsertIntoFile);
  }
}
