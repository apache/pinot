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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DeleteSegmentStatementTest {

  @Test
  public void testDeleteFromWhereSegmentEqual()
      throws Exception {
    String deleteFromSql = "DELETE FROM \"baseballStats\"\n"
        + "WHERE $segmentName = 'foobar';";
    DeleteSegmentStatement deleteSegmentStatement = DeleteSegmentStatement.parse(CalciteSqlParser.compileToSqlNodeAndOptions(deleteFromSql));
    Assert.assertEquals(deleteSegmentStatement.getTable(), "baseballStats");
    Assert.assertEquals(deleteSegmentStatement.getExecutionType(), DataManipulationStatement.ExecutionType.MINION);
    Assert.assertEquals(deleteSegmentStatement.getResultSchema(), new DataSchema(new String[]{"tableName", "taskJobName"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING}));
    Assert.assertEquals(deleteSegmentStatement.getQueryOptions(),
        Map.of("segmentName", "'foobar'", "operator", "="));
    AdhocTaskConfig adhocTaskConfig = deleteSegmentStatement.generateAdhocTaskConfig();
    Assert.assertEquals(adhocTaskConfig.getTaskType(), "SegmentDeletionTask");
    Assert.assertNull(adhocTaskConfig.getTaskName());
    Assert.assertEquals(adhocTaskConfig.getTableName(), "baseballStats");
    Assert.assertEquals(adhocTaskConfig.getTaskConfigs(),
        Map.of("segmentName", "'foobar'", "operator", "="));
  }

  @Test
  public void testDeleteFromWhereSegmentLike()
      throws Exception {
    String deleteFromSql = "DELETE FROM \"baseballStats\"\n"
        + "WHERE $segmentName LIKE 'mySegment';";
    DeleteSegmentStatement deleteSegmentStatement = DeleteSegmentStatement.parse(CalciteSqlParser.compileToSqlNodeAndOptions(deleteFromSql));
    Assert.assertEquals(deleteSegmentStatement.getTable(), "baseballStats");
    Assert.assertEquals(deleteSegmentStatement.getExecutionType(), DataManipulationStatement.ExecutionType.MINION);
    Assert.assertEquals(deleteSegmentStatement.getResultSchema(), new DataSchema(new String[]{"tableName", "taskJobName"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING}));
    Assert.assertEquals(deleteSegmentStatement.getQueryOptions(),
        Map.of("segmentName", "'mySegment'", "operator", "LIKE"));
    AdhocTaskConfig adhocTaskConfig = deleteSegmentStatement.generateAdhocTaskConfig();
    Assert.assertEquals(adhocTaskConfig.getTaskType(), "SegmentDeletionTask");
    Assert.assertNull(adhocTaskConfig.getTaskName());
    Assert.assertEquals(adhocTaskConfig.getTableName(), "baseballStats");
    Assert.assertEquals(adhocTaskConfig.getTaskConfigs(),
        Map.of("segmentName", "'mySegment'", "operator", "LIKE"));
  }

  @Test
  public void testDeleteStatementMissingWhereClause() {
    String deleteFromSql = "DELETE FROM \"baseballStats\"\n";
    try {
      DeleteSegmentStatement.parse(CalciteSqlParser.compileToSqlNodeAndOptions(deleteFromSql));
      Assert.fail("expected exception was not thrown");
    } catch (Exception ex) {
      Assert.assertEquals(ex.getMessage(), "missing WHERE clause");
    }
  }
}
