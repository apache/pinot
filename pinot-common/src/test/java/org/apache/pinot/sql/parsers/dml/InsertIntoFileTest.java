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

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.config.task.AdhocTaskConfig;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InsertIntoFileTest {

  @Test
  public void testInsertIntoStatementParser()
      throws Exception {
    String insertIntoSql = "INSERT INTO \"baseballStats\"\n"
        + "FROM FILE 's3://my-bucket/path/to/data/';\n"
        + "SET taskName = 'myTask-1';\n"
        + "SET \"input.fs.className\" = 'org.apache.pinot.plugin.filesystem.S3PinotFS';\n"
        + "SET \"input.fs.prop.accessKey\" = 'my-access-key';\n"
        + "SET \"input.fs.prop.secretKey\" = 'my-secret-key';\n"
        + "SET \"input.fs.prop.region\" = 'us-west-2';";
    InsertIntoFile insertIntoFile = InsertIntoFile.parse(CalciteSqlParser.compileToSqlNodeAndOptions(insertIntoSql));
    Assert.assertEquals(insertIntoFile.getTable(), "baseballStats");
    Assert.assertEquals(insertIntoFile.getExecutionType(), DataManipulationStatement.ExecutionType.MINION);
    Assert.assertEquals(insertIntoFile.getResultSchema(), new DataSchema(new String[]{"tableName", "taskJobName"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING}));
    Assert.assertEquals(insertIntoFile.getQueryOptions().size(), 6);
    Assert.assertEquals(insertIntoFile.getQueryOptions().get("taskName"), "myTask-1");
    Assert.assertEquals(insertIntoFile.getQueryOptions().get("input.fs.className"),
        "org.apache.pinot.plugin.filesystem.S3PinotFS");
    Assert.assertEquals(insertIntoFile.getQueryOptions().get("input.fs.prop.accessKey"), "my-access-key");
    Assert.assertEquals(insertIntoFile.getQueryOptions().get("input.fs.prop.secretKey"), "my-secret-key");
    Assert.assertEquals(insertIntoFile.getQueryOptions().get("input.fs.prop.region"), "us-west-2");
    Assert.assertEquals(insertIntoFile.getQueryOptions().get("inputDirURI"), "s3://my-bucket/path/to/data/");
    AdhocTaskConfig adhocTaskConfig = insertIntoFile.generateAdhocTaskConfig();
    Assert.assertEquals(adhocTaskConfig.getTaskType(), "SegmentGenerationAndPushTask");
    Assert.assertEquals(adhocTaskConfig.getTaskName(), "myTask-1");
    Assert.assertEquals(adhocTaskConfig.getTableName(), "baseballStats");
    Assert.assertEquals(adhocTaskConfig.getTaskConfigs().size(), 6);
    Assert.assertEquals(adhocTaskConfig.getTaskConfigs().get("taskName"), "myTask-1");
    Assert.assertEquals(adhocTaskConfig.getTaskConfigs().get("input.fs.className"),
        "org.apache.pinot.plugin.filesystem.S3PinotFS");
    Assert.assertEquals(adhocTaskConfig.getTaskConfigs().get("input.fs.prop.accessKey"), "my-access-key");
    Assert.assertEquals(adhocTaskConfig.getTaskConfigs().get("input.fs.prop.secretKey"), "my-secret-key");
    Assert.assertEquals(adhocTaskConfig.getTaskConfigs().get("input.fs.prop.region"), "us-west-2");
    Assert.assertEquals(adhocTaskConfig.getTaskConfigs().get("inputDirURI"), "s3://my-bucket/path/to/data/");
  }
}
