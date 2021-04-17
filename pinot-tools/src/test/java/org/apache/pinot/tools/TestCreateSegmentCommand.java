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
package org.apache.pinot.tools;

import com.google.common.base.Preconditions;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tools.admin.command.CreateSegmentCommand;
import org.testng.annotations.Test;


public class TestCreateSegmentCommand {
  private static final File JSON_INVALID_SAMPLE_DATA_FILE = new File(Preconditions
      .checkNotNull(TestCreateSegmentCommand.class.getClassLoader().getResource("test_data/test_invalid_data.json"))
      .getFile());

  private static CreateSegmentCommand _createSegmentCommand = new CreateSegmentCommand();

  @Test(expectedExceptions = Exception.class, expectedExceptionsMessageRegExp = "^.*test_invalid_data.json.*$")
  public void testReadingInvalidJsonFile() throws Exception {
    String fileDirectoryPath = JSON_INVALID_SAMPLE_DATA_FILE.getParent();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema fakeSchema = new Schema.SchemaBuilder().build();

    // this writes temporary files into pinot-tools/target/test-classes/test_data
    Files.write(Paths.get(fileDirectoryPath + "/tmpTableConfig.json"), tableConfig.toJsonString().getBytes());
    Files.write(Paths.get(fileDirectoryPath + "/tmpSchema.json"), fakeSchema.toSingleLineJsonString().getBytes());

    // tries creating a segment out of an invalid json file
    _createSegmentCommand.setDataDir(fileDirectoryPath);
    _createSegmentCommand.setFormat(FileFormat.JSON);
    _createSegmentCommand.setOutDir(fileDirectoryPath);
    _createSegmentCommand.setTableConfigFile(fileDirectoryPath + "/tmpTableConfig.json");
    _createSegmentCommand.setSchemaFile(fileDirectoryPath + "/tmpSchema.json");
    _createSegmentCommand.execute();

  }
}
