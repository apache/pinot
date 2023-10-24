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
package org.apache.pinot.plugin.ingestion.batch.spark3;

import com.google.common.collect.Lists;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReader;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.Schema.SchemaBuilder;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.ingestion.batch.spec.ExecutionFrameworkSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.RecordReaderSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.spark.SparkContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SparkSegmentGenerationJobRunnerTest {
  private SparkContext _sparkContext;

  @BeforeClass
  public void setup() {
    _sparkContext = new SparkContext("local", SparkSegmentGenerationJobRunnerTest.class.getName());
  }

  @Test
  public void testSegmentGeneration() throws Exception {
    // TODO use common resource definitions & code shared with Hadoop unit test.
    // So probably need a pinot-batch-ingestion-common tests jar that we depend on.

    File testDir = Files.createTempDirectory("testSegmentGeneration-").toFile();
    testDir.delete();
    testDir.mkdirs();

    File inputDir = new File(testDir, "input");
    inputDir.mkdirs();
    File inputFile = new File(inputDir, "input.csv");
    FileUtils.writeLines(inputFile, Lists.newArrayList("col1,col2", "value1,1", "value2,2"));

    // Create an output directory, with two empty files in it. One we'll overwrite,
    // and one we'll leave alone.
    final String outputFilename = "myTable_OFFLINE_0.tar.gz";
    final String existingFilename = "myTable_OFFLINE_100.tar.gz";
    File outputDir = new File(testDir, "output");
    FileUtils.touch(new File(outputDir, outputFilename));
    FileUtils.touch(new File(outputDir, existingFilename));

    // Set up schema file.
    final String schemaName = "myTable";
    File schemaFile = new File(testDir, "myTable.schema");
    Schema schema = new SchemaBuilder()
      .setSchemaName(schemaName)
      .addSingleValueDimension("col1", DataType.STRING)
      .addMetric("col2", DataType.INT)
      .build();
    FileUtils.write(schemaFile, schema.toPrettyJsonString(), StandardCharsets.UTF_8);

    // Set up table config file.
    File tableConfigFile = new File(testDir, "myTable.table");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName("myTable")
      .setNumReplicas(1)
      .build();
    FileUtils.write(tableConfigFile, tableConfig.toJsonString(), StandardCharsets.UTF_8);

    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    jobSpec.setJobType("SegmentCreation");
    jobSpec.setInputDirURI(inputDir.toURI().toString());
    jobSpec.setOutputDirURI(outputDir.toURI().toString());
    jobSpec.setOverwriteOutput(false);

    RecordReaderSpec recordReaderSpec = new RecordReaderSpec();
    recordReaderSpec.setDataFormat("csv");
    recordReaderSpec.setClassName(CSVRecordReader.class.getName());
    recordReaderSpec.setConfigClassName(CSVRecordReaderConfig.class.getName());
    jobSpec.setRecordReaderSpec(recordReaderSpec);

    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName("myTable");
    tableSpec.setSchemaURI(schemaFile.toURI().toString());
    tableSpec.setTableConfigURI(tableConfigFile.toURI().toString());
    jobSpec.setTableSpec(tableSpec);

    ExecutionFrameworkSpec efSpec = new ExecutionFrameworkSpec();
    efSpec.setName("standalone");
    efSpec.setSegmentGenerationJobRunnerClassName(SparkSegmentGenerationJobRunner.class.getName());
    jobSpec.setExecutionFrameworkSpec(efSpec);

    PinotFSSpec pfsSpec = new PinotFSSpec();
    pfsSpec.setScheme("file");
    pfsSpec.setClassName(LocalPinotFS.class.getName());
    jobSpec.setPinotFSSpecs(Collections.singletonList(pfsSpec));

    SparkSegmentGenerationJobRunner jobRunner = new SparkSegmentGenerationJobRunner(jobSpec);
    jobRunner.run();

    // The output directory should still have the original file in it.
    File oldSegmentFile = new File(outputDir, existingFilename);
    Assert.assertTrue(oldSegmentFile.exists());

    // The output directory should have the original file in it (since we aren't overwriting)
    File newSegmentFile = new File(outputDir, outputFilename);
    Assert.assertTrue(newSegmentFile.exists());
    Assert.assertTrue(newSegmentFile.isFile());
    Assert.assertTrue(newSegmentFile.length() == 0);

    // Now run again, but this time with overwriting of output files, and confirm we got a valid segment file.
    jobSpec.setOverwriteOutput(true);
    jobRunner = new SparkSegmentGenerationJobRunner(jobSpec);
    jobRunner.run();

    // The original file should still be there.
    Assert.assertTrue(oldSegmentFile.exists());

    Assert.assertTrue(newSegmentFile.exists());
    Assert.assertTrue(newSegmentFile.isFile());
    Assert.assertTrue(newSegmentFile.length() > 0);

    // FUTURE - validate contents of file?
  }

  @Test
  public void testInputFilesWithSameNameInDifferentDirectories()
      throws Exception {
    File testDir = Files.createTempDirectory("testSegmentGeneration-").toFile();
    testDir.delete();
    testDir.mkdirs();

    File inputDir = new File(testDir, "input");
    File inputSubDir1 = new File(inputDir, "2009");
    File inputSubDir2 = new File(inputDir, "2010");
    inputSubDir1.mkdirs();
    inputSubDir2.mkdirs();

    File inputFile1 = new File(inputSubDir1, "input.csv");
    FileUtils.writeLines(inputFile1, Lists.newArrayList("col1,col2", "value1,1", "value2,2"));

    File inputFile2 = new File(inputSubDir2, "input.csv");
    FileUtils.writeLines(inputFile2, Lists.newArrayList("col1,col2", "value3,3", "value4,4"));

    File outputDir = new File(testDir, "output");

    // Set up schema file.
    final String schemaName = "mySchema";
    File schemaFile = new File(testDir, "schema");
    Schema schema = new SchemaBuilder()
        .setSchemaName(schemaName)
        .addSingleValueDimension("col1", DataType.STRING)
        .addMetric("col2", DataType.INT)
        .build();
    FileUtils.write(schemaFile, schema.toPrettyJsonString(), StandardCharsets.UTF_8);

    // Set up table config file.
    File tableConfigFile = new File(testDir, "tableConfig");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("myTable")
        .setNumReplicas(1)
        .build();
    FileUtils.write(tableConfigFile, tableConfig.toJsonString(), StandardCharsets.UTF_8);

    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    jobSpec.setJobType("SegmentCreation");
    jobSpec.setInputDirURI(inputDir.toURI().toString());
    jobSpec.setSearchRecursively(true);
    jobSpec.setOutputDirURI(outputDir.toURI().toString());
    jobSpec.setOverwriteOutput(true);

    RecordReaderSpec recordReaderSpec = new RecordReaderSpec();
    recordReaderSpec.setDataFormat("csv");
    recordReaderSpec.setClassName(CSVRecordReader.class.getName());
    recordReaderSpec.setConfigClassName(CSVRecordReaderConfig.class.getName());
    jobSpec.setRecordReaderSpec(recordReaderSpec);

    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName("myTable");
    tableSpec.setSchemaURI(schemaFile.toURI().toString());
    tableSpec.setTableConfigURI(tableConfigFile.toURI().toString());
    jobSpec.setTableSpec(tableSpec);

    ExecutionFrameworkSpec efSpec = new ExecutionFrameworkSpec();
    efSpec.setName("standalone");
    efSpec.setSegmentGenerationJobRunnerClassName(SparkSegmentGenerationJobRunner.class.getName());
    jobSpec.setExecutionFrameworkSpec(efSpec);

    PinotFSSpec pfsSpec = new PinotFSSpec();
    pfsSpec.setScheme("file");
    pfsSpec.setClassName(LocalPinotFS.class.getName());
    jobSpec.setPinotFSSpecs(Collections.singletonList(pfsSpec));

    SparkSegmentGenerationJobRunner jobRunner = new SparkSegmentGenerationJobRunner(jobSpec);
    jobRunner.run();

    // Check that both segment files are created

    File newSegmentFile2009 = new File(outputDir, "2009/myTable_OFFLINE_0.tar.gz");
    Assert.assertTrue(newSegmentFile2009.exists());
    Assert.assertTrue(newSegmentFile2009.isFile());
    Assert.assertTrue(newSegmentFile2009.length() > 0);

    File newSegmentFile2010 = new File(outputDir, "2010/myTable_OFFLINE_0.tar.gz");
    Assert.assertTrue(newSegmentFile2010.exists());
    Assert.assertTrue(newSegmentFile2010.isFile());
    Assert.assertTrue(newSegmentFile2010.length() > 0);
  }
}
