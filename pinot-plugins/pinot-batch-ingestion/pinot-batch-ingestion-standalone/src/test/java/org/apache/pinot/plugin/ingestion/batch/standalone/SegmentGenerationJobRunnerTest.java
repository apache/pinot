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
package org.apache.pinot.plugin.ingestion.batch.standalone;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationTaskRunner;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReader;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.Schema.SchemaBuilder;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.ExecutionFrameworkSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.RecordReaderSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class SegmentGenerationJobRunnerTest {

  @Test
  public void testSegmentGeneration() throws Exception {
    // TODO use common resource definitions & code shared with Hadoop unit test.
    // So probably need a pinot-batch-ingestion-common tests jar that we depend on.

    File testDir = makeTestDir();
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

    final String schemaName = "mySchema";
    File schemaFile = makeSchemaFile(testDir, schemaName);
    File tableConfigFile = makeTableConfigFile(testDir, schemaName);
    SegmentGenerationJobSpec jobSpec = makeJobSpec(inputDir, outputDir, schemaFile, tableConfigFile);
    jobSpec.setOverwriteOutput(false);
    SegmentGenerationJobRunner jobRunner = new SegmentGenerationJobRunner(jobSpec);
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
    jobRunner = new SegmentGenerationJobRunner(jobSpec);
    jobRunner.run();

    // The original file should still be there.
    Assert.assertTrue(oldSegmentFile.exists());

    Assert.assertTrue(newSegmentFile.exists());
    Assert.assertTrue(newSegmentFile.isFile());
    Assert.assertTrue(newSegmentFile.length() > 0);

    // FUTURE - validate contents of file?
  }

  /**
   * Enabling consistent data push should generate segment names with timestamps in order to differentiate between
   * the non-unique raw segment names.
   */
  @Test
  public void testSegmentGenerationWithConsistentPush()
      throws Exception {
    File testDir = makeTestDir();
    File inputDir = new File(testDir, "input");
    inputDir.mkdirs();
    File inputFile = new File(inputDir, "input.csv");
    FileUtils.writeLines(inputFile, Lists.newArrayList("col1,col2", "value1,1", "value2,2"));

    // Create an output directory
    File outputDir = new File(testDir, "output");

    final String schemaName = "mySchema";
    File schemaFile = makeSchemaFile(testDir, schemaName);
    File tableConfigFile = makeTableConfigFileWithConsistentPush(testDir, schemaName);
    SegmentGenerationJobSpec jobSpec = makeJobSpec(inputDir, outputDir, schemaFile, tableConfigFile);
    jobSpec.setOverwriteOutput(false);
    SegmentGenerationJobRunner jobRunner = new SegmentGenerationJobRunner(jobSpec);
    jobRunner.run();

    // There should be a tar file generated with timestamp (13 digits)
    String[] list = outputDir.list((dir, name) -> name.matches("myTable_OFFLINE_\\d{13}_0.tar.gz"));
    assertEquals(list.length, 1);
  }

  @Test
  public void testInputFilesWithSameNameInDifferentDirectories()
      throws Exception {
    File testDir = makeTestDir();
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

    final String schemaName = "mySchema";
    File schemaFile = makeSchemaFile(testDir, schemaName);
    File tableConfigFile = makeTableConfigFile(testDir, schemaName);
    SegmentGenerationJobSpec jobSpec = makeJobSpec(inputDir, outputDir, schemaFile, tableConfigFile);
    jobSpec.setSearchRecursively(true);
    SegmentGenerationJobRunner jobRunner = new SegmentGenerationJobRunner(jobSpec);
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

  @Test
  public void testFailureHandling()
      throws Exception {
    File testDir = makeTestDir();
    File inputDir = new File(testDir, "input");
    inputDir.mkdirs();

    File inputFile1 = new File(inputDir, "input1.csv");
    FileUtils.writeLines(inputFile1, Lists.newArrayList("col1,col2", "value11,11", "value12,12"));

    File inputFile2 = new File(inputDir, "input2.csv");
    FileUtils.writeLines(inputFile2, Lists.newArrayList("col1,col2", "value21,notanint", "value22,22"));

    File inputFile3 = new File(inputDir, "input3.csv");
    FileUtils.writeLines(inputFile3, Lists.newArrayList("col1,col2", "value31,31", "value32,32"));

    File outputDir = new File(testDir, "output");

    final String schemaName = "mySchema";
    File schemaFile = makeSchemaFile(testDir, schemaName);
    File tableConfigFile = makeTableConfigFile(testDir, schemaName);
    SegmentGenerationJobSpec jobSpec = makeJobSpec(inputDir, outputDir, schemaFile, tableConfigFile);

    // Set up for a segment name that matches our input filename, so we can validate
    // that only the first input file gets processed.
    SegmentNameGeneratorSpec nameSpec = new SegmentNameGeneratorSpec();
    nameSpec.setType(BatchConfigProperties.SegmentNameGeneratorType.INPUT_FILE);
    nameSpec.getConfigs().put(SegmentGenerationTaskRunner.FILE_PATH_PATTERN, ".+/(.+)\\.csv");
    nameSpec.getConfigs().put(SegmentGenerationTaskRunner.SEGMENT_NAME_TEMPLATE, "${filePathPattern:\\1}");
    jobSpec.setSegmentNameGeneratorSpec(nameSpec);

    try {
      SegmentGenerationJobRunner jobRunner = new SegmentGenerationJobRunner(jobSpec);
      jobRunner.run();
      fail("Job should have failed");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("input2.csv"), "Didn't find filename in exception message");

      // We should only have one output file, since segment generation will
      // terminate after the second input file.
      File[] segments = outputDir.listFiles(new FilenameFilter() {

        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".tar.gz");
        }
      });

      // We rely on the SegmentGenerationJobRunner doing a sort by name, so "input1.csv" will be the
      // first file we process, and "input2.csv" (the bad file) will be the second one.
      assertEquals(segments.length, 1);
      assertTrue(segments[0].getName().endsWith("input1.tar.gz"));
    }
  }

  private File makeTestDir() throws IOException {
    File testDir = Files.createTempDirectory("testSegmentGeneration-").toFile();
    testDir.delete();
    testDir.mkdirs();
    return testDir;
  }

  private File makeSchemaFile(File testDir, String schemaName) throws IOException {
    File schemaFile = new File(testDir, "schema");
    Schema schema = new SchemaBuilder()
      .setSchemaName(schemaName)
      .addSingleValueDimension("col1", DataType.STRING)
      .addMetric("col2", DataType.INT)
      .build();
    FileUtils.write(schemaFile, schema.toPrettyJsonString(), StandardCharsets.UTF_8);
    return schemaFile;
  }

  private File makeTableConfigFile(File testDir, String schemaName) throws IOException {
    File tableConfigFile = new File(testDir, "tableConfig");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("myTable")
        .setNumReplicas(1)
        .build();
    FileUtils.write(tableConfigFile, tableConfig.toJsonString(), StandardCharsets.UTF_8);
    return tableConfigFile;
  }

  private File makeTableConfigFileWithConsistentPush(File testDir, String schemaName) throws IOException {
    File tableConfigFile = new File(testDir, "tableConfig");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "REFRESH", "DAILY", true));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("myTable")
        .setNumReplicas(1)
        .setIngestionConfig(ingestionConfig)
        .build();
    FileUtils.write(tableConfigFile, tableConfig.toJsonString(), StandardCharsets.UTF_8);
    return tableConfigFile;
  }

  private SegmentGenerationJobSpec makeJobSpec(File inputDir, File outputDir, File schemaFile, File tableConfigFile) {
    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    jobSpec.setJobType("SegmentCreation");
    jobSpec.setInputDirURI(inputDir.toURI().toString());
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
    efSpec.setSegmentGenerationJobRunnerClassName(SegmentGenerationJobRunner.class.getName());
    jobSpec.setExecutionFrameworkSpec(efSpec);

    PinotFSSpec pfsSpec = new PinotFSSpec();
    pfsSpec.setScheme("file");
    pfsSpec.setClassName(LocalPinotFS.class.getName());
    jobSpec.setPinotFSSpecs(Collections.singletonList(pfsSpec));

    return jobSpec;
  }
}
