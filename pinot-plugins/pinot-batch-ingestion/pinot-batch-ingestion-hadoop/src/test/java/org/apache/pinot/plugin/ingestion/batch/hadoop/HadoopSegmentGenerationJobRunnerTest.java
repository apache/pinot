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
package org.apache.pinot.plugin.ingestion.batch.hadoop;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.text.WordUtils;
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
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HadoopSegmentGenerationJobRunnerTest {

  @Test
  public void testSegmentGeneration() throws Exception {
    File testDir = Files.createTempDirectory("testSegmentGeneration-").toFile();
    testDir.delete();
    testDir.mkdirs();

    File inputDir = new File(testDir, "input");
    inputDir.mkdirs();
    File inputFile = new File(inputDir, "input.csv");
    FileUtils.writeLines(inputFile, Lists.newArrayList("col1,col2", "value1,1", "value2,2"));

    final String outputFilename = "myTable_OFFLINE_0.tar.gz";
    final String otherFilename = "myTable_OFFLINE_100.tar.gz";
    File outputDir = new File(testDir, "output");
    FileUtils.touch(new File(outputDir, outputFilename));
    FileUtils.touch(new File(outputDir, otherFilename));

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

    File stagingDir = new File(testDir, "staging");
    stagingDir.mkdir();
    // Add the staging output dir, which should cause code to fail unless we've added code to remove
    // the staging dir if it exists.
    FileUtils.touch(new File(stagingDir, "output"));

    // Set up a plugins dir, with a sub-directory. We'll use an external jar,
    // since using a class inside of Pinot to find the enclosing jar is somehow
    // finding the directory of classes vs. the actual jar, on the build server
    // (though it works fine in other configurations).
    File pluginsDir = new File(testDir, "plugins");
    File myPluginDir = new File(pluginsDir, "my-plugin");
    myPluginDir.mkdirs();
    File pluginJar = new File(WordUtils.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    FileUtils.copyFile(pluginJar, new File(myPluginDir, pluginJar.getName()));

    // Set up dependency jars dir.
    // FUTURE set up jar with class that we need for reading file, so we know it's working
    File dependencyJarsDir = new File(testDir, "jars");
    dependencyJarsDir.mkdir();
    File extraJar = new File(Gson.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    FileUtils.copyFile(extraJar, new File(dependencyJarsDir, extraJar.getName()));

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
    efSpec.setName("hadoop");
    efSpec.setSegmentGenerationJobRunnerClassName(HadoopSegmentGenerationJobRunner.class.getName());
    Map<String, String> extraConfigs = new HashMap<>();
    extraConfigs.put("stagingDir", stagingDir.toURI().toString());
    extraConfigs.put("dependencyJarDir", dependencyJarsDir.getAbsolutePath());
    efSpec.setExtraConfigs(extraConfigs);
    jobSpec.setExecutionFrameworkSpec(efSpec);

    PinotFSSpec pfsSpec = new PinotFSSpec();
    pfsSpec.setScheme("file");
    pfsSpec.setClassName(LocalPinotFS.class.getName());
    jobSpec.setPinotFSSpecs(Collections.singletonList(pfsSpec));

    jobSpec.setFailOnEmptySegment(true);

    System.setProperty(PluginManager.PLUGINS_DIR_PROPERTY_NAME, pluginsDir.getAbsolutePath());
    HadoopSegmentGenerationJobRunner jobRunner = new HadoopSegmentGenerationJobRunner(jobSpec);
    jobRunner.run();
    Assert.assertFalse(stagingDir.exists());

    // The output directory should still have the original file in it.
    File oldSegmentFile = new File(outputDir, otherFilename);
    Assert.assertTrue(oldSegmentFile.exists());

    // The output directory should have the original file in it (since we aren't overwriting)
    File newSegmentFile = new File(outputDir, outputFilename);
    Assert.assertTrue(newSegmentFile.exists());
    Assert.assertTrue(newSegmentFile.isFile());
    Assert.assertTrue(newSegmentFile.length() == 0);

    // Now run again, but this time with overwriting of output files, and confirm we got a valid segment file.
    jobSpec.setOverwriteOutput(true);
    jobRunner = new HadoopSegmentGenerationJobRunner(jobSpec);
    jobRunner.run();
    Assert.assertFalse(stagingDir.exists());

    // The original file should still be there.
    Assert.assertTrue(oldSegmentFile.exists());

    Assert.assertTrue(newSegmentFile.exists());
    Assert.assertTrue(newSegmentFile.isFile());
    Assert.assertTrue(newSegmentFile.length() > 0);

    // FUTURE - validate contents of file?
  }
}
