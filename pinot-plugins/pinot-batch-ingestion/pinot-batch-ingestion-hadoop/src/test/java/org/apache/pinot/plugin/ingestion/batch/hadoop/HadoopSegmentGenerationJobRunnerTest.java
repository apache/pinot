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

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig;
import org.apache.pinot.plugin.inputformat.json.JSONRecordReader;
import org.apache.pinot.spi.ingestion.batch.spec.ExecutionFrameworkSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.RecordReaderSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class HadoopSegmentGenerationJobRunnerTest {

  @Test
  public void testDistributedCache() throws Exception {
    File testDir = Files.createTempDirectory("testDistributedCache-").toFile();
    testDir.delete();
    testDir.mkdirs();
    
    File inputDir = new File(testDir, "input");
    inputDir.mkdirs();
    File inputFile = new File(inputDir, "input.csv");
    FileUtils.writeLines(inputFile, Lists.newArrayList("col1,col2", "value1,1", "value2,2"));
    
    File outputDir = new File(testDir, "output");
    
    // Set up schema file.
    File schemaFile = new File(testDir, "schema");
    FileUtils.writeLines(schemaFile, Lists.newArrayList("{ \"schemaName\": \"mySchema\",",
        "\"dimensionFieldSpecs\": [ { \"name\": \"col1\", \"dataType\": \"STRING\" } ],",
        "\"metricFieldSpecs\": [ { \"name\": \"col2\", \"dataType\": \"INT\" } ],",
        "\"dateTimeFieldSpecs\": [] } "));
    
    // Set up table config file.
    File tableConfig = new File(testDir, "tableConfig");
    FileUtils.writeLines(tableConfig, Lists.newArrayList("{ \"tableName\": \"myTable\", \"tableType\":\"OFFLINE\",",
        "\"segmentsConfig\" : { \"schemaName\" : \"mySchema\", \"replication\" : \"1\" }, ",
        "\"tableIndexConfig\" : { }, ",
        "\"tenants\" : { \"broker\":\"DefaultTenant\", \"server\":\"DefaultTenant\" }, ",
        "\"metadata\": {} } "));
    
    File stagingDir = new File(testDir, "staging");
    stagingDir.mkdir();
    
    // Set up plugins dir
    File pluginsDir = new File(testDir, "plugins");
    pluginsDir.mkdir();
    File pluginJar = new File(CSVRecordReaderConfig.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    FileUtils.copyFile(pluginJar, new File(pluginsDir, pluginJar.getName()));
    
    // Set up dependency jars dir.
    // FUTURE set up jar with class that we need for reading file, so we know it's working
    File dependencyJarsDir = new File(testDir, "jars");
    dependencyJarsDir.mkdir();
    File extraJar = new File(JSONRecordReader.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    FileUtils.copyFile(extraJar, new File(dependencyJarsDir, extraJar.getName()));

    SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
    jobSpec.setInputDirURI(inputDir.toURI().toString());
    jobSpec.setOutputDirURI(outputDir.toURI().toString());
    
    RecordReaderSpec recordReaderSpec = new RecordReaderSpec();
    recordReaderSpec.setDataFormat("csv");
    recordReaderSpec.setClassName("org.apache.pinot.plugin.inputformat.csv.CSVRecordReader");
    recordReaderSpec.setConfigClassName("org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig");
    jobSpec.setRecordReaderSpec(recordReaderSpec);
    
    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName("myTable");
    tableSpec.setSchemaURI(schemaFile.toURI().toString());
    tableSpec.setTableConfigURI(tableConfig.toURI().toString());
    jobSpec.setTableSpec(tableSpec);
    
    ExecutionFrameworkSpec efSpec = new ExecutionFrameworkSpec();
    Map<String, String> extraConfigs = new HashMap<>();
    extraConfigs.put("stagingDir", stagingDir.toURI().toString());
    extraConfigs.put("dependencyJarDir", dependencyJarsDir.toURI().toString());
    efSpec.setExtraConfigs(extraConfigs);
    jobSpec.setExecutionFrameworkSpec(efSpec);
    
    PinotFSSpec pfsSpec = new PinotFSSpec();
    pfsSpec.setScheme("file");
    pfsSpec.setClassName("org.apache.pinot.spi.filesystem.LocalPinotFS");
    jobSpec.setPinotFSSpecs(Collections.singletonList(pfsSpec));
    
    System.setProperty(PluginManager.PLUGINS_DIR_PROPERTY_NAME, pluginsDir.getAbsolutePath());    
    HadoopSegmentGenerationJobRunner jobRunner = new HadoopSegmentGenerationJobRunner(jobSpec);
    jobRunner.run();
    
    // The staging directory should be removed
    Assert.assertFalse(stagingDir.exists());
    
    // The output directory should have a single segment in it.
    File segmentFile = new File(outputDir, "myTable_OFFLINE_0.tar.gz");
    Assert.assertTrue(segmentFile.exists());
    Assert.assertTrue(segmentFile.isFile());
    Assert.assertTrue(segmentFile.length() > 0);
    
    // FUTURE - validate contents of file?
  }
}
