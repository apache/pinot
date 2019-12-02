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
package org.apache.pinot.ingestion.standalone;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URI;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.filesystem.PinotFSFactory;
import org.apache.pinot.ingestion.common.PinotFSSpec;
import org.apache.pinot.ingestion.common.SegmentGenerationJobSpec;
import org.apache.pinot.ingestion.common.SegmentGenerationTaskRunner;
import org.apache.pinot.ingestion.common.SegmentGenerationTaskSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.filesystem.PinotFS;


public class SegmentGenerationJobRunner {

  private SegmentGenerationJobSpec _spec;

  public SegmentGenerationJobRunner(SegmentGenerationJobSpec spec) {

    _spec = spec;
  }

  public void run()
      throws Exception {
    //init all file systems
    List<PinotFSSpec> pinotFSSpecs = _spec.getPinotFSSpecs();
    for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
      Configuration config = new MapConfiguration(pinotFSSpec.getConfigs());
      PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), config);
    }

    //Get pinotFS for input
    URI inputDirURI = new URI(_spec.getInputDirURI());
    PinotFS inputDirFS = PinotFSFactory.create(inputDirURI.getScheme());

    //Get outputFS for writing output pinot segments
    URI outputDirURI = new URI(_spec.getOutputDirURI());
    PinotFS outputDirFS = PinotFSFactory.create(outputDirURI.getScheme());

    //Get list of files to process
    String[] files = inputDirFS.listFiles(inputDirURI, true);
    //TODO: sort input files based on creation time
    //TODO: handle input file name filters

    //create tempDirectory for input and output
    File tempDirectory = FileUtils.getTempDirectory();
    File localInputDir = new File(tempDirectory, "input");
    FileUtils.forceMkdir(localInputDir);
    File localOutputTempDirectory = new File(tempDirectory, "output");
    FileUtils.forceMkdir(localOutputTempDirectory);

    //Read TableConfig, Schema
    String schemaJson = IOUtils.toString(new URI(_spec.getTableSpec().getSchemaURI()), "UTF-8");
    Schema schema = Schema.fromString(schemaJson);
    String tableConfigJson = IOUtils.toString(new URI(_spec.getTableSpec().getTableConfigURI()), "UTF-8");
    TableConfig tableConfig = TableConfig.fromJsonString(tableConfigJson);
    //iterate on the file list, for each
    for (int i = 0; i < files.length; i++) {
      //copy input path to local
      File inputDataFile = new File(localInputDir, new File(files[i]).getName());
      inputDirFS.copyToLocalFile(new URI(files[i]), inputDataFile);

      //create taskspec
      SegmentGenerationTaskSpec taskSpec = new SegmentGenerationTaskSpec();
      taskSpec.setInputFilePath(inputDataFile.getAbsolutePath());
      taskSpec.setOutputDirectoryPath(localOutputTempDirectory.getAbsolutePath());
      taskSpec.setRecordReaderSpec(_spec.getRecordReaderSpec());
      taskSpec.setSchema(schema);
      taskSpec.setTableConfig(tableConfig);
      taskSpec.setSequenceId(i);
      taskSpec.setSegmentNameGeneratorSpec(_spec.getSegmentNameGeneratorSpec());

      //invoke segmentGenerationTask
      SegmentGenerationTaskRunner taskRunner = new SegmentGenerationTaskRunner(taskSpec);
      String segmentName = taskRunner.run();

      //move segment to output PinotFS

      File outputSegmentFile = new File(localOutputTempDirectory, segmentName + ".tar.gz");
      outputDirFS.copyFromLocalFile(outputSegmentFile, outputDirURI);

      FileUtils.deleteQuietly(outputSegmentFile);
      FileUtils.deleteQuietly(inputDataFile);
    }
    //clean up
    FileUtils.deleteDirectory(tempDirectory);
  }
}
