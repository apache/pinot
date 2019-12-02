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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.utils.DataSize;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.filesystem.PinotFSFactory;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.common.PinotClusterSpec;
import org.apache.pinot.ingestion.common.PinotFSSpec;
import org.apache.pinot.ingestion.common.SegmentGenerationJobSpec;
import org.apache.pinot.ingestion.common.SegmentGenerationTaskRunner;
import org.apache.pinot.ingestion.common.SegmentGenerationTaskSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentGenerationJobRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentGenerationJobRunner.class);
  private static final String OFFLINE = "OFFLINE";

  private SegmentGenerationJobSpec _spec;

  public SegmentGenerationJobRunner(SegmentGenerationJobSpec spec) {
    _spec = spec;
    if (_spec.getInputDirURI() == null) {
      throw new RuntimeException("Missing property 'inputDirURI' in 'jobSpec' file");
    }
    if (_spec.getOutputDirURI() == null) {
      throw new RuntimeException("Missing property 'outputDirURI' in 'jobSpec' file");
    }
    if (_spec.getRecordReaderSpec() == null) {
      throw new RuntimeException("Missing property 'recordReaderSpec' in 'jobSpec' file");
    }
    if (_spec.getTableSpec() == null) {
      throw new RuntimeException("Missing property 'tableSpec' in 'jobSpec' file");
    }
    if (_spec.getTableSpec().getTableName() == null) {
      throw new RuntimeException("Missing property 'tableName' in 'tableSpec'");
    }
    if (_spec.getTableSpec().getSchemaURI() == null) {
      if (_spec.getPinotClusterSpecs() == null || _spec.getPinotClusterSpecs().length == 0) {
        throw new RuntimeException("Missing property 'schemaURI' in 'tableSpec'");
      }
      PinotClusterSpec pinotClusterSpec = _spec.getPinotClusterSpecs()[0];
      String schemaURI = String
          .format("http://%s:%d/tables/%s/schema", pinotClusterSpec.getHost(), pinotClusterSpec.getPort(),
              _spec.getTableSpec().getTableName());
      _spec.getTableSpec().setSchemaURI(schemaURI);
    }
    if (_spec.getTableSpec().getTableConfigURI() == null) {
      if (_spec.getPinotClusterSpecs() == null || _spec.getPinotClusterSpecs().length == 0) {
        throw new RuntimeException("Missing property 'tableConfigURI' in 'tableSpec'");
      }
      PinotClusterSpec pinotClusterSpec = _spec.getPinotClusterSpecs()[0];
      String tableConfigURI = String
          .format("http://%s:%d/tables/%s", pinotClusterSpec.getHost(), pinotClusterSpec.getPort(),
              _spec.getTableSpec().getTableName());
      _spec.getTableSpec().setTableConfigURI(tableConfigURI);
    }
  }

  /**
   * Generate a relative output directory path when `useRelativePath` flag is on.
   * This method will compute the relative path based on `inputFile` and `baseInputDir`,
   * then apply only the directory part of relative path to `outputDir`.
   * E.g.
   *    baseInputDir = "/path/to/input"
   *    inputFile = "/path/to/input/a/b/c/d.avro"
   *    outputDir = "/path/to/output"
   *    getRelativeOutputPath(baseInputDir, inputFile, outputDir) = /path/to/output/a/b/c
   */
  public static URI getRelativeOutputPath(URI baseInputDir, URI inputFile, URI outputDir) {
    URI relativePath = baseInputDir.relativize(inputFile);
    Preconditions.checkState(relativePath.getPath().length() > 0 && !relativePath.equals(inputFile),
        "Unable to extract out the relative path based on base input path: " + baseInputDir);
    String outputDirStr = outputDir.toString();
    outputDir = !outputDirStr.endsWith("/") ? URI.create(outputDirStr.concat("/")) : outputDir;
    URI relativeOutputURI = outputDir.resolve(relativePath).resolve(".");
    return relativeOutputURI;
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
    outputDirFS.mkdir(outputDirURI);

    //Get list of files to process
    String[] files = inputDirFS.listFiles(inputDirURI, true);

    //TODO: sort input files based on creation time
    List<String> filteredFiles = new ArrayList<>();
    PathMatcher includeFilePathMatcher = null;
    if (_spec.getIncludeFileNamePattern() != null) {
      includeFilePathMatcher = FileSystems.getDefault().getPathMatcher(_spec.getIncludeFileNamePattern());
    }
    PathMatcher excludeFilePathMatcher = null;
    if (_spec.getExcludeFileNamePattern() != null) {
      excludeFilePathMatcher = FileSystems.getDefault().getPathMatcher(_spec.getExcludeFileNamePattern());
    }

    for (String file : files) {
      if (includeFilePathMatcher != null) {
        if (!includeFilePathMatcher.matches(Paths.get(file))) {
          continue;
        }
      }
      if (excludeFilePathMatcher != null) {
        if (excludeFilePathMatcher.matches(Paths.get(file))) {
          continue;
        }
      }
      if (!inputDirFS.isDirectory(new URI(file))) {
        filteredFiles.add(file);
      }
    }

    File localTempDir = new File(FileUtils.getTempDirectory(), "pinot-" + System.currentTimeMillis());
    try {
      //create localTempDir for input and output
      File localInputTempDir = new File(localTempDir, "input");
      FileUtils.forceMkdir(localInputTempDir);
      File localOutputTempDir = new File(localTempDir, "output");
      FileUtils.forceMkdir(localOutputTempDir);

      //Read TableConfig, Schema
      Schema schema = getSchema();
      TableConfig tableConfig = getTableConfig();

      //iterate on the file list, for each
      for (int i = 0; i < filteredFiles.size(); i++) {
        URI inputFileURI = URI.create(filteredFiles.get(i));
        if (inputFileURI.getScheme() == null) {
          inputFileURI =
              new URI(inputDirURI.getScheme(), inputFileURI.getSchemeSpecificPart(), inputFileURI.getFragment());
        }
        //copy input path to local
        File localInputDataFile = new File(localInputTempDir, new File(inputFileURI).getName());
        inputDirFS.copyToLocalFile(inputFileURI, localInputDataFile);

        //create task spec
        SegmentGenerationTaskSpec taskSpec = new SegmentGenerationTaskSpec();
        taskSpec.setInputFilePath(localInputDataFile.getAbsolutePath());
        taskSpec.setOutputDirectoryPath(localOutputTempDir.getAbsolutePath());
        taskSpec.setRecordReaderSpec(_spec.getRecordReaderSpec());
        taskSpec.setSchema(schema);
        taskSpec.setTableConfig(tableConfig);
        taskSpec.setSequenceId(i);
        taskSpec.setSegmentNameGeneratorSpec(_spec.getSegmentNameGeneratorSpec());

        //invoke segmentGenerationTask
        SegmentGenerationTaskRunner taskRunner = new SegmentGenerationTaskRunner(taskSpec);
        String segmentName = taskRunner.run();

        // Tar segment directory to compress file
        File localSegmentDir = new File(localOutputTempDir, segmentName);
        String segmentTarFileName = segmentName + JobConfigConstants.TAR_GZ_FILE_EXT;
        File localSegmentTarFile = new File(localOutputTempDir, segmentTarFileName);
        LOGGER.info("Tarring segment from: {} to: {}", localSegmentDir, localSegmentTarFile);
        TarGzCompressionUtils.createTarGzOfDirectory(localSegmentDir.getPath(), localSegmentTarFile.getPath());
        long uncompressedSegmentSize = FileUtils.sizeOf(localSegmentDir);
        long compressedSegmentSize = FileUtils.sizeOf(localSegmentTarFile);
        LOGGER.info("Size for segment: {}, uncompressed: {}, compressed: {}", segmentName,
            DataSize.fromBytes(uncompressedSegmentSize), DataSize.fromBytes(compressedSegmentSize));
        //move segment to output PinotFS
        URI outputSegmentTarURI =
            getRelativeOutputPath(inputDirURI, inputFileURI, outputDirURI).resolve(segmentTarFileName);
        if (!_spec.isOverwriteOutput() && outputDirFS.exists(outputSegmentTarURI)) {
          LOGGER.warn("Not overwrite existing output segment tar file: {}", outputDirFS.exists(outputSegmentTarURI));
        } else {
          outputDirFS.copyFromLocalFile(localSegmentTarFile, outputSegmentTarURI);
        }
        FileUtils.deleteQuietly(localSegmentDir);
        FileUtils.deleteQuietly(localSegmentTarFile);
        FileUtils.deleteQuietly(localInputDataFile);
      }
    } finally {
      //clean up
      FileUtils.deleteDirectory(localTempDir);
    }
  }

  private Schema getSchema()
      throws Exception {
    URI schemaURI = new URI(_spec.getTableSpec().getSchemaURI());
    String scheme = schemaURI.getScheme();
    String schemaJson;
    if (PinotFSFactory.isSchemeSupported(scheme)) {
      // Try to use PinotFS to read schema URI
      PinotFS pinotFS = PinotFSFactory.create(scheme);
      InputStream schemaStream = pinotFS.open(schemaURI);
      schemaJson = IOUtils.toString(schemaStream, StandardCharsets.UTF_8);
    } else {
      // Try to directly read from URI.
      schemaJson = IOUtils.toString(schemaURI, StandardCharsets.UTF_8);
    }
    return Schema.fromString(schemaJson);
  }

  private TableConfig getTableConfig()
      throws Exception {
    URI tableConfigURI = new URI(_spec.getTableSpec().getTableConfigURI());
    String scheme = tableConfigURI.getScheme();
    String tableConfigJson;
    if (PinotFSFactory.isSchemeSupported(scheme)) {
      // Try to use PinotFS to read table config URI
      PinotFS pinotFS = PinotFSFactory.create(scheme);
      tableConfigJson = IOUtils.toString(pinotFS.open(tableConfigURI), StandardCharsets.UTF_8);
    } else {
      tableConfigJson = IOUtils.toString(tableConfigURI, StandardCharsets.UTF_8);
    }
    // Controller API returns a wrapper of table config.
    JsonNode tableJsonNode = new ObjectMapper().readTree(tableConfigJson);
    if (tableJsonNode.has(OFFLINE)) {
      tableJsonNode = tableJsonNode.get(OFFLINE);
    }
    TableConfig tableConfig = TableConfig.fromJsonConfig(tableJsonNode);
    return tableConfig;
  }
}
