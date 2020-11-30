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

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationTaskRunner;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.runner.IngestionJobRunner;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationTaskSpec;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentGenerationJobRunner implements IngestionJobRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentGenerationJobRunner.class);

  private SegmentGenerationJobSpec _spec;
  private ExecutorService _executorService;

  public SegmentGenerationJobRunner() {
  }

  public SegmentGenerationJobRunner(SegmentGenerationJobSpec spec) {
    init(spec);
  }

  @Override
  public void init(SegmentGenerationJobSpec spec) {
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
      String schemaURI = SegmentGenerationUtils
          .generateSchemaURI(pinotClusterSpec.getControllerURI(), _spec.getTableSpec().getTableName());
      _spec.getTableSpec().setSchemaURI(schemaURI);
    }
    if (_spec.getTableSpec().getTableConfigURI() == null) {
      if (_spec.getPinotClusterSpecs() == null || _spec.getPinotClusterSpecs().length == 0) {
        throw new RuntimeException("Missing property 'tableConfigURI' in 'tableSpec'");
      }
      PinotClusterSpec pinotClusterSpec = _spec.getPinotClusterSpecs()[0];
      String tableConfigURI = SegmentGenerationUtils
          .generateTableConfigURI(pinotClusterSpec.getControllerURI(), _spec.getTableSpec().getTableName());
      _spec.getTableSpec().setTableConfigURI(tableConfigURI);
    }
    final int jobParallelism = _spec.getSegmentCreationJobParallelism();
    int numThreads = JobUtils.getNumThreads(jobParallelism);
    LOGGER.info("Creating an executor service with {} threads(Job parallelism: {}, available cores: {}.)", numThreads,
        jobParallelism, Runtime.getRuntime().availableProcessors());
    _executorService = Executors.newFixedThreadPool(numThreads);
  }

  @Override
  public void run()
      throws Exception {
    //init all file systems
    List<PinotFSSpec> pinotFSSpecs = _spec.getPinotFSSpecs();
    for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
      PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), new PinotConfiguration(pinotFSSpec));
    }

    //Get pinotFS for input
    final URI inputDirURI = getDirectoryUri(_spec.getInputDirURI());
    PinotFS inputDirFS = PinotFSFactory.create(inputDirURI.getScheme());

    //Get outputFS for writing output pinot segments
    final URI outputDirURI = getDirectoryUri(_spec.getOutputDirURI());
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
    File localTempDir = new File(FileUtils.getTempDirectory(), "pinot-" + UUID.randomUUID());
    try {
      //create localTempDir for input and output
      File localInputTempDir = new File(localTempDir, "input");
      FileUtils.forceMkdir(localInputTempDir);
      File localOutputTempDir = new File(localTempDir, "output");
      FileUtils.forceMkdir(localOutputTempDir);

      //Read TableConfig, Schema
      Schema schema = SegmentGenerationUtils.getSchema(_spec.getTableSpec().getSchemaURI());
      TableConfig tableConfig = SegmentGenerationUtils.getTableConfig(_spec.getTableSpec().getTableConfigURI());

      int numInputFiles = filteredFiles.size();
      CountDownLatch segmentCreationTaskCountDownLatch = new CountDownLatch(numInputFiles);
      //iterate on the file list, for each
      for (int i = 0; i < numInputFiles; i++) {
        final URI inputFileURI = getFileURI(filteredFiles.get(i), inputDirURI.getScheme());

        //copy input path to local
        File localInputDataFile = new File(localInputTempDir, new File(inputFileURI.getPath()).getName());
        inputDirFS.copyToLocalFile(inputFileURI, localInputDataFile);

        //create task spec
        SegmentGenerationTaskSpec taskSpec = new SegmentGenerationTaskSpec();
        taskSpec.setInputFilePath(localInputDataFile.getAbsolutePath());
        taskSpec.setOutputDirectoryPath(localOutputTempDir.getAbsolutePath());
        taskSpec.setRecordReaderSpec(_spec.getRecordReaderSpec());
        taskSpec.setSchema(schema);
        taskSpec.setTableConfig(tableConfig.toJsonNode());
        taskSpec.setSequenceId(i);
        taskSpec.setSegmentNameGeneratorSpec(_spec.getSegmentNameGeneratorSpec());
        taskSpec.setCustomProperty(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY, inputFileURI.toString());

        LOGGER.info("Submitting one Segment Generation Task for {}", inputFileURI);
        _executorService.submit(() -> {
          File localSegmentDir = null;
          File localSegmentTarFile = null;
          try {
            //invoke segmentGenerationTask
            SegmentGenerationTaskRunner taskRunner = new SegmentGenerationTaskRunner(taskSpec);
            String segmentName = taskRunner.run();
            // Tar segment directory to compress file
            localSegmentDir = new File(localOutputTempDir, segmentName);
            String segmentTarFileName = segmentName + Constants.TAR_GZ_FILE_EXT;
            localSegmentTarFile = new File(localOutputTempDir, segmentTarFileName);
            LOGGER.info("Tarring segment from: {} to: {}", localSegmentDir, localSegmentTarFile);
            TarGzCompressionUtils.createTarGzFile(localSegmentDir, localSegmentTarFile);
            long uncompressedSegmentSize = FileUtils.sizeOf(localSegmentDir);
            long compressedSegmentSize = FileUtils.sizeOf(localSegmentTarFile);
            LOGGER.info("Size for segment: {}, uncompressed: {}, compressed: {}", segmentName,
                DataSizeUtils.fromBytes(uncompressedSegmentSize), DataSizeUtils.fromBytes(compressedSegmentSize));
            //move segment to output PinotFS
            URI outputSegmentTarURI =
                SegmentGenerationUtils.getRelativeOutputPath(inputDirURI, inputFileURI, outputDirURI)
                    .resolve(segmentTarFileName);
            if (!_spec.isOverwriteOutput() && outputDirFS.exists(outputSegmentTarURI)) {
              LOGGER
                  .warn("Not overwrite existing output segment tar file: {}", outputDirFS.exists(outputSegmentTarURI));
            } else {
              outputDirFS.copyFromLocalFile(localSegmentTarFile, outputSegmentTarURI);
            }
          } catch (Exception e) {
            LOGGER.error("Failed to generate Pinot segment for file - {}", inputFileURI, e);
          } finally {
            segmentCreationTaskCountDownLatch.countDown();
            FileUtils.deleteQuietly(localSegmentDir);
            FileUtils.deleteQuietly(localSegmentTarFile);
            FileUtils.deleteQuietly(localInputDataFile);
          }
        });
      }
      segmentCreationTaskCountDownLatch.await();
    } finally {
      //clean up
      FileUtils.deleteQuietly(localTempDir);
      _executorService.shutdown();
    }
  }

  private URI getDirectoryUri(String uriStr)
      throws URISyntaxException {
    URI uri = new URI(uriStr);
    if (uri.getScheme() == null) {
      uri = new File(uriStr).toURI();
    }
    return uri;
  }

  private URI getFileURI(String uriStr, String fallbackScheme)
      throws URISyntaxException {
    URI fileURI = URI.create(uriStr);
    if (fileURI.getScheme() == null) {
      return new URI(fallbackScheme, fileURI.getSchemeSpecificPart(), fileURI.getFragment());
    }
    return fileURI;
  }
}
