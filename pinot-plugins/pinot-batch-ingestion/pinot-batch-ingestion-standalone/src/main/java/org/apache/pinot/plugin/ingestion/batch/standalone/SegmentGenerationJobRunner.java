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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.segment.generation.SegmentGenerationUtils;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationJobUtils;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationTaskRunner;
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
  private PinotFS _inputDirFS;
  private PinotFS _outputDirFS;
  private URI _inputDirURI;
  private URI _outputDirURI;
  private CountDownLatch _segmentCreationTaskCountDownLatch;
  private Schema _schema;
  private TableConfig _tableConfig;

  public SegmentGenerationJobRunner() {
  }

  public SegmentGenerationJobRunner(SegmentGenerationJobSpec spec) {
    init(spec);
  }

  @Override
  public void init(SegmentGenerationJobSpec spec) {
    _spec = spec;

    //Get pinotFS for input
    if (_spec.getInputDirURI() == null) {
      throw new RuntimeException("Missing property 'inputDirURI' in 'jobSpec' file");
    }
    try {
      _inputDirURI = SegmentGenerationUtils.getDirectoryURI(_spec.getInputDirURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid property: 'inputDirURI'", e);
    }
    _inputDirFS = PinotFSFactory.create(_inputDirURI.getScheme());

    //Get outputFS for writing output pinot segments
    if (_spec.getOutputDirURI() == null) {
      throw new RuntimeException("Missing property 'outputDirURI' in 'jobSpec' file");
    }
    try {
      _outputDirURI = SegmentGenerationUtils.getDirectoryURI(_spec.getOutputDirURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid property: 'outputDirURI'", e);
    }
    _outputDirFS = PinotFSFactory.create(_outputDirURI.getScheme());
    try {
      if (!_outputDirFS.exists(_outputDirURI)) {
        _outputDirFS.mkdir(_outputDirURI);
      } else if (!_outputDirFS.isDirectory(_outputDirURI)) {
        throw new RuntimeException(String.format("Output Directory URI: %s is not a directory", _outputDirURI));
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to validate output 'outputDirURI': " + _outputDirURI, e);
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

    //Read Schema
    if (_spec.getTableSpec().getSchemaURI() == null) {
      if (_spec.getPinotClusterSpecs() == null || _spec.getPinotClusterSpecs().length == 0) {
        throw new RuntimeException("Missing property 'schemaURI' in 'tableSpec'");
      }
      PinotClusterSpec pinotClusterSpec = _spec.getPinotClusterSpecs()[0];
      String schemaURI = SegmentGenerationUtils
          .generateSchemaURI(pinotClusterSpec.getControllerURI(), _spec.getTableSpec().getTableName());
      _spec.getTableSpec().setSchemaURI(schemaURI);
    }
    _schema = SegmentGenerationUtils.getSchema(_spec.getTableSpec().getSchemaURI());

    // Read Table config
    if (_spec.getTableSpec().getTableConfigURI() == null) {
      if (_spec.getPinotClusterSpecs() == null || _spec.getPinotClusterSpecs().length == 0) {
        throw new RuntimeException("Missing property 'tableConfigURI' in 'tableSpec'");
      }
      PinotClusterSpec pinotClusterSpec = _spec.getPinotClusterSpecs()[0];
      String tableConfigURI = SegmentGenerationUtils
          .generateTableConfigURI(pinotClusterSpec.getControllerURI(), _spec.getTableSpec().getTableName());
      _spec.getTableSpec().setTableConfigURI(tableConfigURI);
    }
    _tableConfig = SegmentGenerationUtils.getTableConfig(_spec.getTableSpec().getTableConfigURI());

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

    //Get list of files to process
    String[] files = _inputDirFS.listFiles(_inputDirURI, true);

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
      if (!_inputDirFS.isDirectory(new URI(file))) {
        filteredFiles.add(file);
      }
    }
    File localTempDir = new File(FileUtils.getTempDirectory(), "pinot-" + UUID.randomUUID());
    try {
      int numInputFiles = filteredFiles.size();
      _segmentCreationTaskCountDownLatch = new CountDownLatch(numInputFiles);

      if (!SegmentGenerationJobUtils.useGlobalDirectorySequenceId(_spec.getSegmentNameGeneratorSpec())) {
        Map<String, List<String>> localDirIndex = new HashMap<>();
        for (String filteredFile : filteredFiles) {
          java.nio.file.Path filteredParentPath = Paths.get(filteredFile).getParent();
          localDirIndex.computeIfAbsent(filteredParentPath.toString(), k -> new ArrayList<>()).add(filteredFile);
        }
        for (String parentPath : localDirIndex.keySet()) {
          List<String> siblingFiles = localDirIndex.get(parentPath);
          Collections.sort(siblingFiles);
          for (int i = 0; i < siblingFiles.size(); i++) {
            URI inputFileURI = SegmentGenerationUtils
                .getFileURI(siblingFiles.get(i), SegmentGenerationUtils.getDirectoryURI(parentPath));
            submitSegmentGenTask(localTempDir, inputFileURI, i);
          }
        }
      } else {
        //iterate on the file list, for each
        for (int i = 0; i < numInputFiles; i++) {
          final URI inputFileURI = SegmentGenerationUtils.getFileURI(filteredFiles.get(i), _inputDirURI);
          submitSegmentGenTask(localTempDir, inputFileURI, i);
        }
      }
      _segmentCreationTaskCountDownLatch.await();
    } finally {
      //clean up
      FileUtils.deleteQuietly(localTempDir);
      _executorService.shutdown();
    }
  }

  private void submitSegmentGenTask(File localTempDir, URI inputFileURI, int seqId)
      throws Exception {
    //create localTempDir for input and output
    File localInputTempDir = new File(localTempDir, "input");
    FileUtils.forceMkdir(localInputTempDir);
    File localOutputTempDir = new File(localTempDir, "output");
    FileUtils.forceMkdir(localOutputTempDir);

    //copy input path to local
    File localInputDataFile = new File(localInputTempDir, new File(inputFileURI.getPath()).getName());
    _inputDirFS.copyToLocalFile(inputFileURI, localInputDataFile);

    //create task spec
    SegmentGenerationTaskSpec taskSpec = new SegmentGenerationTaskSpec();
    taskSpec.setOutputDirectoryPath(localOutputTempDir.getAbsolutePath());
    taskSpec.setRecordReaderSpec(_spec.getRecordReaderSpec());
    taskSpec.setSchema(_schema);
    taskSpec.setTableConfig(_tableConfig);
    taskSpec.setSegmentNameGeneratorSpec(_spec.getSegmentNameGeneratorSpec());
    taskSpec.setInputFilePath(localInputDataFile.getAbsolutePath());
    taskSpec.setSequenceId(seqId);
    taskSpec.setFailOnEmptySegment(_spec.isFailOnEmptySegment());
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
        String segmentTarFileName = URLEncoder.encode(segmentName + Constants.TAR_GZ_FILE_EXT, "UTF-8");
        localSegmentTarFile = new File(localOutputTempDir, segmentTarFileName);
        LOGGER.info("Tarring segment from: {} to: {}", localSegmentDir, localSegmentTarFile);
        TarGzCompressionUtils.createTarGzFile(localSegmentDir, localSegmentTarFile);
        long uncompressedSegmentSize = FileUtils.sizeOf(localSegmentDir);
        long compressedSegmentSize = FileUtils.sizeOf(localSegmentTarFile);
        LOGGER.info("Size for segment: {}, uncompressed: {}, compressed: {}", segmentName,
            DataSizeUtils.fromBytes(uncompressedSegmentSize), DataSizeUtils.fromBytes(compressedSegmentSize));
        //move segment to output PinotFS
        URI outputSegmentTarURI =
            SegmentGenerationUtils.getRelativeOutputPath(_inputDirURI, inputFileURI, _outputDirURI)
                .resolve(segmentTarFileName);
        if (!_spec.isOverwriteOutput() && _outputDirFS.exists(outputSegmentTarURI)) {
          LOGGER.warn("Not overwrite existing output segment tar file: {}", _outputDirFS.exists(outputSegmentTarURI));
        } else {
          _outputDirFS.copyFromLocalFile(localSegmentTarFile, outputSegmentTarURI);
        }
      } catch (Exception e) {
        LOGGER.error("Failed to generate Pinot segment for file - {}", inputFileURI, e);
      } finally {
        _segmentCreationTaskCountDownLatch.countDown();
        FileUtils.deleteQuietly(localSegmentDir);
        FileUtils.deleteQuietly(localSegmentTarFile);
        FileUtils.deleteQuietly(localInputDataFile);
      }
    });
  }
}
