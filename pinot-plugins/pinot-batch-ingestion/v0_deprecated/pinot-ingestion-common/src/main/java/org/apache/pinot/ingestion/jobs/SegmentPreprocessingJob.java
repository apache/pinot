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
package org.apache.pinot.ingestion.jobs;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.ingestion.common.ControllerRestApi;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Hadoop job which provides partitioning, sorting, and resizing against the input files, which is raw data in Avro
 * format.
 * Thus, the output files are partitioned, sorted, resized after this job.
 * In order to run this job, the following configs need to be specified in job properties:
 * * enable.preprocessing: false by default. Enables preprocessing job.
 */
public abstract class SegmentPreprocessingJob extends BaseSegmentJob {
  private static final Logger _logger = LoggerFactory.getLogger(SegmentPreprocessingJob.class);
  protected final Path _schemaFile;
  protected final Path _inputSegmentDir;
  protected final Path _preprocessedOutputDir;
  // Optional.
  protected final Path _pathToDependencyJar;
  protected boolean _enablePreprocessing;

  public SegmentPreprocessingJob(final Properties properties) {
    super(properties);

    _enablePreprocessing = Boolean.parseBoolean(_properties.getProperty(JobConfigConstants.ENABLE_PREPROCESSING));

    // get input/output paths.
    _inputSegmentDir = Preconditions.checkNotNull(getPathFromProperty(JobConfigConstants.PATH_TO_INPUT));
    _preprocessedOutputDir = getPathFromProperty(JobConfigConstants.PREPROCESS_PATH_TO_OUTPUT);

    // Optional
    _pathToDependencyJar = getPathFromProperty(JobConfigConstants.PATH_TO_DEPS_JAR);
    _schemaFile = getPathFromProperty(JobConfigConstants.PATH_TO_SCHEMA);

    _logger.info("*********************************************************************");
    _logger.info("enable.preprocessing: {}", _enablePreprocessing);
    _logger.info("path.to.input: {}", _inputSegmentDir);
    _logger.info("preprocess.path.to.output: {}", _preprocessedOutputDir);
    _logger.info("path.to.deps.jar: {}", _pathToDependencyJar);
    _logger.info("push.locations: {}", _pushLocations);
    _logger.info("*********************************************************************");
  }

  protected abstract void run()
      throws Exception;

  @Override
  protected Schema getSchema()
      throws IOException {
    try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
      if (controllerRestApi != null) {
        return controllerRestApi.getSchema();
      } else {
        try (InputStream inputStream = FileSystem.get(_schemaFile.toUri(), getConf()).open(_schemaFile)) {
          return org.apache.pinot.spi.data.Schema.fromInputSteam(inputStream);
        }
      }
    }
  }

  @Override
  protected boolean isDataFile(String fileName) {
    // TODO: support orc format in the future.
    return fileName.endsWith(".avro");
  }
}
