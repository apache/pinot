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
package org.apache.pinot.ingestion.common;

public class JobConfigConstants {
  public static final String PATH_TO_INPUT = "path.to.input";
  public static final String PATH_TO_OUTPUT = "path.to.output";
  public static final String PREPROCESS_PATH_TO_OUTPUT = "preprocess.path.to.output";
  public static final String PATH_TO_DEPS_JAR = "path.to.deps.jar";
  public static final String PATH_TO_READER_CONFIG = "path.to.reader.config";
  // Leave this for backward compatibility. We prefer to use the schema fetched from the controller.
  public static final String PATH_TO_SCHEMA = "path.to.schema";

  public static final String SEGMENT_TAR_DIR = "segmentTar";
  public static final String TAR_GZ_FILE_EXT = ".tar.gz";

  public static final String SEGMENT_TABLE_NAME = "segment.table.name";
  public static final String TABLE_CONFIG = "table.config";
  public static final String SCHEMA = "data.schema";

  public static final String SEGMENT_NAME_GENERATOR_TYPE = "segment.name.generator.type";
  public static final String SIMPLE_SEGMENT_NAME_GENERATOR = "simple";
  public static final String NORMALIZED_DATE_SEGMENT_NAME_GENERATOR = "normalizedDate";
  public static final String DEFAULT_SEGMENT_NAME_GENERATOR = SIMPLE_SEGMENT_NAME_GENERATOR;

  // For SimpleSegmentNameGenerator
  public static final String SEGMENT_NAME_POSTFIX = "segment.name.postfix";

  // For NormalizedDateSegmentNameGenerator
  public static final String SEGMENT_NAME_PREFIX = "segment.name.prefix";
  public static final String EXCLUDE_SEQUENCE_ID = "exclude.sequence.id";

  public static final String PUSH_TO_HOSTS = "push.to.hosts";
  public static final String PUSH_TO_PORT = "push.to.port";

  public static final String DEFAULT_PERMISSIONS_MASK = "fs.permissions.umask-mode";

  // The path to the record reader to be configured
  public static final String RECORD_READER_PATH = "record.reader.path";

  public static final String ENABLE_PREPROCESSING = "enable.preprocessing";

  // This setting should be used if you will generate less # of segments after
  // push. In preprocessing, this is likely because we resize segments.
  public static final String DELETE_EXTRA_SEGMENTS = "delete.extra.segments";

  // This setting is used to match output segments hierarchy along with input file hierarchy.
  public static final String USE_RELATIVE_PATH = "use.relative.path";

  // This setting enables segment push in parallel.
  public static final String ENABLE_PARALLEL_PUSH = "enable.parallel.push";
  public static final String DEFAULT_ENABLE_PARALLEL_PUSH = "false";
  public static final String PUSH_JOB_PARALLELISM = "push.job.parallelism";
  public static final String DEFAULT_PUSH_JOB_PARALLELISM = "4";
  public static final String PUSH_JOB_RETRY = "push.job.retry";
  public static final String DEFAULT_PUSH_JOB_RETRY = "3";

  // This setting ensures jobs only process files with certain time range
  public static final String LOOK_BACK_PERIOD_IN_DAYS = "look.back.period.in.days";

  // Assign sequence ids to input files based at each local directory level
  public static final String LOCAL_DIRECTORY_SEQUENCE_ID = "local.directory.sequence.id";
}
