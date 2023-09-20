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
package org.apache.pinot.spi.ingestion.batch;

/**
 * Defines all the keys used in the batch configs map
 */
public class BatchConfigProperties {
  private BatchConfigProperties() {
  }

  public static final String TABLE_NAME = "tableName";

  public static final String INPUT_DIR_URI = "inputDirURI";
  public static final String OUTPUT_DIR_URI = "outputDirURI";
  public static final String INPUT_FS_CLASS = "input.fs.className";
  public static final String INPUT_FS_PROP_PREFIX = "input.fs.prop";
  public static final String OUTPUT_FS_CLASS = "output.fs.className";
  public static final String OUTPUT_FS_PROP_PREFIX = "output.fs.prop";
  public static final String INPUT_FORMAT = "inputFormat";
  public static final String INCLUDE_FILE_NAME_PATTERN = "includeFileNamePattern";
  public static final String EXCLUDE_FILE_NAME_PATTERN = "excludeFileNamePattern";
  public static final String RECORD_READER_CLASS = "recordReader.className";
  public static final String RECORD_READER_CONFIG_CLASS = "recordReader.configClassName";
  public static final String RECORD_READER_PROP_PREFIX = "recordReader.prop";
  public static final String TABLE_CONFIGS = "tableConfigs";
  public static final String TABLE_CONFIGS_URI = "tableConfigsURI";
  public static final String SCHEMA = "schema";
  public static final String SCHEMA_URI = "schemaURI";
  public static final String SEQUENCE_ID = "sequenceId";
  public static final String SEGMENT_NAME_GENERATOR_TYPE = "segmentNameGenerator.type";
  public static final String SEGMENT_NAME_GENERATOR_PROP_PREFIX = "segmentNameGenerator.configs";
  public static final String SEGMENT_NAME = "segment.name";
  public static final String SEGMENT_NAME_PREFIX = "segment.name.prefix";
  public static final String SEGMENT_NAME_POSTFIX = "segment.name.postfix";
  public static final String EXCLUDE_SEQUENCE_ID = "exclude.sequence.id";
  public static final String OVERWRITE_OUTPUT = "overwriteOutput";
  public static final String INPUT_DATA_FILE_URI_KEY = "input.data.file.uri";
  public static final String PUSH_MODE = "push.mode";
  public static final String PUSH_ATTEMPTS = "push.attempts";
  public static final String PUSH_PARALLELISM = "push.parallelism";
  public static final String PUSH_RETRY_INTERVAL_MILLIS = "push.retry.interval.millis";
  public static final String PUSH_CONTROLLER_URI = "push.controllerUri";
  public static final String PUSH_SEGMENT_URI_PREFIX = "push.segmentUriPrefix";
  public static final String PUSH_SEGMENT_URI_SUFFIX = "push.segmentUriSuffix";
  public static final String FAIL_ON_EMPTY_SEGMENT = "fail.on.empty.segment";
  public static final String AUTH_TOKEN = "authToken";
  public static final String APPEND_UUID_TO_SEGMENT_NAME = "append.uuid.to.segment.name";
  public static final String OMIT_TIMESTAMPS_IN_SEGMENT_NAME = "omit.timestamps.in.segment.name";

  public static final String OUTPUT_SEGMENT_DIR_URI = "output.segment.dir.uri";

  public enum SegmentIngestionType {
    APPEND, REPLACE
  }

  public static class SegmentNameGeneratorType {
    public static final String SIMPLE = "simple";
    public static final String NORMALIZED_DATE = "normalizedDate";
    public static final String FIXED = "fixed";
    public static final String INPUT_FILE = "inputFile";
  }

  public enum SegmentPushType {
    TAR, URI, METADATA
  }
}
