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

import org.apache.commons.lang3.StringUtils;


/**
 * Defines all the keys used in the batch configs map
 */
public class BatchConfigProperties {

  public static final String DOT_SEPARATOR = ".";
  public static final String BATCH_PREFIX = "batch";

  public static final String BATCH_TYPE = "batchType";
  public static final String INPUT_DIR_URI = "inputDirURI";
  public static final String OUTPUT_DIR_URI = "outputDirURI";
  public static final String FS_CLASS = "fs.className";
  public static final String FS_PROP_PREFIX = "fs.prop";
  public static final String INPUT_FORMAT = "inputFormat";
  public static final String RECORD_READER_CLASS = "recordReader.className";
  public static final String RECORD_READER_CONFIG_CLASS = "recordReader.config.className";
  public static final String RECORD_READER_PROP_PREFIX = "recordReader.prop";

  public static final String INPUT_DATA_FILE_URI_KEY = "input.data.file.uri";
  /**
   * Helper method to create a batch config property
   */
  public static String constructBatchProperty(String batchType, String property) {
    return StringUtils.join(BATCH_PREFIX, batchType, property, DOT_SEPARATOR);
  }

}
