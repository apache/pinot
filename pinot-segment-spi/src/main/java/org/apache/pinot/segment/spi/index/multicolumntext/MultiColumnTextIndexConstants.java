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
package org.apache.pinot.segment.spi.index.multicolumntext;

import org.apache.pinot.segment.spi.V1Constants;


/**
 * This class contains keys of entries in metadata.properties files for multi-column text index.
 */
public class MultiColumnTextIndexConstants {

  public static final String INDEX_DIR_NAME = "multi_col_text_idx";
  public static final String INDEX_DIR_FILE_NAME =
      INDEX_DIR_NAME + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION;
  public static final String DOCID_MAPPING_FILE_NAME =
      INDEX_DIR_NAME + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION;

  private MultiColumnTextIndexConstants() {
  }

  public static class MetadataKey {
    public static final String ROOT_SUBSET = "multicolumntext";
    public static final String ROOT_PREFIX = ROOT_SUBSET + '.';

    public static final String ROOT_COLUMNS = ROOT_PREFIX + "columns";
    public static final String COLUMNS = "columns";
    public static final String INDEX_VERSION = "index.version";

    public static final String COLUMN = "column";
    public static final String COLUMN_PREFIX = ROOT_PREFIX + "column.";
    public static final String PROPERTY_SUFFIX = "property.";

    // prefix of all TextIndexConfig properties stored
    public static final String PROPERTY = "property";
    public static final String PROPERTY_SUBSET = "property";
    public static final String PROPERTY_PREFIX = ROOT_PREFIX + "property.";
  }
}
