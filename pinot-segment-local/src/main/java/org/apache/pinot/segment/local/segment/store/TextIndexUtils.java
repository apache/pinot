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
package org.apache.pinot.segment.local.segment.store;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;


class TextIndexUtils {
  private TextIndexUtils() {
  }

  static void cleanupTextIndex(File segDir, String column) {
    // Remove the lucene index file and potentially the docId mapping file.
    File idxFile = new File(segDir, column + V1Constants.Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION);
    FileUtils.deleteQuietly(idxFile);
    File mapFile = new File(segDir, column + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION);
    FileUtils.deleteQuietly(mapFile);
  }

  static boolean hasTextIndex(File segDir, String column) {
    return new File(segDir, column + V1Constants.Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION).exists();
  }
}
