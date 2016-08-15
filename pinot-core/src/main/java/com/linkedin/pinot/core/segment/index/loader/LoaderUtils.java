/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.loader;

import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LoaderUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoaderUtils.class);

  private LoaderUtils() {
  }

  /**
   * Write an index file to v3 format single index file and remove the old one.
   *
   * @param segmentWriter v3 format segment writer.
   * @param column column name.
   * @param indexFile index file to write from.
   * @param indexType index type.
   * @throws IOException
   */
  public static void writeIndexToV3Format(SegmentDirectory.Writer segmentWriter, String column, File indexFile,
      ColumnIndexType indexType)
      throws IOException {
    int fileLength = (int) indexFile.length();
    PinotDataBuffer buffer = null;
    try {
      if (segmentWriter.hasIndexFor(column, indexType)) {
        // Index already exists, try to reuse it.

        buffer = segmentWriter.getIndexFor(column, indexType);
        if (buffer.size() != fileLength) {
          // Existed index size is not equal to index file size.
          // Throw exception to drop and re-download the segment.

          String failureMessage =
              "V3 format segment already has " + indexType + " for column: " + column + " that cannot be removed.";
          LOGGER.error(failureMessage);
          throw new IllegalStateException(failureMessage);
        }
      } else {
        // Index does not exist, create a new buffer for that.

        buffer = segmentWriter.newIndexFor(column, indexType, fileLength);
      }

      buffer.readFrom(indexFile);
    } finally {
      FileUtils.deleteQuietly(indexFile);
      if (buffer != null) {
        buffer.close();
      }
    }
  }

  /**
   * Get string list from segment properties.
   * <p>
   * NOTE: When the property associated with the key is empty, {@link PropertiesConfiguration#getList(String)} will
   * return an empty string singleton list. Using this method will return an empty list instead.
   *
   * @param key property key.
   * @return string list value for the property.
   */
  public static List<String> getStringListFromSegmentProperties(String key, PropertiesConfiguration segmentProperties) {
    List<String> stringList = new ArrayList<>();
    List propertyList = segmentProperties.getList(key);
    if (propertyList != null) {
      for (Object value : propertyList) {
        String stringValue = value.toString();
        if (!stringValue.isEmpty()) {
          stringList.add(stringValue);
        }
      }
    }
    return stringList;
  }
}
