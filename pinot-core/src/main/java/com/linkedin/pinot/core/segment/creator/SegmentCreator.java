/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.creator;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.configuration.ConfigurationException;


/**
 * Interface for segment creators, which create an index over a set of rows and writes the resulting index to disk.
 */
public interface SegmentCreator extends Closeable {

  /**
   * Initializes the segment creation.
   *
   * @param segmentCreationSpec
   * @param indexCreationInfoMap
   * @param schema
   * @param outDir
   * @throws Exception
   */
  void init(SegmentGeneratorConfig segmentCreationSpec, SegmentIndexCreationInfo segmentIndexCreationInfo,
      Map<String, ColumnIndexCreationInfo> indexCreationInfoMap, Schema schema, File outDir) throws Exception;

  /**
   * Adds a row to the index.
   *
   * @param row The row to index.
   */
  void indexRow(GenericRow row);

  /**
   * Sets the name of the segment.
   *
   * @param segmentName The name of the segment
   */
  void setSegmentName(String segmentName);

  /**
   * Seals the segment, flushing it to disk.
   *
   * @throws ConfigurationException
   * @throws IOException
   */
  void seal() throws ConfigurationException, IOException;
}
