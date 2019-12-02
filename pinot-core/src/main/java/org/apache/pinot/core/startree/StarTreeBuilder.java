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
package org.apache.pinot.core.startree;

import com.google.common.collect.BiMap;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.core.segment.creator.ColumnIndexCreationInfo;


public interface StarTreeBuilder extends Closeable {

  /**
   * Initialize the builder, called before append().
   */
  void init(StarTreeBuilderConfig config)
      throws IOException;

  /**
   * Append a document to the star tree.
   */
  void append(GenericRow row)
      throws IOException;

  /**
   * Build the StarTree, called after all documents get appended.
   */
  void build()
      throws IOException;

  /**
   * Iterator to iterate over the records from startDocId to endDocId (exclusive).
   */
  Iterator<GenericRow> iterator(int startDocId, int endDocId)
      throws IOException;

  /**
   * Serialize the star tree into a file.
   */
  void serializeTree(File starTreeFile, Map<String, ColumnIndexCreationInfo> indexCreationInfoMap)
      throws IOException;

  /**
   * Returns the total number of non-aggregate dimension combinations.
   */
  int getTotalRawDocumentCount();

  /**
   * Returns the total number of aggregate dimension combinations.
   */
  int getTotalAggregateDocumentCount();

  /**
   * Returns the split order
   */
  List<String> getDimensionsSplitOrder();

  Set<String> getSkipMaterializationDimensions();

  List<String> getDimensionNames();

  List<BiMap<Object, Integer>> getDimensionDictionaries();
}
