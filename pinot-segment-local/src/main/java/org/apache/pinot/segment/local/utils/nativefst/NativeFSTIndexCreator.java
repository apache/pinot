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
package org.apache.pinot.segment.local.utils.nativefst;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTBuilder;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.creator.FSTIndexCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NativeFSTIndexCreator implements FSTIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeFSTIndexCreator.class);

  private final File _fstIndexFile;
  private final FSTBuilder _fstBuilder;

  private int _dictId;

  /**
   * This index requires values of the column be added in sorted order. Sorted entries could be passed in through
   * constructor or added through addSortedDictIds function. Index of the sorted entry should correspond to the
   * dictionary id.
   *
   * @param indexDir  Index directory
   * @param columnName Column name for which index is being created
   * @param sortedEntries Sorted entries of the unique values of the column.
   */
  public NativeFSTIndexCreator(File indexDir, String columnName, String[] sortedEntries) {
    _fstIndexFile = new File(indexDir, columnName + V1Constants.Indexes.LUCENE_V9_FST_INDEX_FILE_EXTENSION);

    _fstBuilder = new FSTBuilder();
    _dictId = 0;
    if (sortedEntries != null) {
      for (_dictId = 0; _dictId < sortedEntries.length; _dictId++) {
        _fstBuilder.add(sortedEntries[_dictId].getBytes(), 0, sortedEntries[_dictId].length(), _dictId);
      }
    }
  }

  public NativeFSTIndexCreator(IndexCreationContext context) {
    this(context.getIndexDir(), context.getFieldSpec().getName(), (String[]) context.getSortedUniqueElementsArray());
  }

  // Expects dictionary entries in sorted order.
  @Override
  public void add(String document) {
    _fstBuilder.add(document.getBytes(), 0, document.length(), _dictId);
    _dictId++;
  }

  @Override
  public void add(String[] document, int length) {
    throw new UnsupportedOperationException("Multiple values not supported");
  }

  @Override
  public void seal()
      throws IOException {
    LOGGER.info("Sealing FST index: " + _fstIndexFile.getAbsolutePath());
    try (FileOutputStream fileOutputStream = new FileOutputStream(_fstIndexFile)) {
      FST fst = _fstBuilder.complete();
      fst.save(fileOutputStream);
    }
  }

  @Override
  public void close()
      throws IOException {
  }
}
