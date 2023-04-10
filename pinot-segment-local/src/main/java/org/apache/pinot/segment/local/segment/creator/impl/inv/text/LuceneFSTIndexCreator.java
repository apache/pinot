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
package org.apache.pinot.segment.local.segment.creator.impl.inv.text;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.fst.FST;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentColumnarIndexCreator;
import org.apache.pinot.segment.local.utils.fst.FSTBuilder;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.creator.FSTIndexCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This index works only for dictionary enabled columns. It requires entries be added into this index in sorted
 * order and it creates a mapping from sorted entry to the index underneath. This index stores key (column value)
 * to dictionary id as an entry.
 *
 */
public class LuceneFSTIndexCreator implements FSTIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentColumnarIndexCreator.class);
  private final File _fstIndexFile;
  private final FSTBuilder _fstBuilder;
  Integer _dictId;

  /**
   * This index requires values of the column be added in sorted order. Sorted entries could be passed in through
   * constructor or added through addSortedDictIds function. Index of the sorted entry should correspond to the
   * dictionary id.
   *
   * @param indexDir  Index directory
   * @param columnName Column name for which index is being created
   * @param sortedEntries Sorted entries of the unique values of the column.
   * @throws IOException
   */
  public LuceneFSTIndexCreator(File indexDir, String columnName, String[] sortedEntries)
      throws IOException {
    _fstIndexFile = new File(indexDir, columnName + V1Constants.Indexes.FST_INDEX_FILE_EXTENSION);

    _fstBuilder = new FSTBuilder();
    _dictId = 0;
    if (sortedEntries != null) {
      for (_dictId = 0; _dictId < sortedEntries.length; _dictId++) {
        _fstBuilder.addEntry(sortedEntries[_dictId], _dictId);
      }
    }
  }

  public LuceneFSTIndexCreator(IndexCreationContext context)
      throws IOException {
    this(context.getIndexDir(), context.getFieldSpec().getName(), (String[]) context.getSortedUniqueElementsArray());
  }

  // Expects dictionary entries in sorted order.
  @Override
  public void add(String document) {
    try {
      _fstBuilder.addEntry(document, _dictId);
      _dictId++;
    } catch (IOException ex) {
      throw new RuntimeException("Unable to load the schema file", ex);
    }
  }

  @Override
  public void add(String[] documents, int length) {
    throw new UnsupportedOperationException("Multiple values not supported");
  }

  @Override
  public void seal()
      throws IOException {
    LOGGER.info("Sealing FST index: " + _fstIndexFile.getAbsolutePath());
    FileOutputStream fileOutputStream = null;
    try {
      fileOutputStream = new FileOutputStream(_fstIndexFile);
      FST<Long> fst = _fstBuilder.done();
      OutputStreamDataOutput d = new OutputStreamDataOutput(fileOutputStream);
      fst.save(d);
    } finally {
      if (fileOutputStream != null) {
        fileOutputStream.close();
      }
    }
  }

  @Override
  public void close()
      throws IOException {
  }
}
