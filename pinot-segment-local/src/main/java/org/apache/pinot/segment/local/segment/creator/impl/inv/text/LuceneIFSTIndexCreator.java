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
import javax.annotation.Nullable;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.FST;
import org.apache.pinot.segment.local.segment.index.fst.FstIndexType;
import org.apache.pinot.segment.local.utils.MetricUtils;
import org.apache.pinot.segment.local.utils.fst.IFSTBuilder;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.creator.FSTIndexCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Case-insensitive FST index creator that works only for dictionary enabled columns.
 * It requires entries be added into this index in sorted order and it creates a mapping
 * from sorted entry to the index underneath. This index stores key (column value)
 * to dictionary id as an entry, with case-insensitive handling.
 *
 */
public class LuceneIFSTIndexCreator implements FSTIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(LuceneIFSTIndexCreator.class);

  private final File _ifstIndexFile;
  private final String _columnName;
  private final String _tableNameWithType;
  private final boolean _continueOnError;
  private final IFSTBuilder _ifstBuilder = new IFSTBuilder();

  private int _dictId;

  /**
   * This index requires values of the column be added in sorted order. Sorted entries could be passed in through
   * constructor or added through addSortedDictIds function. Index of the sorted entry should correspond to the
   * dictionary id.
   *
   * @param indexDir  Index directory
   * @param columnName Column name for which index is being created
   * @param tableNameWithType table name with type
   * @param continueOnError if true, don't throw exception on add() failures
   * @param sortedEntries Sorted entries of the unique values of the column.
   * @throws IOException
   */
  public LuceneIFSTIndexCreator(File indexDir, String columnName, String tableNameWithType, boolean continueOnError,
      @Nullable String[] sortedEntries)
      throws IOException {
    _ifstIndexFile = new File(indexDir, columnName + V1Constants.Indexes.LUCENE_V912_IFST_INDEX_FILE_EXTENSION);
    _columnName = columnName;
    _tableNameWithType = tableNameWithType;
    _continueOnError = continueOnError;
    if (sortedEntries != null) {
      for (_dictId = 0; _dictId < sortedEntries.length; _dictId++) {
        try {
          _ifstBuilder.addEntry(sortedEntries[_dictId], _dictId);
        } catch (Exception e) {
          if (_continueOnError) {
            // Caught exception while trying to add, update metric and skip the document
            MetricUtils.updateIndexingErrorMetric(_tableNameWithType, FstIndexType.INDEX_DISPLAY_NAME);
          } else {
            LOGGER.error("Caught exception while trying to add to IFST index for table: {}, column: {}",
                tableNameWithType, columnName, e);
            throw e;
          }
        }
      }
    }
  }

  public LuceneIFSTIndexCreator(IndexCreationContext context)
      throws IOException {
    this(context.getIndexDir(), context.getFieldSpec().getName(), context.getTableNameWithType(),
        context.isContinueOnError(), (String[]) context.getSortedUniqueElementsArray());
  }

  // Expects dictionary entries in sorted order.
  @Override
  public void add(String document)
      throws IOException {
    try {
      _ifstBuilder.addEntry(document, _dictId);
    } catch (Exception e) {
      if (_continueOnError) {
        // Caught exception while trying to add, update metric and skip the document
        MetricUtils.updateIndexingErrorMetric(_tableNameWithType, FstIndexType.INDEX_DISPLAY_NAME);
      } else {
        LOGGER.error("Caught exception while trying to add to IFST index for table: {}, column: {}", _tableNameWithType,
            _columnName, e);
        throw e;
      }
    }
    _dictId++;
  }

  @Override
  public void add(String[] documents, int length) {
    throw new UnsupportedOperationException("Multiple values not supported");
  }

  @Override
  public void seal()
      throws IOException {
    LOGGER.info("Sealing IFST index: {}", _ifstIndexFile.getAbsolutePath());
    FST<BytesRef> ifst = _ifstBuilder.done();
    try (FileOutputStream outputStream = new FileOutputStream(_ifstIndexFile);
        OutputStreamDataOutput dataOutput = new OutputStreamDataOutput(outputStream)) {
      ifst.save(dataOutput, dataOutput);
    }
  }

  @Override
  public void close()
      throws IOException {
  }
}
