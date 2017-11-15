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
package com.linkedin.pinot.core.segment.index;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.data.source.ColumnDataSource;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.core.startree.StarTree;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexSegmentImpl implements IndexSegment {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexSegmentImpl.class);

  private SegmentDirectory segmentDirectory;
  private final SegmentMetadataImpl segmentMetadata;
  private final Map<String, ColumnIndexContainer> indexContainerMap;
  private final StarTree starTree;

  public IndexSegmentImpl(SegmentDirectory segmentDirectory, SegmentMetadataImpl segmentMetadata,
      Map<String, ColumnIndexContainer> columnIndexContainerMap, StarTree starTree) throws Exception {
    this.segmentDirectory = segmentDirectory;
    this.segmentMetadata = segmentMetadata;
    this.indexContainerMap = columnIndexContainerMap;
    this.starTree = starTree;
    LOGGER.info("Successfully loaded the index segment : " + segmentDirectory);
  }

  public ImmutableDictionaryReader getDictionaryFor(String column) {
    return indexContainerMap.get(column).getDictionary();
  }

  public DataFileReader getForwardIndexReaderFor(String column) {
    return indexContainerMap.get(column).getForwardIndex();
  }

  public InvertedIndexReader getInvertedIndexFor(String column) {
    return indexContainerMap.get(column).getInvertedIndex();
  }

  @Override
  public IndexType getIndexType() {
    return IndexType.COLUMNAR;
  }

  @Override
  public String getSegmentName() {
    return segmentMetadata.getName();
  }

  @Override
  public String getAssociatedDirectory() {
    return segmentDirectory.getPath().toString();
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    return segmentMetadata;
  }

  @Override
  public ColumnDataSource getDataSource(String columnName) {
    return new ColumnDataSource(indexContainerMap.get(columnName), segmentMetadata.getColumnMetadataFor(columnName));
  }

  @Override
  public String[] getColumnNames() {
    return segmentMetadata.getSchema().getColumnNames().toArray(new String[0]);
  }

  @Override
  public void destroy() {
    LOGGER.info("Trying to destroy segment : {}", this.getSegmentName());
    for (String column : indexContainerMap.keySet()) {
      ColumnIndexContainer columnIndexContainer = indexContainerMap.get(column);

      try {
        ImmutableDictionaryReader dictionary = columnIndexContainer.getDictionary();
        if (dictionary != null) {
          dictionary.close();
        }
      } catch (Exception e) {
        LOGGER.error("Error when close dictionary index for column : " + column, e);
      }
      try {
        columnIndexContainer.getForwardIndex().close();
      } catch (Exception e) {
        LOGGER.error("Error when close forward index for column : " + column, e);
      }
      try {
        InvertedIndexReader invertedIndex = columnIndexContainer.getInvertedIndex();
        if (invertedIndex != null) {
          invertedIndex.close();
        }
      } catch (Exception e) {
        LOGGER.error("Error when close inverted index for column : " + column, e);
      }
    }
    try {
      segmentDirectory.close();
    } catch (Exception e) {
      LOGGER.error("Failed to close segment directory: {}. Continuing with error.", segmentDirectory, e);
    }
    indexContainerMap.clear();
    if (starTree != null) {
      try {
        starTree.close();
      } catch (IOException e) {
        LOGGER.error("Failed to close star-tree. Continuing with error.", e);
      }
    }
  }

  @Override
  public StarTree getStarTree() {
    return starTree;
  }

  @Override
  public long getDiskSizeBytes() {
    return segmentDirectory.getDiskSizeBytes();
  }


  public Iterator<GenericRow> iterator(final int startDocId, final int endDocId) {
    final Map<String, BlockSingleValIterator> singleValIteratorMap = new HashMap<>();
    final Map<String, BlockMultiValIterator> multiValIteratorMap = new HashMap<>();
    for (String column : getColumnNames()) {
      DataSource dataSource = getDataSource(column);
      BlockValIterator iterator = dataSource.nextBlock().getBlockValueSet().iterator();
      if (dataSource.getDataSourceMetadata().isSingleValue()) {
        singleValIteratorMap.put(column, (BlockSingleValIterator) iterator);
      } else {
        multiValIteratorMap.put(column, (BlockMultiValIterator) iterator);
      }
    }

    return new Iterator<GenericRow>() {
      int docId = startDocId;

      @Override
      public boolean hasNext() {
        return docId < endDocId;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }

      @Override
      public GenericRow next() {
        Map<String, Object> map = new HashMap<>();
        for (String column : singleValIteratorMap.keySet()) {
          int dictId = singleValIteratorMap.get(column).nextIntVal();
          Dictionary dictionary = getDictionaryFor(column);
          map.put(column, dictionary.get(dictId));
        }
        for (String column : multiValIteratorMap.keySet()) {
          //TODO:handle multi value
        }
        GenericRow genericRow = new GenericRow();
        genericRow.init(map);
        docId++;
        return genericRow;
      }
    };
  }
}
