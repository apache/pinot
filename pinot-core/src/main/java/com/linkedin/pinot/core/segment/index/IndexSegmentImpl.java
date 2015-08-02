/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.File;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.data.source.ColumnDataSourceImpl;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


/**
 * Nov 12, 2014
 */

public class IndexSegmentImpl implements IndexSegment {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexSegmentImpl.class);

  private final File indexDir;
  private final SegmentMetadataImpl segmentMetadata;
  private final Map<String, ColumnIndexContainer> indexContainerMap;

  public IndexSegmentImpl(File indexDir, SegmentMetadataImpl segmentMetadata,
      Map<String, ColumnIndexContainer> columnIndexContainerMap) throws Exception {
    this.indexDir = indexDir;
    this.segmentMetadata = segmentMetadata;
    this.indexContainerMap = columnIndexContainerMap;
    LOGGER.info("successfully loaded the index segment : " + indexDir.getName());
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
    return indexDir.getAbsolutePath();
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    return segmentMetadata;
  }

  @Override
  public DataSource getDataSource(String columnName) {
    final DataSource d = new ColumnDataSourceImpl(indexContainerMap.get(columnName));
    return d;
  }

  public DataSource getDataSource(String columnName, Predicate p) {
    throw new UnsupportedOperationException("cannot ask for a data source with a predicate");
  }

  @Override
  public String[] getColumnNames() {
    return segmentMetadata.getSchema().getColumnNames().toArray(new String[0]);
  }

  @Override
  public void destroy() {
    for (String column : indexContainerMap.keySet()) {

      try {
        indexContainerMap.get(column).getDictionary().close();
      } catch (Exception e) {
        LOGGER.error("Error when close dictionary index for column : " + column, e);
      }
      try {
        indexContainerMap.get(column).getForwardIndex().close();
      } catch (Exception e) {
        LOGGER.error("Error when close forward index for column : " + column, e);
      }
      try {
        if (indexContainerMap.get(column).getInvertedIndex() != null) {
          indexContainerMap.get(column).getInvertedIndex().close();
        }
      } catch (Exception e) {
        LOGGER.error("Error when close inverted index for column : " + column, e);
      }
    }
    indexContainerMap.clear();
  }

  @Override
  public int getTotalDocs() {
    return segmentMetadata.getTotalDocs();
  }

}
