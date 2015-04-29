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
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.data.source.ColumnDataSourceImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 12, 2014
 */

public class IndexSegmentImpl implements IndexSegment {
  private static final Logger logger = Logger.getLogger(IndexSegmentImpl.class);

  private final File indexDir;
  private final ReadMode indexLoadMode;
  private final SegmentMetadataImpl segmentMetadata;
  private final Map<String, ImmutableDictionaryReader> dictionaryMap;
  private final Map<String, DataFileReader> forwardIndexMap;
  private final Map<String, InvertedIndexReader> invertedIndexMap;

  public IndexSegmentImpl(File indexDir, ReadMode loadMode) throws Exception {
    this(indexDir, loadMode, null);
  }

  public IndexSegmentImpl(File indexDir, ReadMode loadMode, IndexLoadingConfigMetadata indexLoadingConfigMetadata) throws Exception {
    this.indexDir = indexDir;
    indexLoadMode = loadMode;
    segmentMetadata = new SegmentMetadataImpl(indexDir);
    dictionaryMap = new HashMap<String, ImmutableDictionaryReader>();
    forwardIndexMap = new HashMap<String, DataFileReader>();
    invertedIndexMap = new HashMap<String, InvertedIndexReader>();
    String tableName = segmentMetadata.getTableName();

    for (final String column : segmentMetadata.getAllColumns()) {
      logger.debug("loading dictionary, forwardIndex, inverted index for column : " + column);
      dictionaryMap.put(
          column,
          Loaders.Dictionary.load(segmentMetadata.getColumnMetadataFor(column), new File(indexDir, column
              + V1Constants.Dict.FILE_EXTENTION), loadMode));
      forwardIndexMap.put(column, Loaders.ForwardIndex.loadFwdIndexForColumn(
          segmentMetadata.getColumnMetadataFor(column), new File(indexDir, column
              + V1Constants.Indexes.UN_SORTED_FWD_IDX_FILE_EXTENTION), loadMode));
      // TODO:By not breaking our testS, still default load all the inverted indexes. Will change it to below later.
      if (indexLoadingConfigMetadata == null || (!indexLoadingConfigMetadata.containsTable(tableName))
          || indexLoadingConfigMetadata.isLoadingInvertedIndexForColumn(tableName, column)) {
        invertedIndexMap.put(column,
            Loaders.InvertedIndex.load(segmentMetadata.getColumnMetadataFor(column), indexDir, column, loadMode));
      }
    }
    logger.info("successfully loaded the index segment : " + indexDir.getName());
  }

  public Map<String, ImmutableDictionaryReader> getDictionaryMap() {
    return dictionaryMap;
  }

  public ImmutableDictionaryReader getDictionaryFor(String column) {
    return dictionaryMap.get(column);
  }

  public DataFileReader getForwardIndexReaderFor(String column) {
    return forwardIndexMap.get(column);
  }

  public InvertedIndexReader getInvertedIndexFor(String column) {
    return invertedIndexMap.get(column);
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
    final DataSource d =
        new ColumnDataSourceImpl(dictionaryMap.get(columnName), forwardIndexMap.get(columnName),
            invertedIndexMap.get(columnName), segmentMetadata.getColumnMetadataFor(columnName));
    return d;
  }

  @Override
  public DataSource getDataSource(String columnName, Predicate p) {
    final DataSource d =
        new ColumnDataSourceImpl(dictionaryMap.get(columnName), forwardIndexMap.get(columnName),
            invertedIndexMap.get(columnName), segmentMetadata.getColumnMetadataFor(columnName));
    d.setPredicate(p);
    return d;
  }

  @Override
  public String[] getColumnNames() {
    return segmentMetadata.getSchema().getColumnNames().toArray(new String[0]);
  }

  @Override
  public void destroy() {
    for (String column : forwardIndexMap.keySet()) {

      try {
        dictionaryMap.get(column).close();
      } catch (Exception e) {
        logger.error("Error when close dictionary index for column : " + column + ", StackTrace: " + e);
      }
      try {
        forwardIndexMap.get(column).close();
      } catch (Exception e) {
        logger.error("Error when close forward index for column : " + column + ", StackTrace: " + e);
      }
      try {
        invertedIndexMap.get(column).close();
      } catch (Exception e) {
        logger.error("Error when close inverted index for column : " + column + ", StackTrace: " + e);
      }
    }
    dictionaryMap.clear();
    forwardIndexMap.clear();
    invertedIndexMap.clear();
  }

  @Override
  public int getTotalDocs() {
    return segmentMetadata.getTotalDocs();
  }

}
