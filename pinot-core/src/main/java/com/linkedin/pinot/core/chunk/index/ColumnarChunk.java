package com.linkedin.pinot.core.chunk.index;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.chunk.index.data.source.ChunkColumnarDataSource;
import com.linkedin.pinot.core.chunk.index.loader.Loaders;
import com.linkedin.pinot.core.chunk.index.readers.DictionaryReader;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.indexsegment.columnar.BitmapInvertedIndex;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.core.operator.DataSource;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 12, 2014
 */

public class ColumnarChunk implements IndexSegment {
  private static final Logger logger = Logger.getLogger(ColumnarChunk.class);

  private final File indexDir;
  private final ReadMode indexLoadMode;
  private final ColumnarChunkMetadata segmentMetadata;
  private final Map<String, DictionaryReader> dictionaryMap;
  private final Map<String, DataFileReader> forwardIndexMap;
  private final Map<String, BitmapInvertedIndex> invertedIndexMap;

  public ColumnarChunk(File indexDir, ReadMode loadMode) throws Exception {
    this.indexDir = indexDir;
    indexLoadMode = loadMode;
    segmentMetadata = new ColumnarChunkMetadata(new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    dictionaryMap = new HashMap<String, DictionaryReader>();
    forwardIndexMap = new HashMap<String, DataFileReader>();
    invertedIndexMap = new HashMap<String, BitmapInvertedIndex>();

    for (final String column : segmentMetadata.getAllColumns()) {
      logger.info("loading dictionary, forwardIndex, inverted index for column : " + column);
      dictionaryMap.put(column, Loaders.Dictionary.load(segmentMetadata.getColumnMetadataFor(column), new File(indexDir, column
          + V1Constants.Dict.FILE_EXTENTION), loadMode));
      forwardIndexMap.put(
          column,
          Loaders.ForwardIndex.loadFwdIndexForColumn(segmentMetadata.getColumnMetadataFor(column), new File(indexDir, column
              + V1Constants.Indexes.UN_SORTED_FWD_IDX_FILE_EXTENTION), loadMode));
      invertedIndexMap.put(
          column,
          Loaders.InvertedIndex.load(segmentMetadata.getColumnMetadataFor(column), new File(indexDir, column
              + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION), loadMode));
    }
  }

  public Map<String, DictionaryReader> getDictionaryMap() {
    return dictionaryMap;
  }

  public DictionaryReader getDictionaryFor(String column) {
    return dictionaryMap.get(column);
  }

  public DataFileReader getForwardIndexReaderFor(String column) {
    return forwardIndexMap.get(column);
  }

  public BitmapInvertedIndex getInvertedIndexFor(String column) {
    return invertedIndexMap.get(column);
  }

  @Override
  public IndexType getIndexType() {
    return IndexType.columnar;
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
    return null;
  }

  @Override
  public DataSource getDataSource(String columnName, Predicate p) {
    final DataSource d = new ChunkColumnarDataSource(dictionaryMap.get(columnName), forwardIndexMap.get(columnName), invertedIndexMap.get(columnName), segmentMetadata.getColumnMetadataFor(columnName));
    d.setPredicate(p);
    return d;
  }

  @Override
  public String[] getColumnNames() {
    return (String[]) segmentMetadata.getSchema().getColumnNames().toArray();
  }

}
