package com.linkedin.pinot.core.indexsegment.columnar;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.block.intarray.CompressedIntArrayDataSource;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.ColumnarReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.IndexType;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.utils.Helpers;
import com.linkedin.pinot.core.indexsegment.utils.IntArray;
import com.linkedin.pinot.core.operator.DataSource;
import com.linkedin.pinot.core.plan.FilterPlanNode;


/**
 * Jul 15, 2014
 *
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class ColumnarSegment implements IndexSegment {
  public static final Logger logger = Logger.getLogger(ColumnarSegment.class);

  private String _segmentName;
  private File _segmentDir;
  private ColumnarSegmentMetadata _segmentMetadata = null;
  private Map<String, ColumnMetadata> _columnMetadata;
  private Map<String, IntArray> _intArrayMap;
  private Map<String, Dictionary<?>> _dictionaryMap;
  private Map<String, BitmapInvertedIndex> _invertedIndexMap;
  private ReadMode _mode;

  /**
   * 
   * @param indexDir
   * @param mode
   * @throws ConfigurationException
   * @throws IOException
   */
  public ColumnarSegment(String indexDir, ReadMode mode) throws ConfigurationException, IOException {
    init(new File(indexDir), mode);
  }
  
  /**
   * 
   * @param indexDir
   * @param mode
   * @throws ConfigurationException
   * @throws IOException
   */
  public ColumnarSegment(File indexDir, ReadMode mode) throws ConfigurationException, IOException {
    init(indexDir, mode);
  }

  /**
   * 
   * @param segmentMetadata
   * @param mode
   * @throws ConfigurationException
   * @throws IOException
   */
  public ColumnarSegment(SegmentMetadata segmentMetadata, ReadMode mode) throws ConfigurationException, IOException {
    this._segmentMetadata = (ColumnarSegmentMetadata) segmentMetadata;
    init(new File(segmentMetadata.getIndexDir()), mode);
  }

  /**
   * 
   * @param indexDir
   * @param mode
   * @throws ConfigurationException
   * @throws IOException
   */
  public void init(File indexDir, ReadMode mode) throws ConfigurationException, IOException {
    _segmentDir = indexDir;
    _segmentName = indexDir.getName();
    if (_segmentMetadata == null) {
      _segmentMetadata = new ColumnarSegmentMetadata(new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME));
    }
    _columnMetadata = new HashMap<String, ColumnMetadata>();
    _intArrayMap = new HashMap<String, IntArray>();
    _dictionaryMap = new HashMap<String, Dictionary<?>>();
    _invertedIndexMap = new HashMap<String, BitmapInvertedIndex>();
    this._mode = mode;

    for (String column : _segmentMetadata.getAllColumnNames()) {
        logger.info("starting to load column : " + column);
        long start = System.currentTimeMillis();

        _columnMetadata.put(column, new ColumnMetadata(new File(indexDir, V1Constants.MetadataKeys.METADATA_FILE_NAME),
            column, _segmentMetadata.getFieldTypeFor(column)));
        logger.info("loaded metadata for column : " + column);
        _dictionaryMap.put(
            column,
            DictionaryLoader.load(this._mode,
                new File(indexDir, Helpers.STRING.concat(column, V1Constants.Dict.FILE_EXTENTION)),
                _columnMetadata.get(column)));
        logger.info("loaded dictionary for column : " + column + " of type : "
            + _columnMetadata.get(column).getDataType() + " in : " + mode);
        if (_columnMetadata.get(column).isSorted()) {
          _intArrayMap.put(column, IntArrayLoader.load(this._mode,
              new File(indexDir, Helpers.STRING.concat(column, V1Constants.Indexes.SORTED_FWD_IDX_FILE_EXTENTION)),
              _columnMetadata.get(column)));
        } else if (_columnMetadata.get(column).isSingleValued()) {
          _intArrayMap.put(column, IntArrayLoader.load(this._mode,
              new File(indexDir, Helpers.STRING.concat(column, V1Constants.Indexes.UN_SORTED_FWD_IDX_FILE_EXTENTION)),
              _columnMetadata.get(column)));
        }

        logger.info("loaded fwd idx array for column : " + column + " in mode : " + mode);
        if (_columnMetadata.get(column).hasInvertedIndex()) {
          logger.info("loading bitmap for column : " + column);
          _invertedIndexMap.put(
              column,
              BitmapInvertedIndexLoader.load(new File(indexDir, column
                  + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION), mode, _columnMetadata.get(column)));
        }

        logger.info("loaded fwd idx array for column : " + column + " in mode : " + mode);

        logger.info("total processing time for column : " + column + " was : " + (System.currentTimeMillis() - start));
      }
  }

  public BitmapInvertedIndex getInvertedIndexFor(String column) {
    return _invertedIndexMap.get(column);
  }

  public Map<String, BitmapInvertedIndex> getInvertedIndexMap() {
    return _invertedIndexMap;
  }

  public Map<String, ColumnMetadata> getColumnMetadataMap() {
    return _columnMetadata;
  }

  public ColumnMetadata getColumnMetadataFor(String column) {
    return _columnMetadata.get(column);
  }

  public Map<String, IntArray> getIntArraysMap() {
    return _intArrayMap;
  }

  public IntArray getIntArrayFor(String column) {
    return _intArrayMap.get(column);
  }

  public Map<String, Dictionary<?>> getDictionaryMap() {
    return _dictionaryMap;
  }

  public Dictionary<?> getDictionaryFor(String column) {
    return _dictionaryMap.get(column);
  }

  @Override
  public IndexType getIndexType() {
    return IndexType.columnar;
  }

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  public String getAssociatedDirectory() {
    return _segmentDir.getAbsolutePath();
  }

  @Override
  public DataSource getDataSource(String columnName) {
    // TODO Auto-generated method stub
    return new CompressedIntArrayDataSource(_intArrayMap.get(columnName), _dictionaryMap.get(columnName),
        _columnMetadata.get(columnName));
  }

  @Override
  public DataSource getDataSource(String columnName, Predicate p) {
    CompressedIntArrayDataSource ds =
        new CompressedIntArrayDataSource(_intArrayMap.get(columnName), _dictionaryMap.get(columnName),
            _columnMetadata.get(columnName));
    ds.setPredicate(p);
    return ds;
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    return _segmentMetadata;
  }

  /**
   * @param brokerRequest
   */
  @Override
  public Iterator<Integer> getDocIdIterator(BrokerRequest brokerRequest) {
    if (!brokerRequest.isSetFilterQuery() || (!brokerRequest.getFilterQuery().isSetOperator())) {
      return new Iterator<Integer>() {
        private int segmentSize = getSegmentMetadata().getTotalDocs();
        private int next = 0;

        @Override
        public boolean hasNext() {
          return next < segmentSize;
        }

        @Override
        public Integer next() {
          return next++;
        }

        @Override
        public void remove() {
          // TODO Auto-generated method stub

        }
      };
    }
    FilterPlanNode filterPlanNode = new FilterPlanNode(this, brokerRequest);
    final Operator operator = filterPlanNode.run();
    Iterator<Integer> iterator = new Iterator<Integer>() {

      private int next;
      private BlockDocIdIterator currentBlockDocIdIterator;

      @Override
      public boolean hasNext() {
        while ((currentBlockDocIdIterator == null) || ((next = currentBlockDocIdIterator.next()) == Constants.EOF)) {
          Block nextBlock = operator.nextBlock();
          if (nextBlock == null) {
            return false;
          }
          currentBlockDocIdIterator = nextBlock.getBlockDocIdSet().iterator();
        }
        // if it comes here, then next is set
        return true;
      }

      @Override
      public Integer next() {
        return next;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
    return iterator;

  }

  @Override
  public ColumnarReader getColumnarReader(final String column) {
    return new ColumnarReader() {
      Dictionary<?> dict = _dictionaryMap.get(column);
      IntArray arr = _intArrayMap.get(column);

      @Override
      public String getStringValue(int docId) {
        return dict.getString(arr.getInt(docId));
      }

      @Override
      public long getLongValue(int docId) {
        return (Long) dict.getRaw(arr.getInt(docId));
      }

      @Override
      public int getIntegerValue(int docId) {
        return (Integer) dict.getRaw(arr.getInt(docId));
      }

      @Override
      public float getFloatValue(int docId) {
        return (Float) dict.getRaw(arr.getInt(docId));
      }

      @Override
      public double getDoubleValue(int docId) {
        return (Double) dict.getRaw(arr.getInt(docId));
      }

      @Override
      public Object getRawValue(int docId) {
        return dict.getRaw(arr.getInt(docId));
      }
    };
  }

}
