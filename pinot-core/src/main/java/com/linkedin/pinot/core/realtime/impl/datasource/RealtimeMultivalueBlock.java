package com.linkedin.pinot.core.realtime.impl.datasource;

import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.realtime.impl.fwdindex.DimensionTuple;
import com.linkedin.pinot.core.realtime.utils.RealtimeDimensionsSerDe;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class RealtimeMultivalueBlock implements Block {

  private final MutableRoaringBitmap filteredBitmap;
  private final FieldSpec spec;
  private final MutableDictionaryReader dictionary;
  private final Map<Object, Pair<Long, Long>> docIdMap;
  private final String columnName;
  private final int docIdSearchableOffset;
  private final Schema schema;
  private Predicate p;
  private final Map<Long, DimensionTuple> dimemsionTupleMap;
  private final int maxNumberOfMultiValuesMap;
  private final RealtimeDimensionsSerDe dimeSerDe;

  public RealtimeMultivalueBlock(FieldSpec spec, MutableDictionaryReader dictionary,
      Map<Object, Pair<Long, Long>> docIdMap, MutableRoaringBitmap filteredDocids, String columnName, int docIdOffset,
      Schema schema, Map<Long, DimensionTuple> dimemsionTupleMap, int maxNumberOfMultiValuesMap,
      RealtimeDimensionsSerDe dimeSerDe) {
    this.spec = spec;
    this.dictionary = dictionary;
    this.filteredBitmap = filteredDocids;
    this.docIdMap = docIdMap;
    this.columnName = columnName;
    this.docIdSearchableOffset = docIdOffset;
    this.schema = schema;
    this.dimemsionTupleMap = dimemsionTupleMap;
    this.maxNumberOfMultiValuesMap = maxNumberOfMultiValuesMap;
    this.dimeSerDe = dimeSerDe;
  }

  @Override
  public BlockId getId() {
    return null;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    this.p = predicate;
    return true;
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    if (this.p != null) {
      return new BlockDocIdSet() {
        @Override
        public BlockDocIdIterator iterator() {
          return new BlockDocIdIterator() {
            private int counter = 0;
            private int max = docIdSearchableOffset;
            private int[] docIds = filteredBitmap.toArray();

            @Override
            public int skipTo(int targetDocId) {
              int entry = Arrays.binarySearch(docIds, targetDocId);
              if (entry < 0) {
                entry *= -1;
              }

              if (entry >= docIds.length) {
                return Constants.EOF;
              }

              counter = entry;
              return counter;
            }

            @Override
            public int next() {
              if (counter >= docIds.length) {
                return Constants.EOF;
              }
              return docIds[counter++];
            }

            @Override
            public int currentDocId() {
              return docIds[counter];
            }
          };
        }

        @Override
        public Object getRaw() {
          return filteredBitmap;
        }
      };
    }

    return new BlockDocIdSet() {

      @Override
      public BlockDocIdIterator iterator() {
        return new BlockDocIdIterator() {
          private int counter = 0;
          private final int max = docIdSearchableOffset;

          @Override
          public int skipTo(int targetDocId) {
            if (targetDocId >= max) {
              return Constants.EOF;
            }
            counter = targetDocId;
            return counter;
          }

          @Override
          public int next() {
            return counter++;
          }

          @Override
          public int currentDocId() {
            return counter;
          }
        };
      }

      @Override
      public Object getRaw() {
        return null;
      }
    };
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {
        return new BlockMultiValIterator() {
          private int counter = 0;
          private int max = docIdSearchableOffset;

          @Override
          public boolean skipTo(int docId) {
            if (docId > max) {
              return false;
            }
            counter = docId;
            return true;
          }

          @Override
          public int size() {
            return max;
          }

          @Override
          public boolean reset() {
            counter = 0;
            return true;
          }

          @Override
          public boolean next() {
            return false;
          }

          @Override
          public int nextIntVal(int[] intArray) {
            if (counter >= max) {
              return Constants.EOF;
            }

            Pair<Long, Long> documentFinderPair = docIdMap.get(counter);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);
            IntBuffer rawData = tuple.getDimBuff();
            intArray = dimeSerDe.deSerializeAndReturnDicIdsFor(columnName, rawData);
            return intArray.length;
          }

          @Override
          public boolean hasNext() {
            return (counter <= max);
          }

          @Override
          public DataType getValueType() {
            return spec.getDataType();
          }

          @Override
          public int currentDocId() {
            return counter;
          }
        };
      }

      @Override
      public DataType getValueType() {
        return spec.getDataType();
      }
    };
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {

    return null;
  }

  @Override
  public BlockMetadata getMetadata() {
    return new BlockMetadata() {

      @Override
      public boolean isSparse() {
        return false;
      }

      @Override
      public boolean isSorted() {
        return false;
      }

      @Override
      public boolean isSingleValue() {
        return false;
      }

      @Override
      public boolean hasInvertedIndex() {
        return true;
      }

      @Override
      public boolean hasDictionary() {
        return true;
      }

      @Override
      public int getStartDocId() {
        return 0;
      }

      @Override
      public int getSize() {
        return docIdSearchableOffset;
      }

      @Override
      public int getLength() {
        return docIdSearchableOffset;
      }

      @Override
      public int getEndDocId() {
        return docIdSearchableOffset;
      }

      @Override
      public Dictionary getDictionary() {
        return dictionary;
      }

      @Override
      public DataType getDataType() {
        return spec.getDataType();
      }

      @Override
      public int maxNumberOfMultiValues() {
        return maxNumberOfMultiValuesMap;
      }
    };
  }

}
