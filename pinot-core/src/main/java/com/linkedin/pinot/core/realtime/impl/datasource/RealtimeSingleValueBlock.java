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
package com.linkedin.pinot.core.realtime.impl.datasource;

import java.nio.ByteBuffer;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.realtime.utils.RealtimeDimensionsSerDe;
import com.linkedin.pinot.core.realtime.utils.RealtimeMetricsSerDe;
import com.linkedin.pinot.core.segment.index.block.BlockUtils;


public class RealtimeSingleValueBlock implements Block {

  private final MutableRoaringBitmap filteredBitmap;
  private final FieldSpec spec;
  private final MutableDictionaryReader dictionary;
  private final String columnName;
  private final int docIdSearchableOffset;
  private final Schema schema;
  private Predicate p;
  private final RealtimeDimensionsSerDe dimSerDe;
  private final RealtimeMetricsSerDe metSerDe;

  private final ByteBuffer[] dimBuffs;
  private final ByteBuffer[] metBuffs;
  private final int[] time;

  public RealtimeSingleValueBlock(FieldSpec spec, MutableDictionaryReader dictionary,
      MutableRoaringBitmap filteredDocids, String columnName, int docIdOffset, Schema schema,
      RealtimeDimensionsSerDe dimSerDe, RealtimeMetricsSerDe metSerde, ByteBuffer[] dims, ByteBuffer[] mets, int[] time) {
    this.spec = spec;
    this.dictionary = dictionary;
    this.filteredBitmap = filteredDocids;
    this.columnName = columnName;
    this.docIdSearchableOffset = docIdOffset;
    this.schema = schema;
    this.dimSerDe = dimSerDe;
    this.metSerDe = metSerde;
    this.dimBuffs = dims;
    this.metBuffs = mets;
    this.time = time;
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
      return BlockUtils.getBLockDocIdSetBackedByBitmap(filteredBitmap);
    }

    return BlockUtils.getDummyBlockDocIdSet(docIdSearchableOffset);
  }

  @Override
  public BlockValSet getBlockValueSet() {
    if (spec.getFieldType() == FieldType.DIMENSION) {
      return getDimensionBlockValueSet();
    } else if (spec.getFieldType() == FieldType.METRIC) {
      switch (spec.getDataType()) {
        case INT:
          return getMetricIntBlockValueSet();
        case LONG:
          return getMetricLongBlockValueSet();
        case FLOAT:
          return getMetricFloatBlockValueSet();
        case DOUBLE:
          return getMetricDoubleBlockValueSet();
        default:
          break;
      }
    } else if (spec.getFieldType() == FieldType.TIME) {
      return getTimeBlockValueSet();
    }

    throw new UnsupportedOperationException("Not support for getBlockValueSet() on column with field spec - " + spec);
  }

  private BlockValSet getDimensionBlockValueSet() {
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {
        return new BlockSingleValIterator() {
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
            counter++;
            return counter < max;
          }

          @Override
          public int nextIntVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            //            Pair<Long, Object> documentFinderPair = docIdMap.get(counter++);
            //            long hash64 = documentFinderPair.getLeft();
            //            DimensionTuple tuple = dimemsionTupleMap.get(hash64);
            //            ByteBuffer rawData = tuple.getDimBuff();
            //            int vals[] = dimSerDe.deSerializeAndReturnDicIdsFor(columnName, rawData);
            ByteBuffer rawData = dimBuffs[counter++];
            int vals[] = dimSerDe.deSerializeAndReturnDicIdsFor(columnName, rawData);
            return vals[0];
          }

          @Override
          public boolean hasNext() {
            return (counter < max);
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

  private BlockValSet getTimeBlockValueSet() {
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {
        return new BlockSingleValIterator() {
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
            counter++;
            return counter > max;
          }

          @Override
          public int nextIntVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            //Pair<Long, Object> documentFinderPair = docIdMap.get(counter++);
            return (Integer) dictionary.get(time[counter++]);
          }

          @Override
          public long nextLongVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            //Pair<Long, Object> documentFinderPair = docIdMap.get(counter++);
            return ((Integer) dictionary.get(time[counter++])).longValue();
          }

          @Override
          public float nextFloatVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            //Pair<Long, Object> documentFinderPair = docIdMap.get(counter++);
            return ((Integer) dictionary.get(time[counter++])).floatValue();
          }

          @Override
          public double nextDoubleVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            //Pair<Long, Object> documentFinderPair = docIdMap.get(counter++);
            return ((Integer) dictionary.get(time[counter++])).doubleValue();
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

  private BlockValSet getMetricDoubleBlockValueSet() {
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {
        return new BlockSingleValIterator() {
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
            counter++;
            return counter > max;
          }

          @Override
          public int nextIntVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return (int) metSerDe.getDoubleVal(columnName, rawData);
          }

          @Override
          public long nextLongVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return (long) metSerDe.getDoubleVal(columnName, rawData);
          }

          @Override
          public float nextFloatVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return (float) metSerDe.getDoubleVal(columnName, rawData);
          }

          @Override
          public double nextDoubleVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return metSerDe.getDoubleVal(columnName, rawData);
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

  private BlockValSet getMetricFloatBlockValueSet() {
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {
        return new BlockSingleValIterator() {
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
            counter++;
            return counter > max;
          }

          @Override
          public int nextIntVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return (int) metSerDe.getFloatVal(columnName, rawData);
          }

          @Override
          public long nextLongVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return (long) metSerDe.getFloatVal(columnName, rawData);
          }

          @Override
          public float nextFloatVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return metSerDe.getFloatVal(columnName, rawData);
          }

          @Override
          public double nextDoubleVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return metSerDe.getFloatVal(columnName, rawData);
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

  private BlockValSet getMetricLongBlockValueSet() {
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {
        return new BlockSingleValIterator() {
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
            counter++;
            return counter > max;
          }

          @Override
          public int nextIntVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return (int) metSerDe.getLongVal(columnName, rawData);
          }

          @Override
          public long nextLongVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return metSerDe.getLongVal(columnName, rawData);
          }

          @Override
          public float nextFloatVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return metSerDe.getLongVal(columnName, rawData);
          }

          @Override
          public double nextDoubleVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return metSerDe.getLongVal(columnName, rawData);
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

  private BlockValSet getMetricIntBlockValueSet() {
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {

        return new BlockSingleValIterator() {
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
            counter++;
            return counter > max;
          }

          @Override
          public int nextIntVal() {
            if (counter > max) {
              return Constants.EOF;
            }
            ByteBuffer rawData = metBuffs[counter++];
            int ret = metSerDe.getIntVal(columnName, rawData);
            return ret;

          }

          @Override
          public long nextLongVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter++);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return metSerDe.getIntVal(columnName, rawData);
          }

          @Override
          public float nextFloatVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter++);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return metSerDe.getIntVal(columnName, rawData);
          }

          @Override
          public double nextDoubleVal() {
            if (counter > max) {
              return Constants.EOF;
            }

            /*Pair<Long, Object> documentFinderPair = docIdMap.get(counter++);
            long hash64 = documentFinderPair.getLeft();
            DimensionTuple tuple = dimemsionTupleMap.get(hash64);*/
            ByteBuffer rawData = metBuffs[counter++];
            return metSerDe.getIntVal(columnName, rawData);
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BlockMetadata getMetadata() {
    if (spec.getFieldType() == FieldType.DIMENSION) {
      return getDimensionBlockMetadata();
    }

    return getBlockMetadataForMetricsOrTimeColumn();
  }

  private BlockMetadata getDimensionBlockMetadata() {
    return new BlockMetadata() {

      @Override
      public int maxNumberOfMultiValues() {
        return 0;
      }

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
        return true;
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
        // TODO Auto-generated method stub
        return docIdSearchableOffset;
      }

      @Override
      public int getEndDocId() {
        // TODO Auto-generated method stub
        return docIdSearchableOffset;
      }

      @Override
      public com.linkedin.pinot.core.segment.index.readers.Dictionary getDictionary() {
        return dictionary;
      }

      @Override
      public DataType getDataType() {
        return spec.getDataType();
      }
    };
  }

  private BlockMetadata getBlockMetadataForMetricsOrTimeColumn() {
    return new BlockMetadata() {

      @Override
      public int maxNumberOfMultiValues() {
        return -1;
      }

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
        return true;
      }

      @Override
      public boolean hasInvertedIndex() {
        return true;
      }

      @Override
      public boolean hasDictionary() {
        return false;
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
      public com.linkedin.pinot.core.segment.index.readers.Dictionary getDictionary() {
        return null;
      }

      @Override
      public DataType getDataType() {
        return spec.getDataType();
      }
    };
  }
}
