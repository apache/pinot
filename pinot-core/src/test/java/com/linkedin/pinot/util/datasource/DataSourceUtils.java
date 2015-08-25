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
package com.linkedin.pinot.util.datasource;

import java.util.Arrays;

import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.segment.index.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class DataSourceUtils {

  public static DataSource getBitmapBasedDataSource(final int[] dictionary, final int[] values) {
    final MutableRoaringBitmap bitmaps[] = new MutableRoaringBitmap[dictionary.length];

    for (int i = 0; i < bitmaps.length; i++) {
      bitmaps[i] = new MutableRoaringBitmap();
    }

    for (int docId = 0; docId < values.length; docId++) {
      bitmaps[Arrays.binarySearch(dictionary, values[docId])].add(docId);
    }

    return new DataSource() {
      private int blockCounter = 0;

      @Override
      public boolean open() {
        return true;
      }

      @Override
      public Block getNextBlock(BlockId BlockId) {
        return new SingleValueBlock(dictionary, values);
      }

      @Override
      public String getOperatorName() {
        return "DataSourceUtils_DataSource";
      }

      @Override
      public Block getNextBlock() {
        if (blockCounter == 0) {
          return nextBlock(new BlockId(0));
        }
        blockCounter++;
        return null;
      }

      @Override
      public boolean close() {
        return true;
      }

      @Override
      public boolean setPredicate(Predicate predicate) {
        throw new UnsupportedOperationException();
      }

      @Override
      public DataSourceMetadata getDataSourceMetadata() {
        return new DataSourceMetadata() {

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
            return false;
          }

          @Override
          public boolean hasDictionary() {
            return false;
          }

          @Override
          public FieldType getFieldType() {
            return null;
          }

          @Override
          public DataType getDataType() {
            return null;
          }

          @Override
          public int cardinality() {
            return 0;
          }
        };
      }

      @Override
      public InvertedIndexReader getInvertedIndex() {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public Dictionary getDictionary() {
        // TODO Auto-generated method stub
        return null;
      }
    };
  }

  public static void getSortedSolumnDataSource(final int[] dictionary, final int[] values) {
    //final int[] min
  }
  /**
   * return new DataSource() {
      private int blockCounter = 0;

      @Override
      public boolean open() {
        return true;
      }

      @Override
      public Block nextBlock(BlockId BlockId) {
        return null;
      }

      @Override
      public Block nextBlock() {
        if (blockCounter == 0) {
          return nextBlock(new BlockId(0));
        }
        blockCounter++;
        return null;
      }

      @Override
      public boolean close() {
        return true;
      }

      @Override
      public boolean setPredicate(Predicate predicate) {
        throw new UnsupportedOperationException();
      }

      @Override
      public DataSourceMetadata getDataSourceMetadata() {
        return new DataSourceMetadata() {

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
            return false;
          }

          @Override
          public boolean hasDictionary() {
            return false;
          }

          @Override
          public FieldType getFieldType() {
            return null;
          }

          @Override
          public DataType getDataType() {
            return null;
          }

          @Override
          public int cardinality() {
            return 0;
          }
        };
      }
    };
   */
}
