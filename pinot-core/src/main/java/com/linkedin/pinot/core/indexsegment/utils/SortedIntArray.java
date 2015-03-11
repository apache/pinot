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
package com.linkedin.pinot.core.indexsegment.utils;

/**
 *
 * @author Dhaval Patel<dpatel@linkedin.com
 * Aug 19, 2014
 */
public class SortedIntArray implements IntArray {

  GenericRowColumnDataFileReader sortedIndexFile;
  SearchableByteBufferUtil searchableBuffer;

  public SortedIntArray(GenericRowColumnDataFileReader reader) {
    sortedIndexFile = reader;
    searchableBuffer = new SearchableByteBufferUtil(reader);
  }

  @Override
  public void setInt(int index, int value) {
    return;
  }

  @Override
  public int getInt(int docId) {
    return getReader().getValueIndex(docId);
  }

  public int getMinDocId(int dictionaryId) {
    return sortedIndexFile.getInt(dictionaryId, 0);
  }

  public int getMaxDocId(int dictionaryId) {
    return sortedIndexFile.getInt(dictionaryId, 1);
  }

  @Override
  public int size() {
    return sortedIndexFile.getNumberOfRows();
  }

  /*
   * will do this in a better way later
   *
   * */

  private interface SingleValueRandomReader {
    int getValueIndex(int docId);
  }

  private SingleValueRandomReader getReader() {
    return new SingleValueRandomReader() {
      SingleValueRandomReader randomReader = getReaderInternal();

      @Override
      public int getValueIndex(int docId) {
        int ret = randomReader.getValueIndex(docId);
        if (ret < 0) {
          randomReader = getReaderInternal();
        } else {
          return ret;
        }
        ret = randomReader.getValueIndex(docId);
        if (ret < 0) {
          return ret;
        }
        return ret;
      }
    };
  }

  private SingleValueRandomReader getReaderInternal() {
    if (sortedIndexFile.getNumberOfRows() == 1) {
      return new SingleValueRandomReader() {

        @Override
        public int getValueIndex(int docId) {
          if (docId <= getMaxDocId(0)) {
            return 0;
          }
          return -1;
        }
      };
    }

    return new SingleValueRandomReader() {
      private int currentValueId = -1;

      @Override
      public int getValueIndex(int docId) {
        if (currentValueId == -1) {
          if (getMaxDocId(0) >= docId) {
            currentValueId = 0;
          } else {
            int index = searchableBuffer.binarySearch(1, docId, 0, sortedIndexFile.getNumberOfRows());
            if (index < 0) {
              index = (index + 1) * -1;
            }
            currentValueId = index;
            if (index >= sortedIndexFile.getNumberOfRows()) {
              return -1;
            }
          }
        } else if (docId > getMaxDocId(currentValueId)) {
          currentValueId++;
          if (currentValueId == sortedIndexFile.getNumberOfRows()) {
            return -1;
          }
          if (docId > getMaxDocId(currentValueId)) {
            int index = searchableBuffer.binarySearch(1, docId, currentValueId, sortedIndexFile.getNumberOfRows());
            if (index < 0) {
              index = (index + 1) * -1;
            }
            currentValueId = index;
            if (index >= sortedIndexFile.getNumberOfRows()) {
              return -1;
            }
          }

        } else if (docId < getMinDocId(currentValueId)) {
          return -1;
        }
        return currentValueId;
      }
    };
  }
}
