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
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import java.math.BigDecimal;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


/// Shared helper for index handlers that need to populate a [DictionaryBasedInvertedIndexCreator]-shaped index
/// (inverted index, range index) by reading raw values from a forward index and translating them to dict IDs via a
/// [Dictionary].
///
/// [InvertedIndexHandler] and [RangeIndexHandler] both build dict-id-based indexes from raw forward
/// data when a shared dictionary exists. The data-type dispatch and per-row read/lookup/add loops are identical
/// between the two — only the creator argument differs. Both creator types implement
/// [DictionaryBasedInvertedIndexCreator], so this single method handles both.
public final class DictionaryBasedIndexBuilder {
  private DictionaryBasedIndexBuilder() {
  }

  /// Reads raw values from `forwardIndexReader`, looks each value up in `dictionary`, and feeds the
  /// (value, dictId) pair into `creator` for every doc in the segment.
  ///
  /// **MV allocation note:** the multi-value [DictionaryBasedInvertedIndexCreator#addIntMV] family takes
  /// `(values, dictIds)` where `dictIds.length` must equal `values.length`, and the reader's no-buffer overload
  /// returns an exact-length values array. As a result, both arrays are necessarily fresh per-row allocations under
  /// the current SPI; eliminating either one would require a length-aware `addXxxMV(values, dictIds, length)` SPI
  /// method.
  ///
  /// @throws IllegalStateException if the column data type is unsupported.
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static void addRawValuesViaDictionary(DictionaryBasedInvertedIndexCreator creator,
      ForwardIndexReader forwardIndexReader, ForwardIndexReaderContext readerContext, Dictionary dictionary,
      ColumnMetadata columnMetadata, int numDocs) {
    DataType dataType = columnMetadata.getDataType();
    DataType storedType = dataType.getStoredType();
    if (columnMetadata.isSingleValue()) {
      switch (storedType) {
        case INT:
          for (int i = 0; i < numDocs; i++) {
            int value = forwardIndexReader.getInt(i, readerContext);
            creator.addInt(value, dictionary.indexOf(value));
          }
          break;
        case LONG:
          for (int i = 0; i < numDocs; i++) {
            long value = forwardIndexReader.getLong(i, readerContext);
            creator.addLong(value, dictionary.indexOf(value));
          }
          break;
        case FLOAT:
          for (int i = 0; i < numDocs; i++) {
            float value = forwardIndexReader.getFloat(i, readerContext);
            creator.addFloat(value, dictionary.indexOf(value));
          }
          break;
        case DOUBLE:
          for (int i = 0; i < numDocs; i++) {
            double value = forwardIndexReader.getDouble(i, readerContext);
            creator.addDouble(value, dictionary.indexOf(value));
          }
          break;
        case BIG_DECIMAL:
          for (int i = 0; i < numDocs; i++) {
            BigDecimal value = forwardIndexReader.getBigDecimal(i, readerContext);
            creator.addBigDecimal(value, dictionary.indexOf(value));
          }
          break;
        case STRING:
          for (int i = 0; i < numDocs; i++) {
            String value = forwardIndexReader.getString(i, readerContext);
            creator.addString(value, dictionary.indexOf(value));
          }
          break;
        case BYTES:
          for (int i = 0; i < numDocs; i++) {
            byte[] value = forwardIndexReader.getBytes(i, readerContext);
            creator.addBytes(value, dictionary.indexOf(new ByteArray(value)));
          }
          break;
        default:
          throw new IllegalStateException("Unsupported SV data type: " + dataType);
      }
    } else {
      switch (storedType) {
        case INT:
          for (int i = 0; i < numDocs; i++) {
            int[] values = forwardIndexReader.getIntMV(i, readerContext);
            creator.addIntMV(values, lookupDictIds(dictionary, values));
          }
          break;
        case LONG:
          for (int i = 0; i < numDocs; i++) {
            long[] values = forwardIndexReader.getLongMV(i, readerContext);
            creator.addLongMV(values, lookupDictIds(dictionary, values));
          }
          break;
        case FLOAT:
          for (int i = 0; i < numDocs; i++) {
            float[] values = forwardIndexReader.getFloatMV(i, readerContext);
            creator.addFloatMV(values, lookupDictIds(dictionary, values));
          }
          break;
        case DOUBLE:
          for (int i = 0; i < numDocs; i++) {
            double[] values = forwardIndexReader.getDoubleMV(i, readerContext);
            creator.addDoubleMV(values, lookupDictIds(dictionary, values));
          }
          break;
        case BIG_DECIMAL:
          for (int i = 0; i < numDocs; i++) {
            BigDecimal[] values = forwardIndexReader.getBigDecimalMV(i, readerContext);
            creator.addBigDecimalMV(values, lookupDictIds(dictionary, values));
          }
          break;
        case STRING:
          for (int i = 0; i < numDocs; i++) {
            String[] values = forwardIndexReader.getStringMV(i, readerContext);
            creator.addStringMV(values, lookupDictIds(dictionary, values));
          }
          break;
        case BYTES:
          for (int i = 0; i < numDocs; i++) {
            byte[][] values = forwardIndexReader.getBytesMV(i, readerContext);
            creator.addBytesMV(values, lookupDictIds(dictionary, values));
          }
          break;
        default:
          throw new IllegalStateException("Unsupported MV data type: " + dataType);
      }
    }
  }

  private static int[] lookupDictIds(Dictionary dictionary, int[] values) {
    int[] dictIds = new int[values.length];
    for (int j = 0; j < values.length; j++) {
      dictIds[j] = dictionary.indexOf(values[j]);
    }
    return dictIds;
  }

  private static int[] lookupDictIds(Dictionary dictionary, long[] values) {
    int[] dictIds = new int[values.length];
    for (int j = 0; j < values.length; j++) {
      dictIds[j] = dictionary.indexOf(values[j]);
    }
    return dictIds;
  }

  private static int[] lookupDictIds(Dictionary dictionary, float[] values) {
    int[] dictIds = new int[values.length];
    for (int j = 0; j < values.length; j++) {
      dictIds[j] = dictionary.indexOf(values[j]);
    }
    return dictIds;
  }

  private static int[] lookupDictIds(Dictionary dictionary, double[] values) {
    int[] dictIds = new int[values.length];
    for (int j = 0; j < values.length; j++) {
      dictIds[j] = dictionary.indexOf(values[j]);
    }
    return dictIds;
  }

  private static int[] lookupDictIds(Dictionary dictionary, BigDecimal[] values) {
    int[] dictIds = new int[values.length];
    for (int j = 0; j < values.length; j++) {
      dictIds[j] = dictionary.indexOf(values[j]);
    }
    return dictIds;
  }

  private static int[] lookupDictIds(Dictionary dictionary, String[] values) {
    int[] dictIds = new int[values.length];
    for (int j = 0; j < values.length; j++) {
      dictIds[j] = dictionary.indexOf(values[j]);
    }
    return dictIds;
  }

  private static int[] lookupDictIds(Dictionary dictionary, byte[][] values) {
    int[] dictIds = new int[values.length];
    for (int j = 0; j < values.length; j++) {
      dictIds[j] = dictionary.indexOf(new ByteArray(values[j]));
    }
    return dictIds;
  }
}
