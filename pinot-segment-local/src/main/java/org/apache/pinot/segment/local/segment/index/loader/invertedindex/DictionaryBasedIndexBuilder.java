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
  /// @throws IllegalStateException if the column data type is unsupported, or if the column is multi-value with a
  ///     data type that does not support MV (e.g. BIG_DECIMAL).
  @SuppressWarnings("rawtypes")
  public static void addRawValuesViaDictionary(DictionaryBasedInvertedIndexCreator creator,
      ForwardIndexReader forwardIndexReader, ForwardIndexReaderContext readerContext, Dictionary dictionary,
      ColumnMetadata columnMetadata, int numDocs) {
    String columnName = columnMetadata.getColumnName();
    boolean singleValue = columnMetadata.isSingleValue();
    switch (columnMetadata.getDataType().getStoredType()) {
      case INT:
        if (singleValue) {
          for (int i = 0; i < numDocs; i++) {
            int value = forwardIndexReader.getInt(i, readerContext);
            creator.addInt(value, dictionary.indexOf(value));
          }
        } else {
          for (int i = 0; i < numDocs; i++) {
            int[] values = forwardIndexReader.getIntMV(i, readerContext);
            creator.addIntMV(values, lookupDictIds(dictionary, values));
          }
        }
        return;
      case LONG:
        if (singleValue) {
          for (int i = 0; i < numDocs; i++) {
            long value = forwardIndexReader.getLong(i, readerContext);
            creator.addLong(value, dictionary.indexOf(value));
          }
        } else {
          for (int i = 0; i < numDocs; i++) {
            long[] values = forwardIndexReader.getLongMV(i, readerContext);
            creator.addLongMV(values, lookupDictIds(dictionary, values));
          }
        }
        return;
      case FLOAT:
        if (singleValue) {
          for (int i = 0; i < numDocs; i++) {
            float value = forwardIndexReader.getFloat(i, readerContext);
            creator.addFloat(value, dictionary.indexOf(value));
          }
        } else {
          for (int i = 0; i < numDocs; i++) {
            float[] values = forwardIndexReader.getFloatMV(i, readerContext);
            creator.addFloatMV(values, lookupDictIds(dictionary, values));
          }
        }
        return;
      case DOUBLE:
        if (singleValue) {
          for (int i = 0; i < numDocs; i++) {
            double value = forwardIndexReader.getDouble(i, readerContext);
            creator.addDouble(value, dictionary.indexOf(value));
          }
        } else {
          for (int i = 0; i < numDocs; i++) {
            double[] values = forwardIndexReader.getDoubleMV(i, readerContext);
            creator.addDoubleMV(values, lookupDictIds(dictionary, values));
          }
        }
        return;
      case BIG_DECIMAL:
        if (!singleValue) {
          throw new IllegalStateException(
              "Dictionary-based index over raw values not supported for multi-value BIG_DECIMAL column: " + columnName);
        }
        for (int i = 0; i < numDocs; i++) {
          BigDecimal value = forwardIndexReader.getBigDecimal(i, readerContext);
          creator.add(value, dictionary.indexOf(value));
        }
        return;
      case STRING:
        if (singleValue) {
          for (int i = 0; i < numDocs; i++) {
            String value = forwardIndexReader.getString(i, readerContext);
            creator.addString(value, dictionary.indexOf(value));
          }
        } else {
          for (int i = 0; i < numDocs; i++) {
            String[] values = forwardIndexReader.getStringMV(i, readerContext);
            creator.addStringMV(values, lookupDictIds(dictionary, values));
          }
        }
        return;
      case BYTES:
        if (singleValue) {
          for (int i = 0; i < numDocs; i++) {
            byte[] value = forwardIndexReader.getBytes(i, readerContext);
            creator.addBytes(value, dictionary.indexOf(new ByteArray(value)));
          }
        } else {
          for (int i = 0; i < numDocs; i++) {
            byte[][] values = forwardIndexReader.getBytesMV(i, readerContext);
            int[] dictIds = new int[values.length];
            for (int j = 0; j < values.length; j++) {
              dictIds[j] = dictionary.indexOf(new ByteArray(values[j]));
            }
            creator.addBytesMV(values, dictIds);
          }
        }
        return;
      default:
        throw new IllegalStateException(
            "Unsupported data type for dictionary-based index over raw values: " + columnMetadata.getDataType()
                + " (column: " + columnName + ")");
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

  private static int[] lookupDictIds(Dictionary dictionary, String[] values) {
    int[] dictIds = new int[values.length];
    for (int j = 0; j < values.length; j++) {
      dictIds[j] = dictionary.indexOf(values[j]);
    }
    return dictIds;
  }
}
