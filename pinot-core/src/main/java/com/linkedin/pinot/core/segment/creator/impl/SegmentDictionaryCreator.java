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
package com.linkedin.pinot.core.segment.creator.impl;

import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.index.writer.impl.FixedByteWidthRowColDataFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentDictionaryCreator implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDictionaryCreator.class);
  private final Object[] sortedList;
  private final FieldSpec spec;
  private final File dictionaryFile;

  private Int2IntOpenHashMap intValueToIndexMap;
  private Long2IntOpenHashMap longValueToIndexMap;
  private Float2IntOpenHashMap floatValueToIndexMap;
  private Double2IntOpenHashMap doubleValueToIndexMap;
  private Object2IntOpenHashMap<String> stringValueToIndexMap;

  private int stringColumnMaxLength = 0;

  public SegmentDictionaryCreator(boolean hasNulls, Object[] sortedList, FieldSpec spec, File indexDir)
      throws IOException {
    LOGGER.info(
        "Creating segment for column {}, hasNulls = {}, cardinality = {}, dataType = {}, single value field = {}",
        spec.getName(), hasNulls, sortedList.length, spec.getDataType(), spec.isSingleValueField());
    this.sortedList = sortedList;
    this.spec = spec;
    dictionaryFile = new File(indexDir, spec.getName() + ".dict");
    FileUtils.touch(dictionaryFile);
  }

  @Override
  public void close() throws IOException {
  }

  public void build() throws Exception {
    switch (spec.getDataType()) {
      case INT:
        final FixedByteWidthRowColDataFileWriter intDictionaryWrite =
            new FixedByteWidthRowColDataFileWriter(dictionaryFile, sortedList.length, 1,
                V1Constants.Dict.INT_DICTIONARY_COL_SIZE);
        intValueToIndexMap = new Int2IntOpenHashMap(sortedList.length);
        for (int i = 0; i < sortedList.length; i++) {
          final int entry = ((Number) sortedList[i]).intValue();
          intDictionaryWrite.setInt(i, 0, entry);
          intValueToIndexMap.put(entry, i);
        }
        intDictionaryWrite.close();
        break;
      case FLOAT:
        final FixedByteWidthRowColDataFileWriter floatDictionaryWrite =
            new FixedByteWidthRowColDataFileWriter(dictionaryFile, sortedList.length, 1,
                V1Constants.Dict.FLOAT_DICTIONARY_COL_SIZE);
        floatValueToIndexMap = new Float2IntOpenHashMap(sortedList.length);
        for (int i = 0; i < sortedList.length; i++) {
          final float entry = ((Number) sortedList[i]).floatValue();
          floatDictionaryWrite.setFloat(i, 0, entry);
          floatValueToIndexMap.put(entry, i);
        }
        floatDictionaryWrite.close();
        break;
      case LONG:
        final FixedByteWidthRowColDataFileWriter longDictionaryWrite =
            new FixedByteWidthRowColDataFileWriter(dictionaryFile, sortedList.length, 1,
                V1Constants.Dict.LONG_DICTIONARY_COL_SIZE);
        longValueToIndexMap = new Long2IntOpenHashMap(sortedList.length);
        for (int i = 0; i < sortedList.length; i++) {
          final long entry = ((Number) sortedList[i]).longValue();
          longDictionaryWrite.setLong(i, 0, entry);
          longValueToIndexMap.put(entry, i);
        }
        longDictionaryWrite.close();
        break;
      case DOUBLE:
        final FixedByteWidthRowColDataFileWriter doubleDictionaryWrite =
            new FixedByteWidthRowColDataFileWriter(dictionaryFile, sortedList.length, 1,
                V1Constants.Dict.DOUBLE_DICTIONARY_COL_SIZE);
        doubleValueToIndexMap = new Double2IntOpenHashMap(sortedList.length);
        for (int i = 0; i < sortedList.length; i++) {
          final double entry = ((Number) sortedList[i]).doubleValue();
          doubleDictionaryWrite.setDouble(i, 0, entry);
          doubleValueToIndexMap.put(entry, i);
        }
        doubleDictionaryWrite.close();
        break;
      case STRING:
      case BOOLEAN:
        for (final Object e : sortedList) {
          String val = e.toString();
          int length = val.getBytes(Charset.forName("UTF-8")).length;
          if (stringColumnMaxLength < length) {
            stringColumnMaxLength = length;
          }
        }

        final FixedByteWidthRowColDataFileWriter stringDictionaryWrite =
            new FixedByteWidthRowColDataFileWriter(dictionaryFile, sortedList.length, 1,
                new int[] { stringColumnMaxLength });

        final String[] revised = new String[sortedList.length];
        for (int i = 0; i < sortedList.length; i++) {
          final String toWrite = sortedList[i].toString();
          final int padding = stringColumnMaxLength - toWrite.getBytes(Charset.forName("UTF-8")).length;

          final StringBuilder bld = new StringBuilder();
          bld.append(toWrite);
          for (int j = 0; j < padding; j++) {
            bld.append(V1Constants.Str.STRING_PAD_CHAR);
          }
          String entry = bld.toString();
          revised[i] = entry;
          assert (revised[i].getBytes(Charset.forName("UTF-8")).length == stringColumnMaxLength);
        }
        Arrays.sort(revised);

        stringValueToIndexMap = new Object2IntOpenHashMap<>(sortedList.length);
        for (int i = 0; i < revised.length; i++) {
          stringDictionaryWrite.setString(i, 0, revised[i]);
          stringValueToIndexMap.put(revised[i], i);
        }
        stringDictionaryWrite.close();
        break;
      default:
        break;
    }
  }

  public int getStringColumnMaxLength() {
    return stringColumnMaxLength;
  }

  public int indexOfSV(Object e) {
    switch (spec.getDataType()) {
      case INT:
        return intValueToIndexMap.get(e);
      case FLOAT:
        return floatValueToIndexMap.get(e);
      case DOUBLE:
        return doubleValueToIndexMap.get(e);
      case LONG:
        return longValueToIndexMap.get(e);
      case STRING:
      case BOOLEAN:
        final StringBuilder bld = new StringBuilder();
        bld.append(e.toString());
        for (int i = 0; i < (stringColumnMaxLength - ((String) e).getBytes(Charset.forName("UTF-8")).length); i++) {
          bld.append(V1Constants.Str.STRING_PAD_CHAR);
        }
        return stringValueToIndexMap.get(bld.toString());
      default:
        throw new UnsupportedOperationException("Unsupported data type : " + spec.getDataType() +
            " for column : " + spec.getName());
    }
  }

  public int[] indexOfMV(Object e) {

    final Object[] multiValues = (Object[]) e;
    final int[] ret = new int[multiValues.length];

    switch (spec.getDataType()) {
      case INT:
        for (int i = 0; i < multiValues.length; i++) {
          ret[i] = intValueToIndexMap.get(multiValues[i]);
        }
        break;
      case FLOAT:
        for (int i = 0; i < multiValues.length; i++) {
          ret[i] = floatValueToIndexMap.get(multiValues[i]);
        }
        break;
      case LONG:
        for (int i = 0; i < multiValues.length; i++) {
          ret[i] = longValueToIndexMap.get(multiValues[i]);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < multiValues.length; i++) {
          ret[i] = doubleValueToIndexMap.get(multiValues[i]);
        }
        break;
      case STRING:
      case BOOLEAN:
        for (int i = 0; i < multiValues.length; i++) {
          final StringBuilder bld = new StringBuilder();
          String value = multiValues[i].toString();
          bld.append(value);
          for (int j = 0; j < (stringColumnMaxLength - value.getBytes(Charset.forName("UTF-8")).length); j++) {
            bld.append(V1Constants.Str.STRING_PAD_CHAR);
          }
          ret[i] = stringValueToIndexMap.get(bld.toString());
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported data type : " + spec.getDataType() +
            " for multivalue column : " + spec.getName());
    }

    return ret;
  }
}
