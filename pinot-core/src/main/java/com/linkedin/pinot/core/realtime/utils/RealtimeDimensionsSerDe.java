/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.realtime.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionary;


public class RealtimeDimensionsSerDe {

  private final List<String> dimensionsList;
  private final Schema dataSchema;
  private final Map<String, MutableDictionary> dictionaryMap;

  public RealtimeDimensionsSerDe(List<String> dimensionName, Schema schema,
      Map<String, MutableDictionary> dictionary) {
    this.dimensionsList = dimensionName;
    this.dataSchema = schema;
    this.dictionaryMap = dictionary;
  }

  public ByteBuffer serialize(GenericRow row) {
    List<Integer> rowConvertedToDictionaryId = new LinkedList<Integer>();
    List<Integer> columnOffsets = new LinkedList<Integer>();
    int pointer = 0;

    for (int i = 0; i < dataSchema.getDimensionNames().size(); i++) {
      columnOffsets.add(pointer);

      if (dataSchema.getFieldSpecFor(dataSchema.getDimensionNames().get(i)).isSingleValueField()) {
        rowConvertedToDictionaryId.add(dictionaryMap.get(dataSchema.getDimensionNames().get(i))
            .indexOf(row.getValue(dataSchema.getDimensionNames().get(i))));
        pointer += 1;
      } else {
        Object[] multivalues = (Object[]) row.getValue(dataSchema.getDimensionNames().get(i));
        if (multivalues != null && multivalues.length > 0) {
          Arrays.sort(multivalues);
          for (Object multivalue : multivalues) {
            rowConvertedToDictionaryId.add(
                dictionaryMap.get(dataSchema.getDimensionNames().get(i)).indexOf(multivalue));
          }
          pointer += multivalues.length;
        } else {
          rowConvertedToDictionaryId.add(0);
          pointer += 1;
        }
      }
      if (i == dataSchema.getDimensionNames().size() - 1) {
        columnOffsets.add(pointer);
      }
    }

    ByteBuffer buff = ByteBuffer.allocate((columnOffsets.size() + rowConvertedToDictionaryId.size()) * 4);
    for (Integer offset : columnOffsets) {
      buff.putInt(offset + columnOffsets.size());
    }
    for (Integer dicId : rowConvertedToDictionaryId) {
      buff.putInt(dicId);
    }
    return buff;
  }

  public int[] deSerializeAndReturnDicIdsFor(String column, ByteBuffer buffer) {
    int ret[] = null;
    int dimIndex = dataSchema.getDimensionNames().indexOf(column);
    int start = buffer.getInt(dimIndex * 4);
    int end = buffer.getInt((dimIndex + 1) * 4);
    ret = new int[end - start];
    int counter = 0;
    for (int i = start; i < end; i++) {
      ret[counter] = buffer.getInt(i * 4);
      counter++;
    }
    return ret;
  }

  public GenericRow deSerialize(ByteBuffer buffer) {
    GenericRow row = new GenericRow();
    Map<String, Object> rowValues = new HashMap<String, Object>();

    for (String dimension : dataSchema.getDimensionNames()) {
      int[] ret = deSerializeAndReturnDicIdsFor(dimension, buffer);

      if (dataSchema.getFieldSpecFor(dimension).isSingleValueField()) {
        rowValues.put(dimension, dictionaryMap.get(dimension).get(ret[0]));
      } else {
        Object[] mV = new Object[ret.length];
        for (int i = 0; i < ret.length; i++) {
          mV[i] = dictionaryMap.get(dimension).get(ret[i]);
        }
        rowValues.put(dimension, mV);
      }
    }

    row.init(rowValues);

    return row;
  }
}
