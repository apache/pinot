/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.startree.v2;

import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.io.BufferedOutputStream;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.startree.DimensionBuffer;
import com.linkedin.pinot.core.data.readers.PinotSegmentColumnReader;


public class StarTreeV2BaseClass {

  protected int _dimensionSize;
  protected int _dimensionsCount;
  protected BufferedOutputStream _outputStream;

  /**
   * enumerate dimension set.
   */
  protected List<Integer> enumerateDimensions(List<String> dimensionNames, List<String> dimensionsOrder) {
    List<Integer> enumeratedDimensions = new ArrayList<>();
    if (dimensionsOrder != null) {
      for (String dimensionName : dimensionsOrder) {
        enumeratedDimensions.add(dimensionNames.indexOf(dimensionName));
      }
    }

    return enumeratedDimensions;
  }

  /**
   * compute a defualt split order.
   */
  protected List<Integer> computeDefaultSplitOrder(List<Integer> dimensionCardinality) {
    List<Integer> defaultSplitOrder = new ArrayList<>();
    for (int i = 0; i < _dimensionsCount; i++) {
      defaultSplitOrder.add(i);
    }

    Collections.sort(defaultSplitOrder, new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return dimensionCardinality.get(o2) - dimensionCardinality.get(o1);
      }
    });

    return defaultSplitOrder;
  }

  /**
   * compute a defualt split order.
   */
  protected Object readHelper(PinotSegmentColumnReader reader, FieldSpec.DataType dataType, int docId) {
    switch (dataType) {
      case INT:
        return reader.readInt(docId);
      case FLOAT:
        return reader.readFloat(docId);
      case LONG:
        return reader.readLong(docId);
      case DOUBLE:
        return reader.readDouble(docId);
      case STRING:
        return reader.readString(docId);
    }

    return null;
  }

  /**
   * compute a defualt split order.
   */
  protected void appendToRawBuffer(DimensionBuffer dimensions, AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer) throws IOException {
    appendToBuffer(dimensions, aggregationFunctionColumnPairBuffer);
  }

  /**
   * compute a defualt split order.
   */
  protected void appendToAggBuffer(DimensionBuffer dimensions, AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer) throws IOException {
    appendToBuffer(dimensions, aggregationFunctionColumnPairBuffer);
  }

  /**
   * compute a defualt split order.
   */
  protected void appendToBuffer(DimensionBuffer dimensions, AggregationFunctionColumnPairBuffer aggregationFunctionColumnPairBuffer) throws IOException {
    _outputStream.write(dimensions.toBytes(), 0, _dimensionSize);
    _outputStream.write(aggregationFunctionColumnPairBuffer.toBytes(), 0, aggregationFunctionColumnPairBuffer._totalBytes);
  }

}
