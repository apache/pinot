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
package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;
import org.roaringbitmap.RoaringBitmap;


public class ExtractTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "extract";
  private TransformFunction _mainTransformFunction;
  protected Field _field;
  protected Chronology _chronology = ISOChronology.getInstanceUTC();

  private enum Field {
    YEAR, MONTH, DAY, DOW, HOUR, MINUTE, SECOND, QUARTER
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    if (arguments.size() != 2) {
      throw new IllegalArgumentException("Exactly 2 arguments are required for EXTRACT transform function");
    }

    _field = Field.valueOf(((LiteralTransformFunction) arguments.get(0)).getStringLiteral());
    _mainTransformFunction = arguments.get(1);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return INT_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initIntValuesSV(numDocs);
    long[] timestamps = _mainTransformFunction.transformToLongValuesSV(valueBlock);
    convert(timestamps, numDocs, _intValuesSV);
    return _intValuesSV;
  }

  private void convert(long[] timestamps, int numDocs, int[] output) {
    for (int i = 0; i < numDocs; i++) {
      DateTimeField accessor;
      switch (_field) {
        case YEAR:
          accessor = _chronology.year();
          output[i] = accessor.get(timestamps[i]);
          break;
        case QUARTER:
          accessor = _chronology.monthOfYear();
          output[i] = (accessor.get(timestamps[i]) - 1) / 3 + 1;
          break;
        case MONTH:
          accessor = _chronology.monthOfYear();
          output[i] = accessor.get(timestamps[i]);
          break;
        case DAY:
          accessor = _chronology.dayOfMonth();
          output[i] = accessor.get(timestamps[i]);
          break;
        case DOW:
          accessor = _chronology.dayOfWeek();
          output[i] = accessor.get(timestamps[i]);
          break;
        case HOUR:
          accessor = _chronology.hourOfDay();
          output[i] = accessor.get(timestamps[i]);
          break;
        case MINUTE:
          accessor = _chronology.minuteOfHour();
          output[i] = accessor.get(timestamps[i]);
          break;
        case SECOND:
          accessor = _chronology.secondOfMinute();
          output[i] = accessor.get(timestamps[i]);
          break;
        default:
          throw new IllegalArgumentException("Unsupported FIELD type");
      }
    }
  }

  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    return _mainTransformFunction.getNullBitmap(valueBlock);
  }
}
