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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.joda.time.Chronology;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.roaringbitmap.RoaringBitmap;


public abstract class DateTimeTransformFunction extends BaseTransformFunction {
  protected static final Chronology UTC = ISOChronology.getInstanceUTC();
  protected static final TransformResultMetadata METADATA =
      new TransformResultMetadata(FieldSpec.DataType.INT, true, false);

  protected final String _name;
  protected TransformFunction _timestampsFunction;
  protected Chronology _chronology;

  protected DateTimeTransformFunction(String name) {
    _name = name;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    Preconditions.checkArgument(!arguments.isEmpty() && arguments.size() <= 2, "%s takes one or two arguments", _name);
    _timestampsFunction = arguments.get(0);
    if (arguments.size() == 2) {
      Preconditions.checkArgument(arguments.get(1) instanceof LiteralTransformFunction,
          "zoneId parameter %s must be a literal", _name);
      _chronology = ISOChronology.getInstance(
          DateTimeZone.forID(((LiteralTransformFunction) arguments.get(1)).getStringLiteral()));
    } else {
      _chronology = UTC;
    }
  }

  @Override
  public String getName() {
    return _name;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int numDocs = valueBlock.getNumDocs();
    initIntValuesSV(numDocs);
    long[] timestamps = _timestampsFunction.transformToLongValuesSV(valueBlock);
    convert(timestamps, numDocs, _intValuesSV);
    return _intValuesSV;
  }

  protected abstract void convert(long[] timestamps, int numDocs, int[] output);

  public static final class Year extends DateTimeTransformFunction {

    public Year() {
      super(TransformFunctionType.YEAR.getName());
    }

    @Override
    protected void convert(long[] timestamps, int numDocs, int[] output) {
      DateTimeField accessor = _chronology.year();
      for (int i = 0; i < numDocs; i++) {
        output[i] = accessor.get(timestamps[i]);
      }
    }
  }

  public static final class YearOfWeek extends DateTimeTransformFunction {

    public YearOfWeek() {
      super(TransformFunctionType.YEAR_OF_WEEK.getName());
    }

    @Override
    protected void convert(long[] timestamps, int numDocs, int[] output) {
      DateTimeField accessor = _chronology.weekyear();
      for (int i = 0; i < numDocs; i++) {
        output[i] = accessor.get(timestamps[i]);
      }
    }
  }

  public static final class Month extends DateTimeTransformFunction {

    public Month() {
      super(TransformFunctionType.MONTH_OF_YEAR.getName());
    }

    @Override
    protected void convert(long[] timestamps, int numDocs, int[] output) {
      DateTimeField accessor = _chronology.monthOfYear();
      for (int i = 0; i < numDocs; i++) {
        output[i] = accessor.get(timestamps[i]);
      }
    }
  }

  public static final class WeekOfYear extends DateTimeTransformFunction {

    public WeekOfYear() {
      super(TransformFunctionType.WEEK_OF_YEAR.getName());
    }

    @Override
    protected void convert(long[] timestamps, int numDocs, int[] output) {
      DateTimeField accessor = _chronology.weekOfWeekyear();
      for (int i = 0; i < numDocs; i++) {
        output[i] = accessor.get(timestamps[i]);
      }
    }
  }

  public static final class DayOfYear extends DateTimeTransformFunction {

    public DayOfYear() {
      super(TransformFunctionType.DAY_OF_YEAR.getName());
    }

    @Override
    protected void convert(long[] timestamps, int numDocs, int[] output) {
      DateTimeField accessor = _chronology.dayOfYear();
      for (int i = 0; i < numDocs; i++) {
        output[i] = accessor.get(timestamps[i]);
      }
    }
  }

  public static final class DayOfMonth extends DateTimeTransformFunction {

    public DayOfMonth() {
      super(TransformFunctionType.DAY_OF_MONTH.getName());
    }

    @Override
    protected void convert(long[] timestamps, int numDocs, int[] output) {
      DateTimeField accessor = _chronology.dayOfMonth();
      for (int i = 0; i < numDocs; i++) {
        output[i] = accessor.get(timestamps[i]);
      }
    }
  }

  public static final class DayOfWeek extends DateTimeTransformFunction {

    public DayOfWeek() {
      super(TransformFunctionType.DAY_OF_WEEK.getName());
    }

    @Override
    protected void convert(long[] timestamps, int numDocs, int[] output) {
      DateTimeField accessor = _chronology.dayOfWeek();
      for (int i = 0; i < numDocs; i++) {
        output[i] = accessor.get(timestamps[i]);
      }
    }
  }

  public static final class Hour extends DateTimeTransformFunction {

    public Hour() {
      super(TransformFunctionType.HOUR.getName());
    }

    @Override
    protected void convert(long[] timestamps, int numDocs, int[] output) {
      DateTimeField accessor = _chronology.hourOfDay();
      for (int i = 0; i < numDocs; i++) {
        output[i] = accessor.get(timestamps[i]);
      }
    }
  }

  public static final class Minute extends DateTimeTransformFunction {

    public Minute() {
      super(TransformFunctionType.MINUTE.getName());
    }

    @Override
    protected void convert(long[] timestamps, int numDocs, int[] output) {
      DateTimeField accessor = _chronology.minuteOfHour();
      for (int i = 0; i < numDocs; i++) {
        output[i] = accessor.get(timestamps[i]);
      }
    }
  }

  public static final class Second extends DateTimeTransformFunction {

    public Second() {
      super(TransformFunctionType.SECOND.getName());
    }

    @Override
    protected void convert(long[] timestamps, int numDocs, int[] output) {
      DateTimeField accessor = _chronology.secondOfMinute();
      for (int i = 0; i < numDocs; i++) {
        output[i] = accessor.get(timestamps[i]);
      }
    }
  }

  public static final class Millisecond extends DateTimeTransformFunction {

    public Millisecond() {
      super(TransformFunctionType.MILLISECOND.getName());
    }

    @Override
    protected void convert(long[] timestamps, int numDocs, int[] output) {
      DateTimeField accessor = _chronology.millisOfSecond();
      for (int i = 0; i < numDocs; i++) {
        output[i] = accessor.get(timestamps[i]);
      }
    }
  }

  public static final class Quarter extends DateTimeTransformFunction {

    public Quarter() {
      super(TransformFunctionType.QUARTER.getName());
    }

    @Override
    protected void convert(long[] timestamps, int numDocs, int[] output) {
      DateTimeField accessor = _chronology.monthOfYear();
      for (int i = 0; i < numDocs; i++) {
        output[i] = (accessor.get(timestamps[i]) - 1) / 3 + 1;
      }
    }
  }

  @Override
  public RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    return _timestampsFunction.getNullBitmap(valueBlock);
  }
}
