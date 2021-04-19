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

package org.apache.pinot.controller.recommender.data.generator;

import com.google.common.annotations.VisibleForTesting;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * A class to generate data for a time column
 */
public class TimeGenerator implements Generator {
  private static final double DEFAULT_NUMBER_OF_VALUES_PER_ENTRY = 1;

  private final NumberGenerator _numberGenerator;
  private final FieldSpec.DataType _dataType;
  private final Number _initialValue;

  public TimeGenerator(Integer cardinality, FieldSpec.DataType dataType, TimeUnit timeUnit) {
    _numberGenerator = new NumberGenerator(cardinality, dataType, DEFAULT_NUMBER_OF_VALUES_PER_ENTRY);
    _dataType = dataType;
    Date now = new Date();
    _initialValue = convert(now, timeUnit, dataType);
  }

  @Override
  public void init() {
    _numberGenerator.init();
  }

  @Override
  public Object next() {
    Object next = _numberGenerator.next();
    if (_dataType == FieldSpec.DataType.LONG) {
      return ((long) next) + _initialValue.longValue();
    }
    return ((int) next) + _initialValue.intValue();
  }

  @VisibleForTesting
  static Number convert(Date date, TimeUnit timeUnit, FieldSpec.DataType dataType) {
    long convertedTime = timeUnit.convert(date.getTime(), TimeUnit.MILLISECONDS);
    if (dataType == FieldSpec.DataType.LONG) {
      return convertedTime;
    }
    if (dataType == FieldSpec.DataType.INT) {
      return (int) convertedTime;
    }
    throw new IllegalArgumentException("Time column can be only INT or LONG: " + dataType);
  }
}
