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

import java.util.Date;
import java.util.Random;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;


public class DateTimeGenerator implements Generator {

  private static final int MULTIPLIER_CARDINALITY = 5;
  private final DateTimeFormatSpec _formatSpec;
  private final DateTimeGranularitySpec _granularitySpec;
  private long _currentValue;
  private Random _multiplier = new Random();

  public DateTimeGenerator(String format, String granularity) {
    _formatSpec = new DateTimeFormatSpec(format);
    _granularitySpec = new DateTimeGranularitySpec(granularity);
  }

  @Override
  public void init() {
    _currentValue = new Date().getTime();
  }

  @Override
  public Object next() {
    _currentValue += _granularitySpec.granularityToMillis() * _multiplier.nextInt(MULTIPLIER_CARDINALITY);
    return _formatSpec.fromMillisToFormat(_currentValue);
  }
}
