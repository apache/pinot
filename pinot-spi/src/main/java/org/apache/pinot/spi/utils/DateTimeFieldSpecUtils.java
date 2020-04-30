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
package org.apache.pinot.spi.utils;

import com.google.common.base.Preconditions;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;


public final class DateTimeFieldSpecUtils {

  private DateTimeFieldSpecUtils() {

  }

  /**
   * Helper method that converts a {@link TimeFieldSpec} to {@link DateTimeFieldSpec}
   * 1) If timeFieldSpec contains only incoming granularity spec, directly convert it to a dateTimeFieldSpec
   * 2) If timeFieldSpec contains incoming aas well as outgoing granularity spec, use the outgoing spec to construct the dateTimeFieldSpec,
   *    and configure a transform function for the conversion from incoming
   */
  public static DateTimeFieldSpec convertToDateTimeFieldSpec(TimeFieldSpec timeFieldSpec) {
    DateTimeFieldSpec dateTimeFieldSpec = new DateTimeFieldSpec();
    TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
    TimeGranularitySpec outgoingGranularitySpec = timeFieldSpec.getOutgoingGranularitySpec();

    dateTimeFieldSpec.setName(outgoingGranularitySpec.getName());
    dateTimeFieldSpec.setDataType(outgoingGranularitySpec.getDataType());

    int outgoingTimeSize = outgoingGranularitySpec.getTimeUnitSize();
    TimeUnit outgoingTimeUnit = outgoingGranularitySpec.getTimeType();
    String outgoingTimeFormat = outgoingGranularitySpec.getTimeFormat();
    String[] split = outgoingTimeFormat.split(DateTimeFormatSpec.COLON_SEPARATOR);
    DateTimeFormatSpec formatSpec;
    if (split[0].equals(TimeFormat.EPOCH.toString())) {
      formatSpec = new DateTimeFormatSpec(outgoingTimeSize, outgoingTimeUnit.toString(), split[0]);
    } else {
      formatSpec = new DateTimeFormatSpec(outgoingTimeSize, outgoingTimeUnit.toString(), split[0], split[1]);
    }
    dateTimeFieldSpec.setFormat(formatSpec.getFormat());
    DateTimeGranularitySpec granularitySpec = new DateTimeGranularitySpec(outgoingTimeSize, outgoingTimeUnit);
    dateTimeFieldSpec.setGranularity(granularitySpec.getGranularity());

    if (timeFieldSpec.getTransformFunction() != null) {
      dateTimeFieldSpec.setTransformFunction(timeFieldSpec.getTransformFunction());
    } else if (!incomingGranularitySpec.equals(outgoingGranularitySpec)) {
      String incomingName = incomingGranularitySpec.getName();
      int incomingTimeSize = incomingGranularitySpec.getTimeUnitSize();
      TimeUnit incomingTimeUnit = incomingGranularitySpec.getTimeType();
      String incomingTimeFormat = incomingGranularitySpec.getTimeFormat();
      Preconditions.checkState(incomingTimeFormat.equals(TimeFormat.EPOCH.toString()) && outgoingTimeFormat
              .equals(TimeFormat.EPOCH.toString()),
          "Conversion from incoming to outgoing is not supported for SIMPLE_DATE_FORMAT");
      String transformFunction =
          getTransformFunction(incomingName, incomingTimeSize, incomingTimeUnit, outgoingTimeSize, outgoingTimeUnit);
      dateTimeFieldSpec.setTransformFunction(transformFunction);
    }

    dateTimeFieldSpec.setMaxLength(timeFieldSpec.getMaxLength());
    dateTimeFieldSpec.setDefaultNullValue(timeFieldSpec.getDefaultNullValue());

    return dateTimeFieldSpec;
  }

  private static String getTransformFunction(String incomingName, int incomingTimeSize, TimeUnit incomingTimeUnit,
      int outgoingTimeSize, TimeUnit outgoingTimeUnit) {

    String innerFunction = incomingName;
    switch (incomingTimeUnit) {

      case MILLISECONDS:
        // do nothing
        break;
      case SECONDS:
        if (incomingTimeSize > 1) {
          innerFunction = String.format("fromEpochSeconds(%s, %d)", incomingName, incomingTimeSize);
        } else {
          innerFunction = String.format("fromEpochSeconds(%s)", incomingName);
        }
        break;
      case MINUTES:
        if (incomingTimeSize > 1) {
          innerFunction = String.format("fromEpochMinutes(%s, %d)", incomingName, incomingTimeSize);
        } else {
          innerFunction = String.format("fromEpochMinutes(%s)", incomingName);
        }
        break;
      case HOURS:
        if (incomingTimeSize > 1) {
          innerFunction = String.format("fromEpochHours(%s, %d)", incomingName, incomingTimeSize);
        } else {
          innerFunction = String.format("fromEpochHours(%s)", incomingName);
        }
        break;
      case DAYS:
        if (incomingTimeSize > 1) {
          innerFunction = String.format("fromEpochDays(%s, %d)", incomingName, incomingTimeSize);
        } else {
          innerFunction = String.format("fromEpochDays(%s)", incomingName);
        }
        break;
    }

    String outerFunction = null;
    switch (outgoingTimeUnit) {

      case MILLISECONDS:
        break;
      case SECONDS:
        if (outgoingTimeSize > 1) {
          outerFunction = String.format("toEpochSeconds(%s, %d)", innerFunction, outgoingTimeSize);
        } else {
          outerFunction = String.format("toEpochSeconds(%s)", innerFunction);
        }
        break;
      case MINUTES:
        if (outgoingTimeSize > 1) {
          outerFunction = String.format("toEpochMinutes(%s, %d)", innerFunction, outgoingTimeSize);
        } else {
          outerFunction = String.format("toEpochMinutes(%s)", innerFunction);
        }
        break;
      case HOURS:
        if (outgoingTimeSize > 1) {
          outerFunction = String.format("toEpochHours(%s, %d)", innerFunction, outgoingTimeSize);
        } else {
          outerFunction = String.format("toEpochHours(%s)", innerFunction);
        }
        break;
      case DAYS:
        if (outgoingTimeSize > 1) {
          outerFunction = String.format("toEpochDays(%s, %d)", innerFunction, outgoingTimeSize);
        } else {
          outerFunction = String.format("toEpochDays(%s)", innerFunction);
        }
        break;
    }
    return outerFunction;
  }
}
