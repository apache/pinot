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

package org.apache.pinot.core.operator.transform.transformer.datetimehop;

import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;


public class DateTimeWindowHopTransformerFactory {

  private static final TimeFormat EPOCH = DateTimeFieldSpec.TimeFormat.EPOCH;
  private static final TimeFormat TIMESTAMP = DateTimeFieldSpec.TimeFormat.TIMESTAMP;

  private DateTimeWindowHopTransformerFactory() {
  }

  public static BaseDateTimeWindowHopTransformer getDateTimeTransformer(String inputFormatStr, String outputFormatStr,
      String outputGranularityStr, String hopSizeStr) {
    DateTimeFormatSpec inputFormatSpec = new DateTimeFormatSpec(inputFormatStr);
    DateTimeFormatSpec outputFormatSpec = new DateTimeFormatSpec(outputFormatStr);
    DateTimeGranularitySpec outputGranularity = new DateTimeGranularitySpec(outputGranularityStr);
    DateTimeGranularitySpec hopSizeFormat = new DateTimeGranularitySpec(hopSizeStr);

    TimeFormat inputFormat = inputFormatSpec.getTimeFormat();
    TimeFormat outputFormat = outputFormatSpec.getTimeFormat();

    if (isEpochOrTimestamp(inputFormat) && isEpochOrTimestamp(outputFormat)) {
      return new EpochToEpochWindowHopTransformer(inputFormatSpec, outputFormatSpec, outputGranularity, hopSizeFormat);
    } else if (isEpochOrTimestamp(inputFormat) && isStringFormat(outputFormat)) {
      return new EpochToSDFHopWindowTransformer(inputFormatSpec, outputFormatSpec, outputGranularity, hopSizeFormat);
    } else if (isStringFormat(inputFormat) && isEpochOrTimestamp(outputFormat)) {
      return new SDFToEpochWindowHopTransformer(inputFormatSpec, outputFormatSpec, outputGranularity, hopSizeFormat);
    } else if (isStringFormat(inputFormat) && isStringFormat(outputFormat)) {
      return new SDFToSDFWindowHopTransformer(inputFormatSpec, outputFormatSpec, outputGranularity, hopSizeFormat);
    }
    throw new IllegalArgumentException("Wrong inputFormat: " + inputFormat + " outputFormat: " + outputFormat);
  }

  private static boolean isEpochOrTimestamp(TimeFormat format) {
    return format == EPOCH || format == TIMESTAMP;
  }

  private static boolean isStringFormat(TimeFormat format) {
    return format == TimeFormat.SIMPLE_DATE_FORMAT;
  }
}
