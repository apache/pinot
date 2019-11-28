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
package org.apache.pinot.core.operator.transform.transformer.datetime;

import org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;


public class DateTimeTransformerFactory {
  private DateTimeTransformerFactory() {
  }

  public static BaseDateTimeTransformer getDateTimeTransformer(String inputFormatStr, String outputFormatStr,
      String outputGranularityStr) {
    DateTimeFormatSpec inputFormat = new DateTimeFormatSpec(inputFormatStr);
    DateTimeFormatSpec outputFormat = new DateTimeFormatSpec(outputFormatStr);
    DateTimeGranularitySpec outputGranularity = new DateTimeGranularitySpec(outputGranularityStr);

    TimeFormat inputTimeFormat = inputFormat.getTimeFormat();
    TimeFormat outputTimeFormat = outputFormat.getTimeFormat();
    if (inputTimeFormat == TimeFormat.EPOCH) {
      if (outputTimeFormat == TimeFormat.EPOCH) {
        return new EpochToEpochTransformer(inputFormat, outputFormat, outputGranularity);
      } else {
        return new EpochToSDFTransformer(inputFormat, outputFormat, outputGranularity);
      }
    } else {
      if (outputTimeFormat == TimeFormat.EPOCH) {
        return new SDFToEpochTransformer(inputFormat, outputFormat, outputGranularity);
      } else {
        return new SDFToSDFTransformer(inputFormat, outputFormat, outputGranularity);
      }
    }
  }
}
