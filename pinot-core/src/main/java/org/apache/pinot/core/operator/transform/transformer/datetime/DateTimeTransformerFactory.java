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

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.TimeZone;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.joda.time.DateTimeZone;


public class DateTimeTransformerFactory {
  private DateTimeTransformerFactory() {
  }

  public static BaseDateTimeTransformer getDateTimeTransformer(String inputFormatStr, String outputFormatStr,
      String outputGranularityStr) {
    return getDateTimeTransformer(inputFormatStr, outputFormatStr, outputGranularityStr, null);
  }

  public static BaseDateTimeTransformer getDateTimeTransformer(String inputFormatStr, String outputFormatStr,
      String outputGranularityStr, @Nullable String bucketTimeZoneStr) {

    DateTimeFormatSpec inputFormat = new DateTimeFormatSpec(inputFormatStr);
    DateTimeFormatSpec outputFormat = new DateTimeFormatSpec(outputFormatStr);
    DateTimeGranularitySpec outputGranularity = new DateTimeGranularitySpec(outputGranularityStr);
    DateTimeZone bucketingTz = null;
    if (bucketTimeZoneStr != null) {
      try {
        // we're not using TimeZone.getTimeZone() because it's globally synchronized and returns default TZ when str
        // makes no sense
        bucketingTz = DateTimeZone.forTimeZone(TimeZone.getTimeZone(ZoneId.of(bucketTimeZoneStr)));
      } catch (DateTimeException dte) {
        throw new IllegalArgumentException("Error parsing bucketing time zone: " + dte.getMessage(), dte);
      }
    }

    TimeFormat inputTimeFormat = inputFormat.getTimeFormat();
    TimeFormat outputTimeFormat = outputFormat.getTimeFormat();
    if (inputTimeFormat == TimeFormat.EPOCH || inputTimeFormat == TimeFormat.TIMESTAMP) {
      if (outputTimeFormat == TimeFormat.EPOCH || outputTimeFormat == TimeFormat.TIMESTAMP) {
        return new EpochToEpochTransformer(inputFormat, outputFormat, outputGranularity, bucketingTz);
      } else {
        return new EpochToSDFTransformer(inputFormat, outputFormat, outputGranularity, bucketingTz);
      }
    } else {
      if (outputTimeFormat == TimeFormat.EPOCH || outputTimeFormat == TimeFormat.TIMESTAMP) {
        return new SDFToEpochTransformer(inputFormat, outputFormat, outputGranularity, bucketingTz);
      } else {
        return new SDFToSDFTransformer(inputFormat, outputFormat, outputGranularity, bucketingTz);
      }
    }
  }
}
