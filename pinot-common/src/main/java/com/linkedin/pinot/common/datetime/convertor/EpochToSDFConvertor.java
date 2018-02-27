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
package com.linkedin.pinot.common.datetime.convertor;

import com.linkedin.pinot.common.data.DateTimeFormatSpec;
import com.linkedin.pinot.common.data.DateTimeGranularitySpec;

/**
 * Convertor to convert a datetime value from an epoch format to an sdf format
 */
public class EpochToSDFConvertor extends DateTimeConvertor {

  public EpochToSDFConvertor(DateTimeFormatSpec inputFormat, DateTimeFormatSpec outputFormat,
      DateTimeGranularitySpec outputGranularity) {
    super(inputFormat, outputFormat, outputGranularity);
  }

  @Override
  public Long convert(Object dateTimeValue) {
    Long dateTimeColumnValueMS = convertEpochToMillis(dateTimeValue);
    Long dateTimeValueConverted = convertMillisToSDF(dateTimeColumnValueMS);
    return dateTimeValueConverted;
  }

}
