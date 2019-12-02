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

import javax.annotation.Nonnull;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;


/**
 * Date time transformer to transform and bucket date time values from epoch format to simple date format.
 */
public class EpochToSDFTransformer extends BaseDateTimeTransformer<long[], String[]> {

  public EpochToSDFTransformer(DateTimeFormatSpec inputFormat, DateTimeFormatSpec outputFormat,
      DateTimeGranularitySpec outputGranularity) {
    super(inputFormat, outputFormat, outputGranularity);
  }

  @Override
  public void transform(@Nonnull long[] input, @Nonnull String[] output, int length) {
    for (int i = 0; i < length; i++) {
      // NOTE: No need to bucket time because it's implicit in the output simple date format
      output[i] = transformMillisToSDF(transformEpochToMillis(input[i]));
    }
  }
}
