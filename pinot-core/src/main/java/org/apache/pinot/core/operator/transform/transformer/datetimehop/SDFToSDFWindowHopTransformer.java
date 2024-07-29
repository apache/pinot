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

import java.util.List;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;


public class SDFToSDFWindowHopTransformer extends BaseDateTimeWindowHopTransformer<String[], String[][]> {

  public SDFToSDFWindowHopTransformer(DateTimeFormatSpec inputFormat, DateTimeFormatSpec outputFormat,
      DateTimeGranularitySpec outputGranularity, DateTimeGranularitySpec hopWindowSize) {
    super(inputFormat, outputFormat, outputGranularity, hopWindowSize);
  }

  @Override
  public void transform(String[] input, String[][] output, int length) {
    for (int i = 0; i < length; i++) {
      String sdfTime = input[i];
      long millisSinceEpoch = transformSDFToMillis(sdfTime);
      List<Long> hopWindows = hopWindows(millisSinceEpoch);

      String[] transformedStrings = new String[hopWindows.size()];
      for (int j = 0; j < hopWindows.size(); j++) {
        transformedStrings[j] = transformMillisToSDF(hopWindows.get(j));
      }
      output[i] = transformedStrings;
    }
  }
}
