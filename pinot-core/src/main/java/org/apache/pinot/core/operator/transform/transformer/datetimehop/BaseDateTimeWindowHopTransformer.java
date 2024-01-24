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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.operator.transform.transformer.DataTransformer;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeFormatUnitSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;


public abstract class BaseDateTimeWindowHopTransformer<I, O> implements DataTransformer<I, O> {
  protected final long _hopWindowSizeMillis;
  private final int _inputTimeSize;
  private final TimeUnit _inputTimeUnit;
  private final DateTimeFormatter _inputDateTimeFormatter;
  private final int _outputTimeSize;
  private final DateTimeFormatUnitSpec.DateTimeTransformUnit _outputTimeUnit;
  private final DateTimeFormatter _outputDateTimeFormatter;
  private final long _outputGranularityMillis;

  public BaseDateTimeWindowHopTransformer(DateTimeFormatSpec inputFormat, DateTimeFormatSpec outputFormat,
      DateTimeGranularitySpec outputGranularity, DateTimeGranularitySpec hopWindowSize) {
    _inputTimeSize = inputFormat.getColumnSize();
    _inputTimeUnit = inputFormat.getColumnUnit();
    _inputDateTimeFormatter = inputFormat.getDateTimeFormatter();
    _outputTimeSize = outputFormat.getColumnSize();
    _outputTimeUnit = outputFormat.getColumnDateTimeTransformUnit();
    _outputDateTimeFormatter = outputFormat.getDateTimeFormatter();
    _outputGranularityMillis = outputGranularity.granularityToMillis();
    _hopWindowSizeMillis = hopWindowSize.granularityToMillis();
  }

  protected long transformEpochToMillis(long epochTime) {
    return _inputTimeUnit.toMillis(epochTime * _inputTimeSize);
  }

  protected long transformSDFToMillis(String sdfTime) {
    return _inputDateTimeFormatter.parseMillis(sdfTime);
  }

  protected long transformMillisToEpoch(long millisSinceEpoch) {
    return _outputTimeUnit.fromMillis(millisSinceEpoch) / _outputTimeSize;
  }

  protected String transformMillisToSDF(long millisSinceEpoch) {
    return _outputDateTimeFormatter.print(new DateTime(millisSinceEpoch));
  }

  protected long transformToOutputGranularity(long millisSinceEpoch) {
    return (millisSinceEpoch / _outputGranularityMillis) * _outputGranularityMillis;
  }

  protected List<Long> hopWindows(long millisSinceEpoch) {
    List<Long> hops = new ArrayList<>();
    long totalHopMillis = _hopWindowSizeMillis;
    long granularityMillis = _outputGranularityMillis;

    long adjustedMillis = (millisSinceEpoch / granularityMillis) * granularityMillis;

    // Start from the adjusted timestamp and decrement by the hop until we've covered the entire window duration
    for (long currentMillis = adjustedMillis; currentMillis > millisSinceEpoch - totalHopMillis;
        currentMillis -= granularityMillis) {
      hops.add(currentMillis);
    }
    return hops;
  }
}
