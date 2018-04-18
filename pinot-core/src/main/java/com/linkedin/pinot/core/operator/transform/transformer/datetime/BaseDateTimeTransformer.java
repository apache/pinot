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
package com.linkedin.pinot.core.operator.transform.transformer.datetime;

import com.linkedin.pinot.common.data.DateTimeFormatSpec;
import com.linkedin.pinot.common.data.DateTimeFormatUnitSpec.DateTimeTransformUnit;
import com.linkedin.pinot.common.data.DateTimeGranularitySpec;
import com.linkedin.pinot.core.operator.transform.transformer.DataTransformer;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.joda.time.format.DateTimeFormatter;


/**
 * Base date time transformer to transform and bucket date time values from epoch/simple date format to epoch/simple
 * date format.
 * <p>NOTE: time size and time unit do not apply to simple date format.
 */
public abstract class BaseDateTimeTransformer<I, O> implements DataTransformer<I, O> {
  private final int _inputTimeSize;
  private final TimeUnit _inputTimeUnit;
  private final DateTimeFormatter _inputDateTimeFormatter;
  private final int _outputTimeSize;
  private final DateTimeTransformUnit _outputTimeUnit;
  private final DateTimeFormatter _outputDateTimeFormatter;
  private final long _outputGranularityMillis;

  public BaseDateTimeTransformer(@Nonnull DateTimeFormatSpec inputFormat, @Nonnull DateTimeFormatSpec outputFormat,
      @Nonnull DateTimeGranularitySpec outputGranularity) {
    _inputTimeSize = inputFormat.getColumnSize();
    _inputTimeUnit = inputFormat.getColumnUnit();
    _inputDateTimeFormatter = inputFormat.getDateTimeFormatter();
    _outputTimeSize = outputFormat.getColumnSize();
    _outputTimeUnit = outputFormat.getColumnDateTimeTransformUnit();
    _outputDateTimeFormatter = outputFormat.getDateTimeFormatter();
    _outputGranularityMillis = outputGranularity.granularityToMillis();
  }

  protected long transformEpochToMillis(long epochTime) {
    return _inputTimeUnit.toMillis(epochTime * _inputTimeSize);
  }

  protected long transformSDFToMillis(@Nonnull String sdfTime) {
    return _inputDateTimeFormatter.parseMillis(sdfTime);
  }

  protected long transformMillisToEpoch(long millisSinceEpoch) {
    return _outputTimeUnit.fromMillis(millisSinceEpoch) / _outputTimeSize;
  }

  protected String transformMillisToSDF(long millisSinceEpoch) {
    return _outputDateTimeFormatter.print(transformToOutputGranularity(millisSinceEpoch));
  }

  protected long transformToOutputGranularity(long millisSinceEpoch) {
    return (millisSinceEpoch / _outputGranularityMillis) * _outputGranularityMillis;
  }
}
