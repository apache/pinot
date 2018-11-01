/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.data.recordtransformer;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.common.utils.time.TimeConverter;
import com.linkedin.pinot.common.utils.time.TimeConverterProvider;
import com.linkedin.pinot.core.data.GenericRow;


/**
 * The {@code TimeTransformer} class will convert the time value based on the {@link TimeFieldSpec}.
 * <p>NOTE: should put this before the {@link DataTypeTransformer}. After this, time column can be treated as regular
 * column for other record transformers (incoming time column can be ignored).
 */
public class TimeTransformer implements RecordTransformer {
  private final String _incomingTimeColumn;
  private final String _outgoingTimeColumn;
  private final TimeConverter _timeConverter;

  public TimeTransformer(Schema schema) {
    TimeFieldSpec timeFieldSpec = schema.getTimeFieldSpec();
    if (timeFieldSpec != null) {
      TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
      TimeGranularitySpec outgoingGranularitySpec = timeFieldSpec.getOutgoingGranularitySpec();
      if (!incomingGranularitySpec.equals(outgoingGranularitySpec)) {
        _incomingTimeColumn = incomingGranularitySpec.getName();
        _outgoingTimeColumn = outgoingGranularitySpec.getName();
        _timeConverter = TimeConverterProvider.getTimeConverter(incomingGranularitySpec, outgoingGranularitySpec);
        return;
      }
    }
    _incomingTimeColumn = null;
    _outgoingTimeColumn = null;
    _timeConverter = null;
  }

  @Override
  public GenericRow transform(GenericRow record) {
    if (_timeConverter == null) {
      return record;
    }
    // Skip transformation if outgoing value already exist
    // NOTE: outgoing value might already exist for OFFLINE data
    if (record.getValue(_outgoingTimeColumn) == null) {
      record.putField(_outgoingTimeColumn, _timeConverter.convert(record.getValue(_incomingTimeColumn)));
    }
    return record;
  }
}
