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
package org.apache.pinot.core.data.recordtransformer;

import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.data.TimeGranularitySpec;
import org.apache.pinot.common.utils.time.TimeConverter;
import org.apache.pinot.common.utils.time.TimeUtils;
import org.apache.pinot.core.data.GenericRow;


/**
 * The {@code TimeTransformer} class will convert the time value based on the {@link TimeFieldSpec}.
 * <p>NOTE: should put this before the {@link DataTypeTransformer}. After this, time column can be treated as regular
 * column for other record transformers (incoming time column can be ignored).
 */
public class TimeTransformer implements RecordTransformer {
  private String _incomingTimeColumn;
  private String _outgoingTimeColumn;
  private TimeConverter _incomingTimeConverter;
  private TimeConverter _outgoingTimeConverter;
  private boolean _isValidated;

  public TimeTransformer(Schema schema) {
    TimeFieldSpec timeFieldSpec = schema.getTimeFieldSpec();
    if (timeFieldSpec != null) {
      TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
      TimeGranularitySpec outgoingGranularitySpec = timeFieldSpec.getOutgoingGranularitySpec();

      // Perform time conversion only if incoming and outgoing granularity spec are different
      if (!incomingGranularitySpec.equals(outgoingGranularitySpec)) {
        _incomingTimeColumn = incomingGranularitySpec.getName();
        _outgoingTimeColumn = outgoingGranularitySpec.getName();
        _incomingTimeConverter = new TimeConverter(incomingGranularitySpec);
        _outgoingTimeConverter = new TimeConverter(outgoingGranularitySpec);
      }
    }
  }

  @Override
  public GenericRow transform(GenericRow record) {
    if (_incomingTimeColumn == null) {
      return record;
    }

    Object incomingTimeValue = record.getValue(_incomingTimeColumn);
    // Validate the time values and determine whether the conversion is needed
    if (!_isValidated) {
      // If incoming time value does not exist or the value is invalid after conversion, check the outgoing time value.
      // If the outgoing time value is valid, skip time conversion, otherwise, throw exception.
      if (incomingTimeValue == null || !TimeUtils
          .timeValueInValidRange(_incomingTimeConverter.toMillisSinceEpoch(incomingTimeValue))) {
        Object outgoingTimeValue = record.getValue(_outgoingTimeColumn);
        if (outgoingTimeValue == null || !TimeUtils
            .timeValueInValidRange(_outgoingTimeConverter.toMillisSinceEpoch(outgoingTimeValue))) {
          throw new IllegalStateException(
              "No valid time value found in either incoming time column: " + _incomingTimeColumn
                  + " or outgoing time column: " + _outgoingTimeColumn);
        } else {
          disableConversion();
          return record;
        }
      }
      _isValidated = true;
    }

    record.putField(_outgoingTimeColumn,
        _outgoingTimeConverter.fromMillisSinceEpoch(_incomingTimeConverter.toMillisSinceEpoch(incomingTimeValue)));
    return record;
  }

  private void disableConversion() {
    _incomingTimeColumn = null;
    _outgoingTimeColumn = null;
    _incomingTimeConverter = null;
    _outgoingTimeConverter = null;
  }
}
