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
package org.apache.pinot.spi.data.function.evaluators;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.TimeConverter;
import org.apache.pinot.spi.utils.TimeUtils;


/**
 * The {@code DefaultTimeSpecEvaluator} class will convert the time value based on the {@link TimeFieldSpec}.
 */
public class DefaultTimeSpecEvaluator implements ExpressionEvaluator {
  private String _incomingTimeColumn;
  private String _outgoingTimeColumn;
  private TimeConverter _incomingTimeConverter;
  private TimeConverter _outgoingTimeConverter;
  private boolean _isValidated;
  private boolean _convert = false;
  private boolean _useOutgoing = false;

  public DefaultTimeSpecEvaluator(TimeFieldSpec timeFieldSpec) {
    TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
    TimeGranularitySpec outgoingGranularitySpec = timeFieldSpec.getOutgoingGranularitySpec();

    _incomingTimeColumn = incomingGranularitySpec.getName();

    // Perform time conversion only if incoming and outgoing granularity spec are different
    if (outgoingGranularitySpec != null && !incomingGranularitySpec.equals(outgoingGranularitySpec)) {
      _incomingTimeConverter = new TimeConverter(incomingGranularitySpec);
      _outgoingTimeColumn = outgoingGranularitySpec.getName();
      _outgoingTimeConverter = new TimeConverter(outgoingGranularitySpec);
      _convert = true;
    }
  }

  @Override
  public List<String> getArguments() {
    return Lists.newArrayList(_incomingTimeColumn);
  }

  /**
   * Performs time transformation
   */
  @Override
  public Object evaluate(GenericRow genericRow) {

    if (!_isValidated) {
      Object incomingTimeValue = genericRow.getValue(_incomingTimeColumn);

      if (_convert) { // Validate if we can convert.
        // If incoming time value does not exist or the value is invalid after conversion, check if we have outgoing time value.
        // If the outgoing time value is valid, don't convert, just use outgoing
        // otherwise, throw exception.
        if (incomingTimeValue == null || !TimeUtils
            .timeValueInValidRange(_incomingTimeConverter.toMillisSinceEpoch(incomingTimeValue))) {
          Object outgoingTimeValue = genericRow.getValue(_outgoingTimeColumn);
          if (outgoingTimeValue == null || !TimeUtils
              .timeValueInValidRange(_outgoingTimeConverter.toMillisSinceEpoch(outgoingTimeValue))) {
            throw new IllegalStateException(
                "No valid time value found in either incoming time column: " + _incomingTimeColumn
                    + " or outgoing time column: " + _outgoingTimeColumn);
          } else {
            _convert = false;
            _useOutgoing = true;
          }
        }
      }
      _isValidated = true;
    }

    Object convertedTime;
    if (_convert) {
      convertedTime = _outgoingTimeConverter
          .fromMillisSinceEpoch(_incomingTimeConverter.toMillisSinceEpoch(genericRow.getValue(_incomingTimeColumn)));
    } else {
      if (_useOutgoing) {
        convertedTime = genericRow.getValue(_outgoingTimeColumn);
      } else {
        convertedTime = genericRow.getValue(_incomingTimeColumn);
      }
    }
    return convertedTime;
  }
}
