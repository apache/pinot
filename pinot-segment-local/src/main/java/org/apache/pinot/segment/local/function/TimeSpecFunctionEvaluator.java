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
package org.apache.pinot.segment.local.function;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.TimeConverter;
import org.apache.pinot.spi.utils.TimeUtils;


/**
 * An implementation of {@link FunctionEvaluator} for converting the time value based on the {@link TimeFieldSpec}.
 */
public class TimeSpecFunctionEvaluator implements FunctionEvaluator {
  private final String _incomingTimeColumn;
  private final String _outgoingTimeColumn;
  private final TimeConverter _incomingTimeConverter;
  private final TimeConverter _outgoingTimeConverter;
  private boolean _isValidated = false;

  public TimeSpecFunctionEvaluator(TimeGranularitySpec incomingGranularitySpec,
      TimeGranularitySpec outgoingGranularitySpec) {
    Preconditions.checkState(!incomingGranularitySpec.equals(outgoingGranularitySpec));
    _incomingTimeColumn = incomingGranularitySpec.getName();
    _outgoingTimeColumn = outgoingGranularitySpec.getName();
    Preconditions.checkState(!_incomingTimeColumn.equals(_outgoingTimeColumn));
    _incomingTimeConverter = new TimeConverter(incomingGranularitySpec);
    _outgoingTimeConverter = new TimeConverter(outgoingGranularitySpec);
  }

  @Override
  public List<String> getArguments() {
    return Collections.singletonList(_incomingTimeColumn);
  }

  /**
   * Performs time transformation
   */
  @Override
  public Object evaluate(GenericRow genericRow) {
    return evaluate(genericRow.getValue(_incomingTimeColumn));
  }

  @Override
  public Object evaluate(Object[] values) {
    return evaluate(values[0]);
  }

  private Object evaluate(Object incomingTimeValue) {
    if (!_isValidated) {
      if (_incomingTimeColumn == null
          || !TimeUtils.timeValueInValidRange(_incomingTimeConverter.toMillisSinceEpoch(incomingTimeValue))) {
        throw new IllegalStateException("No valid time value found in incoming time column: " + _incomingTimeColumn);
      }
      _isValidated = true;
    }
    return _outgoingTimeConverter.fromMillisSinceEpoch(_incomingTimeConverter.toMillisSinceEpoch(incomingTimeValue));
  }
}
