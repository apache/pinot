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
package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.transformer.timeunit.TimeUnitTransformer;
import org.apache.pinot.core.operator.transform.transformer.timeunit.TimeUnitTransformerFactory;
import org.apache.pinot.segment.spi.datasource.DataSource;


public class TimeConversionTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "timeConvert";

  private TransformFunction _mainTransformFunction;
  private TimeUnitTransformer _timeUnitTransformer;
  private long[] _outputTimes;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are exactly 3 arguments
    if (arguments.size() != 3) {
      throw new IllegalArgumentException("Exactly 3 arguments are required for TIME_CONVERT transform function");
    }

    TransformFunction firstArgument = arguments.get(0);
    if (firstArgument instanceof LiteralTransformFunction || !firstArgument.getResultMetadata().isSingleValue()) {
      throw new IllegalArgumentException(
          "The first argument of TIME_CONVERT transform function must be a single-valued column or a transform "
              + "function");
    }
    _mainTransformFunction = firstArgument;

    _timeUnitTransformer = TimeUnitTransformerFactory.getTimeUnitTransformer(
        TimeUnit.valueOf(((LiteralTransformFunction) arguments.get(1)).getLiteral().toUpperCase()),
        ((LiteralTransformFunction) arguments.get(2)).getLiteral());
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return LONG_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public long[] transformToLongValuesSV(ProjectionBlock projectionBlock) {
    int length = projectionBlock.getNumDocs();

    if (_outputTimes == null || _outputTimes.length < length) {
      _outputTimes = new long[length];
    }

    _timeUnitTransformer.transform(_mainTransformFunction.transformToLongValuesSV(projectionBlock), _outputTimes,
        length);
    return _outputTimes;
  }
}
