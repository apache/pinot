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
package org.apache.pinot.core.util.trace;

import org.apache.pinot.spi.trace.BaseRecording;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.InvocationScope;
import org.apache.pinot.spi.trace.NoOpRecording;
import org.apache.pinot.spi.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BuiltInTracer implements Tracer {

  private static final Logger LOGGER = LoggerFactory.getLogger(BuiltInTracer.class);

  private static final class MilliTimeSpan extends BaseRecording implements InvocationScope {

    private final long _startTimeMillis = System.currentTimeMillis();
    private final Class<?> _operator;

    public MilliTimeSpan(Class<?> operator) {
      super(true);
      _operator = operator;
    }

    @Override
    public void close() {
      String operatorName = _operator.getSimpleName();
      long duration = System.currentTimeMillis() - _startTimeMillis;
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Time spent in {}: {}", operatorName, duration);
      }
      TraceContext.logTime(operatorName, duration);
    }

    @Override
    public void close(Object context) {
      String operatorName = _operator.getSimpleName();
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Context collected for {}: {}", operatorName, context);
      }
      TraceContext.logInfo(operatorName, context);
    }
  }

  @Override
  public void register(long requestId) {
    TraceContext.register(requestId);
  }

  @Override
  public void unregister() {
    TraceContext.unregister();
  }

  @Override
  public InvocationScope createScope(Class<?> operatorClass) {
    return TraceContext.traceEnabled() ? new MilliTimeSpan(operatorClass) : NoOpRecording.INSTANCE;
  }

  @Override
  public InvocationRecording activeRecording() {
    return NoOpRecording.INSTANCE;
  }
}
