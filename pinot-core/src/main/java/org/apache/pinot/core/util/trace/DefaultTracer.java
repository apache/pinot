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

import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.InvocationSpan;
import org.apache.pinot.spi.trace.TraceState;
import org.apache.pinot.spi.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultTracer implements Tracer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTracer.class);
  private static final ThreadLocal<Deque<InvocationRecording>> STACK = ThreadLocal.withInitial(ArrayDeque::new);

  private static class NoOpSpan implements InvocationSpan {

    @Override
    public void close() {
    }
  }

  private static final NoOpSpan NO_OP_SPAN = new NoOpSpan();

  private static final class MilliTimeSpan implements InvocationSpan {

    private final long _startTimeMillis = System.currentTimeMillis();
    private final Class<?> _operator;
    private final Runnable _onClose;

    public MilliTimeSpan(Class<?> operator, Runnable onClose) {
      _operator = operator;
      _onClose = onClose;
    }

    @Override
    public void close() {
      String operatorName = _operator.getSimpleName();
      long duration = System.currentTimeMillis() - _startTimeMillis;
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Time spent in {}: {}", operatorName, duration);
      }
      org.apache.pinot.core.util.trace.TraceContext.logTime(operatorName, duration);
      _onClose.run();
    }
  }

  @Override
  public void register(long requestId) {
    TraceContext.register(requestId);
  }

  @Override
  public InvocationSpan beginInvocation(Class<?> operatorClass) {
    if (TraceContext.traceEnabled()) {
      Deque<InvocationRecording> stack = getStack();
      MilliTimeSpan execution = new MilliTimeSpan(operatorClass, stack::removeLast);
      stack.addLast(execution);
      return execution;
    }
    return NO_OP_SPAN;
  }

  @Override
  public InvocationRecording activeRecording() {
    Deque<InvocationRecording> stack = getStack();
    return stack.isEmpty() ? NO_OP_SPAN : stack.peekLast();
  }

  private Deque<InvocationRecording> getStack() {
    Thread thread = Thread.currentThread();
    if (thread instanceof TraceState) {
      return ((TraceState) thread).getRecordings();
    }
    return STACK.get();
  }
}
