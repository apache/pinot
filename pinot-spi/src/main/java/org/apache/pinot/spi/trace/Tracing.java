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
package org.apache.pinot.spi.trace;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Tracing {

  private Tracing() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Tracing.class);

  private static final AtomicReference<Tracer> REGISTRATION = new AtomicReference<>();

  /**
   * User once registration point to allow customization of tracing behaviour. Registration will be successful
   * if this was the first attempt to register and registration happened before first use of the tracer.
   * @param tracer the tracer implementation
   * @return true if the registration was successful.
   */
  public static boolean register(Tracer tracer) {
    return REGISTRATION.compareAndSet(null, tracer);
  }

  private static final class Holder {
    static final Tracer TRACER = REGISTRATION.get() == null ? createDefaultTracer() : REGISTRATION.get();
  }

  /**
   * @return the registered tracer.
   */
  public static Tracer getTracer() {
    return Holder.TRACER;
  }

  /**
   * Get the active recording on the current thread to write values into.
   * @return the active recording
   */
  public static InvocationRecording activeRecording() {
    return getTracer().activeRecording();
  }

  private static Tracer createDefaultTracer() {
    // create the default tracer via method handles if no override is registered
    String defaultImplementationClassName = "org.apache.pinot.core.util.trace.BuiltInTracer";
    try {
      Class<?> clazz = Class.forName(defaultImplementationClassName, false, Tracing.class.getClassLoader());
      return (Tracer) MethodHandles.publicLookup()
          .findConstructor(clazz, MethodType.methodType(void.class)).invoke();
    } catch (Throwable missing) {
      LOGGER.error("could not construct MethodHandle for {}", defaultImplementationClassName, missing);
      return NoOpTracer.INSTANCE;
    }
  }

  private static final class NoOpTracer implements Tracer {

    static final NoOpTracer INSTANCE = new NoOpTracer();

    @Override
    public void register(long requestId) {
    }

    @Override
    public InvocationSpan beginInvocation(Class<?> clazz) {
      return NoOpSpan.INSTANCE;
    }

    @Override
    public InvocationRecording activeRecording() {
      return NoOpSpan.INSTANCE;
    }
  }
}
