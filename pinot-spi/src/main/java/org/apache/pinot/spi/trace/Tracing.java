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


/**
 * Global registration for tracing implementations.
 */
public class Tracing {
  private Tracing() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Tracing.class);

  /// This is the registration point for third party [Tracer] implementations. Only one tracer can be registered to
  /// avoid the overhead of polymorphic calls, and it must be registered before the first call to [#getTracer()].
  private static final AtomicReference<Tracer> TRACER_REGISTRY = new AtomicReference<>();

  /// User once registration point to allow customization of tracing behaviour. Registration will be successful
  /// if it is the first attempt to register and registration happened before first call to [#getTracer()].
  public static boolean register(Tracer tracer) {
    return TRACER_REGISTRY.compareAndSet(null, tracer);
  }

  private static final class TracerHolder {
    static final Tracer TRACER;

    static {
      Tracer tracer = TRACER_REGISTRY.get();
      TRACER = tracer != null ? tracer : createDefaultTracer();
    }
  }

  /// Returns the registered [Tracer].
  public static Tracer getTracer() {
    return TracerHolder.TRACER;
  }

  /**
   * Get the active recording on the current thread to write values into.
   * @return the active recording
   */
  public static InvocationRecording activeRecording() {
    return getTracer().activeRecording();
  }

  private static Tracer createDefaultTracer() {
    String builtInTracerClassPath = "org.apache.pinot.core.util.trace.BuiltInTracer";
    try {
      Class<?> clazz = Class.forName(builtInTracerClassPath, false, Tracing.class.getClassLoader());
      Tracer builtInTracer =
          (Tracer) MethodHandles.publicLookup().findConstructor(clazz, MethodType.methodType(void.class)).invoke();
      LOGGER.info("Using built-in tracer");
      return builtInTracer;
    } catch (Throwable t) {
      LOGGER.error("Failed to initialize built-in tracer, using no-op tracer", t);
      return FallbackTracer.INSTANCE;
    }
  }

  /**
   * Used only when something has gone wrong and even the default tracer cannot be loaded
   * (won't happen except in tests or completely custom deployments which exclude pinot-segment-local).
   */
  private static final class FallbackTracer implements Tracer {
    static final FallbackTracer INSTANCE = new FallbackTracer();

    @Override
    public void register(long requestId) {
    }

    @Override
    public void unregister() {
    }

    @Override
    public InvocationScope createScope(Class<?> clazz) {
      return NoOpRecording.INSTANCE;
    }

    @Override
    public InvocationRecording activeRecording() {
      return NoOpRecording.INSTANCE;
    }
  }
}
