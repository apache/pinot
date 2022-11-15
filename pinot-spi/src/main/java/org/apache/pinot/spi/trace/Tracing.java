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
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.spi.accounting.ExecutionContext;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Global registration for tracing and thread accounting implementations
 */
public class Tracing {
  private static final Logger LOGGER = LoggerFactory.getLogger(Tracing.class);

  private Tracing() {
  }

  /**
   * This is the registration point for third party tracing implementations, which can register an {@see Tracer} here.
   * Only one tracer can be registered to avoid the overhead of polymorphic calls in what can be hot code paths.
   * The tracer registered here will be used by pinot for all manually instrumented scopes, so long as it is
   * registered before the first call to {@see getTracer} or {@see activeRecording}.
   */
  private static final AtomicReference<Tracer> TRACER_REGISTRATION = new AtomicReference<>();

  /**
   * This is the registration point for ThreadAccountant implementations. Similarly, only one ThreadAccountant can be
   * registered. The thread accountant registered here will be used for thread usage/ task status accountant, so long
   * as it is registered before the first call to {@see getThreadAccountant} or {@see ThreadAccountantOps}.
   */
  private static final AtomicReference<ThreadAccountant> ACCOUNTANT_REGISTRATION = new AtomicReference<>();

  private static final class Holder {
    static final Tracer TRACER = TRACER_REGISTRATION.get() == null ? createDefaultTracer() : TRACER_REGISTRATION.get();
    static final ThreadAccountant ACCOUNTANT = ACCOUNTANT_REGISTRATION.get() == null
        ? createDefaultThreadAccountant() : ACCOUNTANT_REGISTRATION.get();
  }

  /**
   * User once registration point to allow customization of tracing behaviour. Registration will be successful
   * if this was the first attempt to register and registration happened before first use of the tracer.
   * @param tracer the tracer implementation
   * @return true if the registration was successful.
   */
  public static boolean register(Tracer tracer) {
    return TRACER_REGISTRATION.compareAndSet(null, tracer);
  }

  /**
   * Registration point to allow customization of thread accounting behavior. Registration will be successful
   * if this was the first attempt to register and registration happened before first use of the thread accountant.
   * @param threadAccountant the threadAccountant implementation
   * @return true if the registration was successful.
   */
  public static boolean register(ThreadAccountant threadAccountant) {
    return ACCOUNTANT_REGISTRATION.compareAndSet(null, threadAccountant);
  }

  /**
   * @return the registered tracer.
   */
  public static Tracer getTracer() {
    return Holder.TRACER;
  }

  /**
   * @return the registered threadAccountant.
   */
  public static ThreadAccountant getThreadAccountant() {
    return Holder.ACCOUNTANT;
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
      return FallbackTracer.INSTANCE;
    }
  }

  /**
   * Create default thread accountant for query preemption hardening. Use when {@see register} not called and
   * {@see initializeThreadAccountant} not loading any class
   * @return default thread accountant that only tracks the corresponding runner thread of each worker thread
   */
  private static DefaultThreadAccountant createDefaultThreadAccountant() {
    LOGGER.info("Using default thread accountant");
    return new DefaultThreadAccountant();
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

  /**
   * Default accountant that is used to enable worker thread cancellation upon runner thread's interruption
   */
  public static class DefaultThreadAccountant implements ThreadAccountant {

    // worker thread's corresponding runner thread, worker will also interrupt if it finds runner's flag is raised
    private final ThreadLocal<Thread> _rootThread;

    public DefaultThreadAccountant() {
      _rootThread = new ThreadLocal<>();
    }

    @Override
    public boolean isRootThreadInterrupted() {
      Thread thread = _rootThread.get();
      return thread != null && thread.isInterrupted();
    }

    @Override
    public void clear() {
      _rootThread.set(null);
    }

    @Override
    public void setThreadResourceUsageProvider(ThreadResourceUsageProvider threadResourceUsageProvider) {
    }

    @Override
    public void sampleThreadCPUTime() {
    }

    @Override
    public void sampleThreadBytesAllocated() {
    }

    @Override
    public final void createExecutionContext(String queryId, int taskId,
        ExecutionContext parentContext) {
      _rootThread.set(parentContext == null ? Thread.currentThread() : parentContext.getRootThread());
      createExecutionContextInner(queryId, taskId, parentContext);
    }

    public void createExecutionContextInner(String queryId, int taskId, ExecutionContext parentContext) {
    }

    @Override
    public ExecutionContext getExecutionContext() {
      return new ExecutionContext() {
        @Override
        public String getQueryId() {
          return null;
        }

        @Override
        public Thread getRootThread() {
          return _rootThread.get();
        }
      };
    }

    @Override
    public void startWatcherTask() {
    }

    @Override
    public String getErrorMsg() {
      return StringUtils.EMPTY;
    }
  }

  /**
   * Accountant related Ops util class
   */
  public static class ThreadAccountantOps {

    private ThreadAccountantOps() {
    }

    public static void setupRunner(String queryId) {
      Tracing.getThreadAccountant().setThreadResourceUsageProvider(new ThreadResourceUsageProvider());
      Tracing.getThreadAccountant().createExecutionContext(queryId, CommonConstants.Accounting.INVALID_TASK_ID,
          null);
    }

    public static void setupWorker(int taskId, ThreadResourceUsageProvider threadResourceUsageProvider,
        ExecutionContext executionContext) {
      Tracing.getThreadAccountant().setThreadResourceUsageProvider(threadResourceUsageProvider);
      Tracing.getThreadAccountant().createExecutionContext(null, taskId, executionContext);
    }

    public static void sample() {
      Tracing.getThreadAccountant().sampleThreadCPUTime();
      Tracing.getThreadAccountant().sampleThreadBytesAllocated();
    }

    public static void clear() {
      Tracing.getThreadAccountant().clear();
    }

    public static void initializeThreadAccountant(int numPqr, int numPqw, PinotConfiguration config) {
      String factoryName = config.getProperty(CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME);
      try {
        ThreadAccountantFactory threadAccountantFactory =
            (ThreadAccountantFactory) Class.forName(factoryName).getDeclaredConstructor().newInstance();
        boolean registered = Tracing.register(threadAccountantFactory.init(numPqr, numPqw, config));
        LOGGER.info("Using accountant provided by {}", factoryName);
        if (!registered) {
          LOGGER.warn("ThreadAccountant {} register unsuccessful, as it is already registered.", factoryName);
        }
      } catch (Exception exception) {
        LOGGER.warn("Using default implementation of thread accountant, "
            + "due to invalid thread accountant factory {} provided.", factoryName);
      }
      Tracing.getThreadAccountant().startWatcherTask();
    }

    public static boolean isInterrupted() {
      return Thread.interrupted() || Tracing.getThreadAccountant().isRootThreadInterrupted();
    }
  }
}
