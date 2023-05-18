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
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.EarlyTerminationException;
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
  private static final AtomicReference<ThreadResourceUsageAccountant> ACCOUNTANT_REGISTRATION = new AtomicReference<>();

  private static final class Holder {
    static final Tracer TRACER = TRACER_REGISTRATION.get() == null ? createDefaultTracer() : TRACER_REGISTRATION.get();
    static final ThreadResourceUsageAccountant ACCOUNTANT =
        ACCOUNTANT_REGISTRATION.get() == null ? createDefaultThreadAccountant() : ACCOUNTANT_REGISTRATION.get();
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
   * @param threadResourceUsageAccountant the threadAccountant implementation
   * @return true if the registration was successful.
   */
  public static boolean register(ThreadResourceUsageAccountant threadResourceUsageAccountant) {
    return ACCOUNTANT_REGISTRATION.compareAndSet(null, threadResourceUsageAccountant);
  }

  /**
   * visible for testing only
   */
  public static boolean isAccountantRegistered() {
    return ACCOUNTANT_REGISTRATION.get() != null;
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
  public static ThreadResourceUsageAccountant getThreadAccountant() {
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
      return (Tracer) MethodHandles.publicLookup().findConstructor(clazz, MethodType.methodType(void.class)).invoke();
    } catch (Throwable missing) {
      return FallbackTracer.INSTANCE;
    }
  }

  /**
   * Create default thread accountant for query preemption hardening. Use when {@see register} not called and
   * {@see initializeThreadAccountant} not loading any class
   * @return default thread accountant that only tracks the corresponding runner thread of each worker thread
   */
  private static DefaultThreadResourceUsageAccountant createDefaultThreadAccountant() {
    LOGGER.info("Using default thread accountant");
    return new DefaultThreadResourceUsageAccountant();
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
  public static class DefaultThreadResourceUsageAccountant implements ThreadResourceUsageAccountant {

    // worker thread's corresponding anchor thread, worker will also interrupt if it finds anchor's flag is raised
    private final ThreadLocal<Thread> _anchorThread;

    public DefaultThreadResourceUsageAccountant() {
      _anchorThread = new ThreadLocal<>();
    }

    @Override
    public boolean isAnchorThreadInterrupted() {
      Thread thread = _anchorThread.get();
      return thread != null && thread.isInterrupted();
    }

    @Override
    public void clear() {
      _anchorThread.set(null);
    }

    @Override
    public void setThreadResourceUsageProvider(ThreadResourceUsageProvider threadResourceUsageProvider) {
    }

    @Override
    public void sampleUsage() {
    }

    @Override
    public void updateQueryUsageConcurrently(String queryId) {
    }

    @Override
    public final void createExecutionContext(String queryId, int taskId, ThreadExecutionContext parentContext) {
      _anchorThread.set(parentContext == null ? Thread.currentThread() : parentContext.getAnchorThread());
      createExecutionContextInner(queryId, taskId, parentContext);
    }

    public void createExecutionContextInner(String queryId, int taskId, ThreadExecutionContext parentContext) {
    }

    @Override
    public ThreadExecutionContext getThreadExecutionContext() {
      return new ThreadExecutionContext() {
        @Override
        public String getQueryId() {
          return null;
        }

        @Override
        public Thread getAnchorThread() {
          return _anchorThread.get();
        }
      };
    }

    @Override
    public void startWatcherTask() {
    }

    @Override
    public Exception getErrorStatus() {
      return null;
    }
  }

  /**
   * Accountant related Ops util class
   */
  public static class ThreadAccountantOps {

    public static final int MAX_ENTRIES_KEYS_MERGED_PER_INTERRUPTION_CHECK_MASK = 0b1_1111_1111_1111;

    private ThreadAccountantOps() {
    }

    public static void setupRunner(String queryId) {
      Tracing.getThreadAccountant().setThreadResourceUsageProvider(new ThreadResourceUsageProvider());
      Tracing.getThreadAccountant().createExecutionContext(queryId, CommonConstants.Accounting.ANCHOR_TASK_ID, null);
    }

    public static void setupWorker(int taskId, ThreadResourceUsageProvider threadResourceUsageProvider,
        ThreadExecutionContext threadExecutionContext) {
      Tracing.getThreadAccountant().setThreadResourceUsageProvider(threadResourceUsageProvider);
      Tracing.getThreadAccountant().createExecutionContext(null, taskId, threadExecutionContext);
    }

    public static void sample() {
      Tracing.getThreadAccountant().sampleUsage();
    }

    public static void clear() {
      Tracing.getThreadAccountant().clear();
    }

    public static void initializeThreadAccountant(PinotConfiguration config, String instanceId) {
      String factoryName = config.getProperty(CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME);
      if (factoryName == null) {
        LOGGER.warn("No thread accountant factory provided, using default implementation");
      } else {
        LOGGER.info("Config-specified accountant factory name {}", factoryName);
        try {
          ThreadAccountantFactory threadAccountantFactory =
              (ThreadAccountantFactory) Class.forName(factoryName).getDeclaredConstructor().newInstance();
          boolean registered = Tracing.register(threadAccountantFactory.init(config, instanceId));
          LOGGER.info("Using accountant provided by {}", factoryName);
          if (!registered) {
            LOGGER.warn("ThreadAccountant {} register unsuccessful, as it is already registered.", factoryName);
          }
        } catch (Exception exception) {
          LOGGER.warn("Using default implementation of thread accountant, "
              + "due to invalid thread accountant factory {} provided, exception:", factoryName, exception);
        }
      }
      Tracing.getThreadAccountant().startWatcherTask();
    }

    public static boolean isInterrupted() {
      return Thread.interrupted() || Tracing.getThreadAccountant().isAnchorThreadInterrupted();
    }

    public static void sampleAndCheckInterruption() {
      if (isInterrupted()) {
        throw new EarlyTerminationException("Interrupted while merging records");
      }
      sample();
    }

    public static void updateQueryUsageConcurrently(String queryId) {
      Tracing.getThreadAccountant().updateQueryUsageConcurrently(queryId);
    }

    public static void setThreadResourceUsageProvider() {
      Tracing.getThreadAccountant().setThreadResourceUsageProvider(new ThreadResourceUsageProvider());
    }

    // Check for thread interruption, every time after merging 8192 keys
    public static void sampleAndCheckInterruptionPeriodically(int mergedKeys) {
      if ((mergedKeys & MAX_ENTRIES_KEYS_MERGED_PER_INTERRUPTION_CHECK_MASK) == 0) {
        sampleAndCheckInterruption();
      }
    }
  }
}
