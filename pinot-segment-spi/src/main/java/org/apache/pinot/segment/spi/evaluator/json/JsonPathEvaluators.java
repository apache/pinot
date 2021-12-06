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
package org.apache.pinot.segment.spi.evaluator.json;

import com.google.common.base.Preconditions;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Allows registration of a custom {@see JsonPathEvaluator} which can handle custom storage
 * functionality also present in a plugin. A default evaluator which can handle all default
 * storage types will be provided to delegate to when standard storage types are encountered.
 *
 * This is an evolving SPI and subject to change.
 */
public final class JsonPathEvaluators {

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonPathEvaluators.class);

  private static final AtomicReferenceFieldUpdater<JsonPathEvaluators, JsonPathEvaluatorProvider> UPDATER =
      AtomicReferenceFieldUpdater.newUpdater(JsonPathEvaluators.class, JsonPathEvaluatorProvider.class, "_provider");
  private static final JsonPathEvaluators INSTANCE = new JsonPathEvaluators();
  private static final DefaultProvider DEFAULT_PROVIDER = new DefaultProvider();
  private volatile JsonPathEvaluatorProvider _provider;

  /**
   * Registration point to override how JSON paths are evaluated. This should be used
   * when a Pinot plugin has special storage capabilities. For instance, imagine a
   * plugin with a raw forward index which stores JSON in a binary format which
   * pinot-core is unaware of and cannot evaluate JSON paths against (pinot-core only
   * understands true JSON documents). Whenever JSON paths are evaluated against this
   * custom storage, different storage access operations may be required, and the provided
   * {@see JsonPathEvaluator} can inspect the provided {@see ForwardIndexReader} to
   * determine whether it is the custom implementation and evaluate the JSON path against
   * the binary JSON managed by the custom reader. If it is not the custom implementation,
   * then the evaluation should be delegated to the provided delegate.
   *
   * This prevents the interface {@see ForwardIndexReader} from needing to be able to model
   * any plugin storage format, which creates flexibility for the kinds of data structure
   * plugins can employ.
   *
   * @param provider provides {@see JsonPathEvaluator}
   * @return true if registration is successful, false otherwise
   */
  public static boolean registerProvider(JsonPathEvaluatorProvider provider) {
    Preconditions.checkArgument(provider != null, "");
    if (!UPDATER.compareAndSet(INSTANCE, null, provider)) {
      LOGGER.warn("failed to register {} - {} already registered", provider, INSTANCE._provider);
      return false;
    }
    return true;
  }

  /**
   * pinot-core must construct {@see JsonPathEvaluator} via this method to ensure it uses
   * the registered implementation. Using the registered implementation allows pinot-core
   * to evaluate JSON paths against data structures it doesn't understand or model.
   * @param jsonPath the JSON path
   * @param defaultValue the default value
   * @return a JSON path evaluator which must understand all possible storage representations of JSON.
   */
  public static JsonPathEvaluator create(String jsonPath, Object defaultValue) {
    // plugins compose and delegate to the default implementation.
    JsonPathEvaluator defaultEvaluator = DEFAULT_PROVIDER.create(jsonPath, defaultValue);
    return Holder.PROVIDER.create(defaultEvaluator, jsonPath, defaultValue);
  }

  /**
   * Storing the registered evaluator in this holder and initialising it during
   * the class load gives the best of both worlds: plugins have until the first
   * JSON path evaluation to register an evaluator via
   * {@see JsonPathEvaluators#registerProvider}, but once this class is loaded,
   * the provider is constant and calls may be optimise aggressively by the JVM
   * in ways which are impossible with a volatile reference.
   */
  private static final class Holder {
    static final JsonPathEvaluatorProvider PROVIDER;

    static {
      JsonPathEvaluatorProvider provider = JsonPathEvaluators.INSTANCE._provider;
      if (provider == null) {
        provider = DEFAULT_PROVIDER;
        if (!UPDATER.compareAndSet(INSTANCE, null, provider)) {
          provider = JsonPathEvaluators.INSTANCE._provider;
        }
      }
      PROVIDER = provider;
    }
  }

  private static class DefaultProvider implements JsonPathEvaluatorProvider {

    // default implementation uses MethodHandles to avoid pulling lots of implementation details into the SPI layer

    private static final MethodHandle FACTORY;

    static {
      String className = "org.apache.pinot.core.common.evaluators.DefaultJsonPathEvaluator";
      MethodHandle factory = null;
      try {
        Class<?> clazz = Class.forName(className, false, JsonPathEvaluators.class.getClassLoader());
        factory = MethodHandles.publicLookup()
            .findStatic(clazz, "create", MethodType.methodType(JsonPathEvaluator.class, String.class, Object.class));
      } catch (Throwable implausible) {
        LOGGER.error("could not construct MethodHandle for {}", className,
            implausible);
      }
      FACTORY = factory;
    }

    public JsonPathEvaluator create(String jsonPath, Object defaultValue) {
      return create(null, jsonPath, defaultValue);
    }

    @Override
    public JsonPathEvaluator create(JsonPathEvaluator delegate, String jsonPath, Object defaultValue) {
      try {
        return (JsonPathEvaluator) FACTORY.invokeExact(jsonPath, defaultValue);
      } catch (IllegalArgumentException e) {
        throw e;
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }
}
