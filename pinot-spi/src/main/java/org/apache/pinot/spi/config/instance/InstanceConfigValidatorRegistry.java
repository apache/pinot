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
package org.apache.pinot.spi.config.instance;

import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.pinot.spi.exception.ConfigValidationException;


/**
 * Registry for {@link InstanceConfigValidator} implementations.
 * Supports multiple validators invoked in registration order; the first rejection short-circuits with an error.
 * When no validators are registered, {@link #validate} is a no-op.
 *
 * <p><b>Lifecycle:</b> {@code BaseControllerStarter#stop()} invokes {@link #reset()} automatically, so
 * subclasses do <b>not</b> need to call it themselves. Validators registered against controller-owned state
 * (e.g. {@code HelixAdmin}, {@code HelixManager}, ZK client references) become stale once the controller
 * stops; the automatic {@code reset()} in {@code stop()} ensures they are removed before any subsequent
 * {@code start()} registers replacements. This matters for in-process restart scenarios such as integration
 * tests that reuse a single JVM across multiple controller lifecycles — without it, the next config-mutation
 * request would fail when a stale validator dereferences its torn-down dependencies.</p>
 */
public class InstanceConfigValidatorRegistry {
  private InstanceConfigValidatorRegistry() {
  }

  private static final CopyOnWriteArrayList<InstanceConfigValidator> VALIDATORS = new CopyOnWriteArrayList<>();

  /**
   * Registers a validator. Called during startup.
   *
   * @param validator The validator to register
   */
  public static void register(InstanceConfigValidator validator) {
    VALIDATORS.add(validator);
  }

  /**
   * Invokes all registered validators in registration order.
   * Short-circuits on the first rejection.
   *
   * @param instance The instance to validate
   * @throws ConfigValidationException if any validator rejects the config
   */
  public static void validate(Instance instance)
      throws ConfigValidationException {
    for (InstanceConfigValidator validator : VALIDATORS) {
      validator.validate(instance);
    }
  }

  /**
   * Removes a previously registered validator. No-op if the validator is not registered. Removes only the
   * first matching reference if the same validator was registered multiple times.
   *
   * @param validator The validator instance to remove
   * @return {@code true} if a validator was removed, {@code false} otherwise
   */
  public static boolean unregister(InstanceConfigValidator validator) {
    return VALIDATORS.remove(validator);
  }

  /**
   * Removes all registered validators. Intended to be called from a service's shutdown/stop hook when the
   * validators it registered are about to become stale (e.g. they hold references to soon-to-be-disconnected
   * Helix clients).
   *
   * <p>In production this is rarely needed since service shutdown coincides with JVM shutdown. It is essential
   * for in-process restart scenarios such as integration tests that reuse a single JVM across multiple
   * controller lifecycles.</p>
   */
  public static void reset() {
    VALIDATORS.clear();
  }
}
