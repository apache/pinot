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
package org.apache.pinot.spi.config.table;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.ConfigValidationException;


/**
 * Registry for {@link TableConfigValidator} implementations.
 * Supports multiple validators invoked in registration order; the first rejection short-circuits with an error.
 * When no validators are registered, {@link #validate} is a no-op.
 */
public class TableConfigValidatorRegistry {
  private TableConfigValidatorRegistry() {
  }

  private static final CopyOnWriteArrayList<TableConfigValidator> VALIDATORS = new CopyOnWriteArrayList<>();

  /**
   * Registers a validator. Called during startup.
   *
   * @param validator The validator to register
   */
  public static void register(TableConfigValidator validator) {
    VALIDATORS.add(validator);
  }

  /**
   * Invokes all registered validators in registration order.
   * Short-circuits on the first rejection.
   *
   * @param tableConfig The table config to validate
   * @param schema The table's schema, or null if not available
   * @throws ConfigValidationException if any validator rejects the config
   */
  public static void validate(TableConfig tableConfig, @Nullable Schema schema)
      throws ConfigValidationException {
    for (TableConfigValidator validator : VALIDATORS) {
      validator.validate(tableConfig, schema);
    }
  }

  @VisibleForTesting
  public static void reset() {
    VALIDATORS.clear();
  }
}
