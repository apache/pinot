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

import javax.annotation.Nullable;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.ConfigValidationException;


/**
 * SPI interface for validating table config mutations (create and update).
 * Implementations are registered via {@link TableConfigValidatorRegistry}
 * and invoked before table config is persisted. Throw {@link ConfigValidationException} to reject.
 *
 * <p>Implementations must be thread-safe — they may be called concurrently from multiple request threads.</p>
 */
public interface TableConfigValidator {

  /**
   * Validates the given table config before persistence.
   *
   * @param tableConfig The table config being created or updated
   * @param schema The table's schema, or null if not available
   * @throws ConfigValidationException if the table config violates validation rules
   */
  void validate(TableConfig tableConfig, @Nullable Schema schema)
      throws ConfigValidationException;
}
