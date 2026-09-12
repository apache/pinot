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


/// SPI interface for extending table-config and schema-mutation validation.
/// Implementations are registered via [TableConfigValidatorRegistry]. [#validate(TableConfig, Schema)] is invoked
/// before a table config is persisted; schema validation is a separate opt-in phase described by [#validateSchema].
/// Throw [ConfigValidationException] to reject a mutation.
///
/// Implementations must be thread-safe — they may be called concurrently from multiple request threads.
public interface TableConfigValidator {

  /// Validates the given table config before persistence.
  ///
  /// @param tableConfig The table config being created or updated
  /// @param schema The table's schema, or null if not available
  /// @throws ConfigValidationException if the table config violates validation rules
  void validate(TableConfig tableConfig, @Nullable Schema schema)
      throws ConfigValidationException;

  /// Validates a proposed schema against an existing table config.
  ///
  /// This is a separate, opt-in phase from [#validate(TableConfig, Schema)]. Existing validators may depend on
  /// table-config mutation state and therefore must not run for schema writes unless they explicitly override this
  /// method. The default implementation is a no-op.
  ///
  /// @param tableConfig The existing table config associated with the schema
  /// @param schema The proposed schema
  /// @throws ConfigValidationException if the schema violates validation rules
  default void validateSchema(TableConfig tableConfig, Schema schema)
      throws ConfigValidationException {
  }
}
