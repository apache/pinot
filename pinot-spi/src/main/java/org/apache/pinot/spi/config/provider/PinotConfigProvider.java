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
package org.apache.pinot.spi.config.provider;

import java.util.List;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * An interface for the provider of pinot table configs and schemas.
 */
public interface PinotConfigProvider {

  /**
   * Returns the table config for the given table name with type suffix.
   */
  TableConfig getTableConfig(String tableNameWithType);

  /**
   * Registers the {@link TableConfigChangeListener} and notifies it whenever any changes (addition, update, removal)
   * to any of the table configs are detected. If the listener is successfully registered,
   * {@link TableConfigChangeListener#onChange(List)} will be invoked with the current table configs.
   *
   * @return {@code true} if the listener is successfully registered, {@code false} if the listener is already
   *         registered.
   */
  boolean registerTableConfigChangeListener(TableConfigChangeListener tableConfigChangeListener);

  /**
   * Returns the schema for the given raw table name.
   */
  Schema getSchema(String rawTableName);

  /**
   * Registers the {@link SchemaChangeListener} and notifies it whenever any changes (addition, update, removal) to any
   * of the schemas are detected. If the listener is successfully registered,
   * {@link SchemaChangeListener#onChange(List)} will be invoked with the current schemas.
   *
   * @return {@code true} if the listener is successfully registered, {@code false} if the listener is already
   *         registered.
   */
  boolean registerSchemaChangeListener(SchemaChangeListener schemaChangeListener);

  /**
   * Returns the logical table for the given logical table name.
   * @param logicalTableName the name of the logical table
   * @return the logical table
   */
  LogicalTableConfig getLogicalTable(String logicalTableName);

  /**
   * Registers the {@link LogicalTableChangeListener} and notifies it whenever any changes (addition, update,
   * @param logicalTableChangeListener the listener to be registered
   * @return {@code true} if the listener is successfully registered, {@code false} if the listener is already
   *         registered.
   */
  boolean registerLogicalTableChangeListener(LogicalTableChangeListener logicalTableChangeListener);
}
