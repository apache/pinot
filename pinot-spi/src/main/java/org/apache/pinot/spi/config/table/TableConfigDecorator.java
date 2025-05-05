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

/**
 * Interface for decorating TableConfig objects.
 * Implementations of this interface can wrap a TableConfig
 * to add or modify behavior.
 */
public interface TableConfigDecorator {

  /**
   * Decorates the given TableConfig.
   * Implementations should modify the provided TableConfig object or return a new one
   * with the desired changes.
   *
   * @param tableConfig The original TableConfig loaded from the metadata store.
   * @return The decorated (potentially modified) TableConfig.
   */
  TableConfig decorate(TableConfig tableConfig);
}
