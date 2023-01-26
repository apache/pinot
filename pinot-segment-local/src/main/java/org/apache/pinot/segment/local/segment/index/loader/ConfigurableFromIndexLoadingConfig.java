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

package org.apache.pinot.segment.local.segment.index.loader;

import java.util.Map;
import org.apache.pinot.segment.spi.index.IndexDeclaration;
import org.apache.pinot.segment.spi.index.IndexType;


/**
 * This interface can be optionally implemented by {@link org.apache.pinot.segment.spi.index.IndexType index types} to
 * indicate that they can extract their configuration from an older {@link IndexLoadingConfig} object.
 */
public interface ConfigurableFromIndexLoadingConfig<C> {

  /**
   * Returns a map where keys are the columns and values are the configs declared for these columns.
   *
   * Sometimes {@link IndexLoadingConfig} is not completely configured and
   * {@link IndexLoadingConfig#getAllKnownColumns()} does not return all columns in the table. In that case,
   * implementations have to fill the map as much as they can and callers should complete the map, usually by adding
   * new entries whose key is the missing column and the value is {@link IndexType#getDefaultConfig()}.
   *
   * @param indexLoadingConfig
   * @return
   */
  Map<String, IndexDeclaration<C>> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig);
}
