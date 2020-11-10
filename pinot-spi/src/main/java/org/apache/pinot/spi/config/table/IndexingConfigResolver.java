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

import org.apache.pinot.spi.data.Schema;


public interface IndexingConfigResolver {
  /**
   * This interface is meant to customize indexing config based on the implementation.
   *
   * For instance, an auto-indexing resolver can take an indexing config as shown below:
   *     "tableIndexConfig": {
   *        “mode”: “auto”
   *        "streamConfigs" : {
   *           "streamType": "kafka",
   *           ...
   *         }
   *      }
   *
   * and automatically generate fields within Pinot Indexing config (eg: inverted indices,
   * no dictionary columns and so on).
   */
  public IndexingConfig resolveIndexingConfig(IndexingConfig initialConfig);

  /**
   * Register the table schema with this resolver.
   */
  public void registerSchema(Schema schema);
}
