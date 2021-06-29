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
package org.apache.pinot.controller.tuner;

import javax.annotation.Nullable;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TunerConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.yetus.audience.InterfaceStability;


/**
 * Interface for Table Config Tuner.
 *
 * Currently, we have a very simplistic implementation, which only needs the schema.
 * But we anticipate that more sophisticated implementations would need more information,
 * hence marking the interface as evolving.
 */
@InterfaceStability.Evolving
public interface TableConfigTuner {
  /**
   * Used to initialize underlying implementation with Schema
   * and custom properties (eg: metrics end point)
   *
   * @param pinotHelixResourceManager Pinot Helix Resource Manager to access Helix resources
   * @param tunerConfig Tuner configurations
   * @param schema Table schema
   */
  void init(@Nullable PinotHelixResourceManager pinotHelixResourceManager, TunerConfig tunerConfig, Schema schema);

  /**
   * Takes the original TableConfig and returns a tuned one
   */
  TableConfig apply(TableConfig initialConfig);
}
