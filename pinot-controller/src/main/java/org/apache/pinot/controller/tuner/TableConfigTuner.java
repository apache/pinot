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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.yetus.audience.InterfaceStability;


/**
 * Interface for Table Config Tuner.
 */
@InterfaceStability.Evolving
public interface TableConfigTuner {

  /**
   * Apply tuner to a {@link TableConfig}.
   *
   * @param pinotHelixResourceManager Pinot Helix Resource Manager to access Helix resources
   * @param tableConfig tableConfig that needs to be tuned.
   * @param schema Table schema
   * @param extraProperties extraProperties for the tuner implementation.
   */
  TableConfig apply(@Nullable PinotHelixResourceManager pinotHelixResourceManager,
      TableConfig tableConfig, Schema schema, Map<String, String> extraProperties);
}
