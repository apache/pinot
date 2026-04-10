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

import org.apache.pinot.spi.exception.ConfigValidationException;


/**
 * SPI interface for validating instance config mutations (add, update, updateTags).
 * Implementations are registered via {@link InstanceConfigValidatorRegistry}
 * and invoked before instance config is persisted to Helix. Throw {@link ConfigValidationException} to reject.
 *
 * <p>Implementations must be thread-safe — they may be called concurrently from multiple request threads.</p>
 */
public interface InstanceConfigValidator {

  /**
   * Validates the given instance before persistence.
   *
   * @param instance The instance being added or updated (for updateTags, this is reconstructed
   *                 from the existing InstanceConfig with the new tags applied)
   * @throws ConfigValidationException if the instance config violates validation rules
   */
  void validate(Instance instance)
      throws ConfigValidationException;
}
