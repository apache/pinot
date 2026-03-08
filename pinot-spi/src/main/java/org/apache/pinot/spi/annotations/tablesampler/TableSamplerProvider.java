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
package org.apache.pinot.spi.annotations.tablesampler;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Annotation for table sampler providers.
 *
 * NOTE:
 *   - The annotated class must implement {@code org.apache.pinot.broker.routing.tablesampler.TableSampler}.
 *   - The annotated class must be public and concrete with a no-arg constructor.
 *   - The class must be discoverable via the packages configured with
 *     {@code pinot.broker.table.sampler.annotation.packages}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface TableSamplerProvider {

  /**
   * Alias name for the sampler (used in table config).
   */
  String name();

  boolean enabled() default true;
}
