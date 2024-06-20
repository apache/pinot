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
package org.apache.pinot.spi.executor;

import java.util.concurrent.ExecutorService;
import org.apache.pinot.spi.env.PinotConfiguration;


public interface ExecutorServiceProvider {
  /**
   * An id that identifies this specific provider.
   * <p>
   * This id is used to select the provider in the configuration.
   */
  String id();

  /**
   * Creates a new {@link ExecutorService} instance.
   *
   * @param conf the configuration to use
   * @param confPrefix the prefix to use for the configuration
   * @param baseName the base name for the threads. A prefix that all threads will share.
   * @return a new {@link ExecutorService} instance
   */
  ExecutorService create(PinotConfiguration conf, String confPrefix, String baseName);
}
