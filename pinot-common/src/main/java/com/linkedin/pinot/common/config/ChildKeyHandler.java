/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.config;

import io.vavr.collection.Map;


/**
 * Child key handler interface, used to convert the child keys of a given configuration object into a Java object.
 */
public interface ChildKeyHandler<T> {
  /**
   * Convert child keys of a given configuration object into a value for this configuration object.
   *
   * @param childKeys The child keys of the configuration object
   * @param pathPrefix The current path in the configuration map
   * @return The newly created configuration object
   */
  T handleChildKeys(Map<String, ?> childKeys, String pathPrefix);

  /**
   * Transform a configuration object into a map of child keys.
   *
   * @param value The value to turn into a map of child keys
   * @param pathPrefix The current path of the configuration object
   * @return A map representation of the configuration object
   */
  Map<String, ?> unhandleChildKeys(T value, String pathPrefix);
}
