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
package org.apache.pinot.segment.spi.index.creator;

import java.util.Map;


/**
 * Creates the durable representation of a map index. Metadata about the Map Column can be passed through via
 * the IndexCreationContext and the implementation of this Interface can use that to determine the on
 * disk representation of the Map.
 */
public interface MapIndexCreator extends ForwardIndexCreator {
  int VERSION_1 = 1;

  /**
   *
   * @param value The nonnull value of the cell. In case the cell was actually null, a default value is received instead
   * @param dict This is ignored as the MapIndexCreator will manage the construction of dictionaries itself.
   */
  @Override
  default void add(Object value, int dict) {
    Map<String, Object> mapValue = (Map<String, Object>) value;
    add(mapValue);
  }

  void add(Map<String, Object> mapValue);
}
