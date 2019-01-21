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
package org.apache.pinot.common.data;

import java.util.List;

/**
 * Common interface for complex Object types such as HyperLogLog, Map, JSON etc.
 * Flow to convert byte[] to PinotObject
 * - compute the objectTypeClass from objectType (from schema/fieldSpec.objectType)
 * - Instantiate PinotObject instance
 * - call init(bytes)
 * - expects all other methods to be implemented.
 */
public interface PinotObject {

  /**
   * Initializes the PinotObject from byte[]. Note that this method can be repeatedly called on the
   * same instance of PinotObject.
   * @param bytes
   */
  void init(byte[] bytes);

  /**
   * @return serialized byte form
   */
  byte[] toBytes();

  /**
   * @return list of properties in this object. Note, this can return nested properties using dot
   *         notation
   *         
   */
  List<String> getPropertyNames();

  /**
   * @param fieldName
   * @return the value of the property, it can be a single object or a list of objects.
   */
  Object getProperty(String propertyName);

}
