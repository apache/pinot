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
package org.apache.pinot.plugin.inputformat.avro;

import java.util.Collection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;

/**
 * AvroCustomGenericData is a custom implementation of GenericData that overrides the newArray method.
 * This class provides a custom way to create new array instances based on the provided schema.
 * Reason for custom implementation: when creating a new array instance, Avro's default implementation uses
 * {@link org.apache.avro.generic.PrimitivesArrays} for primitive types which results into boxing/unboxing value issue.
 */
public class AvroCustomGenericData extends GenericData {

  /**
   * Creates a new array instance based on the provided schema.
   * If the old object is an instance of {@link org.apache.avro.generic.GenericArray} or {@link java.util.Collection}, it clears and reuses it.
   * Otherwise, it creates a new {@link org.apache.avro.generic.GenericData.Array} instance.
   * This is the old implementation of avro, which is changed in version 1.12.0
   *
   * @param old the old array object to reuse if possible
   * @param size the size of the new array
   * @param schema the schema of the array items
   * @return a new or reused array instance
   */
  @Override
  public Object newArray(Object old, int size, Schema schema) {
    if (old instanceof GenericArray) {
      ((GenericArray<?>) old).clear();
      return old;
    } else if (old instanceof Collection) {
      ((Collection<?>) old).clear();
      return old;
    }
    return new GenericData.Array<>(size, schema);
  }
}
