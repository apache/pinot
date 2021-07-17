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
package org.apache.pinot.spi.env;

public abstract class PropertyConverter {
  @SuppressWarnings("unchecked")
  public static <T> T convert(Object value, Class<T> returnType) {
    if (Integer.class.equals(returnType)) {
      return (T) Integer.valueOf(value.toString());
    } else if (Boolean.class.equals(returnType)) {
      return (T) Boolean.valueOf(value.toString());
    } else if (Long.class.equals(returnType)) {
      return (T) Long.valueOf(value.toString());
    } else if (Double.class.equals(returnType)) {
      return (T) Double.valueOf(value.toString());
    } else if(String.class.equals(returnType)) {
      return (T) value.toString();
    } else {
      throw new IllegalArgumentException(returnType + " is not a supported type for conversion.");
    }
  }
}
