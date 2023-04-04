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
package org.apache.pinot.segment.spi.memory.unsafe;

import java.lang.reflect.Field;
import sun.misc.Unsafe;

public class Unsafer {

  public static final Unsafe UNSAFE;

  static {
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      UNSAFE = Unsafe.class.cast(f.get(null));
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException("sun.misc.Unsafe is not available in this JVM");
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("sun.misc.Unsafe is not available in this JVM");
    }
  }

  private Unsafer() {
  }
}
