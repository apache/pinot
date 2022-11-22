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
package org.apache.pinot.segment.spi.memory;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;


public class ByteBufferUtil {

  private static final Constructor<? extends ByteBuffer> _dbbCC = findDirectByteBufferConstructor();

  private ByteBufferUtil() {
  }

  public static ByteBuffer newDirectByteBuffer(long addr, int size, Object att) {
    _dbbCC.setAccessible(true);
    try {
      return _dbbCC.newInstance(Long.valueOf(addr), Integer.valueOf(size), att);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create DirectByteBuffer", e);
    }
  }

  @SuppressWarnings("unchecked")
  private static Constructor<? extends ByteBuffer> findDirectByteBufferConstructor() {
    try {
      return (Constructor<? extends ByteBuffer>) Class.forName("java.nio.DirectByteBuffer")
          .getDeclaredConstructor(Long.TYPE, Integer.TYPE, Object.class);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Failed to find java.nio.DirectByteBuffer", e);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("Failed to find constructor f java.nio.DirectByteBuffer", e);
    }
  }
}
