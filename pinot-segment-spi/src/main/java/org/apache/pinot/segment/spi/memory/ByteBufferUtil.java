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

import com.google.common.collect.Lists;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * A utility class used to allocate a ByteBuffer pointing to an already used address in memory.
 *
 * This feature is used internally by the JVM but there is no way to directly call these internal methods from user Java
 * code. By using reflection, this class let us call these methods. Given that these methods are private, they may
 * change from JVM to JVM and in fact they do. Therefore this class tries several methods that we know that exists in
 * different JVM versions.
 */
public class ByteBufferUtil {

  private static final ByteBufferCreator CREATOR;
  private static final List<CreatorSupplier> _SUPPLIERS = Lists.newArrayList(
      // From Java 21
      () -> {
        Class<?> memorySegmentProxyClass = Class.forName("java.lang.foreign.MemorySegment");
        Constructor<? extends ByteBuffer> dbbCC =
            (Constructor<? extends ByteBuffer>) Class.forName("java.nio.DirectByteBuffer")
                .getDeclaredConstructor(Long.TYPE, Integer.TYPE, Object.class, memorySegmentProxyClass);
        return (addr, size, att) -> {
          dbbCC.setAccessible(true);
          try {
            return dbbCC.newInstance(Long.valueOf(addr), Integer.valueOf(size), att, null);
          } catch (Exception e) {
            throw new IllegalStateException("Failed to create DirectByteBuffer", e);
          }
        };
      },
      // From Java 17 to 20
      () -> {
        Constructor<? extends ByteBuffer> dbbCC =
            (Constructor<? extends ByteBuffer>) Class.forName("java.nio.DirectByteBuffer")
                .getDeclaredConstructor(Long.TYPE, Integer.TYPE, Object.class);
        return (addr, size, att) -> {
          dbbCC.setAccessible(true);
          try {
            return dbbCC.newInstance(Long.valueOf(addr), Integer.valueOf(size), att);
          } catch (Exception e) {
            throw new IllegalStateException("Failed to create DirectByteBuffer", e);
          }
        };
      },
      // Java < 17
      () -> {
        Constructor<? extends ByteBuffer> dbbCC =
            (Constructor<? extends ByteBuffer>) Class.forName("java.nio.DirectByteBuffer")
                .getDeclaredConstructor(Long.TYPE, Integer.TYPE);
        return (addr, size, att) -> {
          dbbCC.setAccessible(true);
          try {
            return dbbCC.newInstance(Long.valueOf(addr), Integer.valueOf(size));
          } catch (Exception e) {
            throw new IllegalStateException("Failed to create DirectByteBuffer", e);
          }
        };
      }
  );

  private ByteBufferUtil() {
  }

  static {
    ByteBufferCreator creator = null;
    Exception firstException = null;
    for (CreatorSupplier supplier : _SUPPLIERS) {
      try {
        creator = supplier.createCreator();
      } catch (ClassNotFoundException | NoSuchMethodException e) {
        if (firstException == null) {
          firstException = e;
        }
      }
    }
    if (creator == null) {
      throw new IllegalStateException("Cannot find a way to instantiate DirectByteBuffer. "
          + "Please verify you are using a supported JVM", firstException);
    }
    CREATOR = creator;
  }

  public static ByteBuffer newDirectByteBuffer(long addr, int size, Object att) {
    return CREATOR.newDirectByteBuffer(addr, size, att);
  }

  private interface CreatorSupplier {
    ByteBufferCreator createCreator() throws ClassNotFoundException, NoSuchMethodException;
  }

  private interface ByteBufferCreator {
    ByteBuffer newDirectByteBuffer(long addr, int size, Object att);
  }
}
