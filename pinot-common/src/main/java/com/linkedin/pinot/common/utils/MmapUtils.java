/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.utils;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utilities to deal with memory mapped buffers, which may not be portable.
 *
 */
public class MmapUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(MmapUtils.class);
  private static final AtomicLong DIRECT_BYTE_BUFFER_USAGE = new AtomicLong(0L);
  private static final long BYTES_IN_MEGABYTE = 1024L * 1024L;
  private static final WeakHashMap<ByteBuffer, String> BUFFER_TO_CONTEXT_MAP = new WeakHashMap<>();

  /**
   * Unloads a byte buffer from memory
   * @param buffer The buffer to unload
   */
  public static void unloadByteBuffer(ByteBuffer buffer) {
    if (null == buffer)
      return;

    // Non direct byte buffers do not require any special handling, since they're on heap
    if (!buffer.isDirect())
      return;

    // Remove usage from direct byte buffer usage
    final int bufferSize = buffer.capacity();
    DIRECT_BYTE_BUFFER_USAGE.addAndGet(-bufferSize);
    LOGGER.info("Releasing byte buffer of size {} with context {}, currently allocated {} MB",
        bufferSize, BUFFER_TO_CONTEXT_MAP.get(buffer), DIRECT_BYTE_BUFFER_USAGE.get() / BYTES_IN_MEGABYTE);

    // A DirectByteBuffer can be cleaned up by doing buffer.cleaner().clean(), but this is not a public API. This is
    // probably not portable between JVMs.
    try {
      Method cleanerMethod = buffer.getClass().getMethod("cleaner");
      Method cleanMethod = Class.forName("sun.misc.Cleaner").getMethod("clean");

      cleanerMethod.setAccessible(true);
      cleanMethod.setAccessible(true);

      // buffer.cleaner().clean()
      Object cleaner = cleanerMethod.invoke(buffer);
      if (cleaner != null) {
        cleanMethod.invoke(cleaner);
      }
    } catch (Exception e) {
      LOGGER.warn("Caught (ignored) exception while unloading byte buffer", e);
    }
  }

  /**
   * Allocates a direct byte buffer, tracking usage information.
   *
   * @param capacity The capacity to allocate.
   * @param allocationContext The file that this byte buffer refers to
   * @param details Further details about the allocation
   * @return A new allocated byte buffer with the requested capacity
   */
  public static ByteBuffer allocateDirectByteBuffer(int capacity, File allocationContext, String details) {
    String context;
    if (allocationContext != null) {
      context = allocationContext.getAbsolutePath() + " (" + details + ")";
    } else {
      context = "no file (" + details + ")";
    }

    DIRECT_BYTE_BUFFER_USAGE.addAndGet(capacity);

    LOGGER.info("Allocating byte buffer of size {} with context {}, currently allocated {} MB", capacity,
        context, DIRECT_BYTE_BUFFER_USAGE.get() / BYTES_IN_MEGABYTE);

    final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(capacity);

    BUFFER_TO_CONTEXT_MAP.put(byteBuffer, context);

    return byteBuffer;
  }
}
