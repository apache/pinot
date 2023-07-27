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

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexByteRange;
import org.apache.pinot.segment.spi.memory.unsafe.UnsafePinotBufferFactory;
import org.apache.pinot.segment.spi.utils.JavaVersion;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class <code>PinotDataBuffer</code> is the byte buffer for Pinot data that resides in off-heap memory.
 *
 * <p>The byte buffer may be memory mapped (MMAP) or direct allocated (DIRECT).
 * <p>Supports buffers larger than 2GB.
 * <p>This class will also track the number and memory usage of the buffers.
 * <p>NOTE: All the accesses to the buffer are unchecked for performance reason. Behavior of accessing buffer with
 * invalid index is undefined.
 * <p>Backward-compatible:
 * <ul>
 *   <li>Index file (forward index, inverted index, dictionary) is always big-endian</li>
 *   <li>Star-tree file is always little-endian</li>
 *   <li>Temporary buffer should be allocated using native-order for performance</li>
 * </ul>
 */
@ThreadSafe
public abstract class PinotDataBuffer implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotDataBuffer.class);

  public static final ByteOrder NATIVE_ORDER = ByteOrder.nativeOrder();
  public static final ByteOrder NON_NATIVE_ORDER =
      NATIVE_ORDER == ByteOrder.BIG_ENDIAN ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
  // We use this threshold to decide whether we use bulk bytes processing or not
  // With number of bytes less than this threshold, we get/put bytes one by one
  // With number of bytes more than this threshold, we create a ByteBuffer from the buffer and use bulk get/put method
  public static final int BULK_BYTES_PROCESSING_THRESHOLD = 10;

  private static class BufferContext {
    enum Type {
      DIRECT, MMAP
    }

    final Type _type;
    final long _size;
    final String _filePath;
    final String _description;

    BufferContext(Type type, long size, @Nullable String filePath, @Nullable String description) {
      _type = type;
      _size = size;
      _filePath = filePath;
      _description = description;
    }

    @Override
    public String toString() {
      String context = "Type: " + _type + ", Size: " + _size;
      if (_filePath != null) {
        context += ", File Path: " + _filePath;
      }
      if (_description != null) {
        context += ", Description: " + _description;
      }
      return context;
    }
  }

  private static final AtomicLong DIRECT_BUFFER_COUNT = new AtomicLong();
  private static final AtomicLong DIRECT_BUFFER_USAGE = new AtomicLong();
  private static final AtomicLong MMAP_BUFFER_COUNT = new AtomicLong();
  private static final AtomicLong MMAP_BUFFER_USAGE = new AtomicLong();
  private static final AtomicLong ALLOCATION_FAILURE_COUNT = new AtomicLong();
  private static final Map<PinotDataBuffer, BufferContext> BUFFER_CONTEXT_MAP = new WeakHashMap<>();

  /**
   * Configuration key used to change the offheap buffer factory used by Pinot.
   * Value should be the qualified path of a class that extends {@link PinotBufferFactory} and has empty
   * constructor.
   */
  private static final String OFFHEAP_BUFFER_FACTORY_CONFIG = "pinot.offheap.buffer.factory";
  /**
   * Boolean configuration that decides whether to allocate using {@link ByteBufferPinotBufferFactory} when the buffer
   * to allocate fits in a {@link ByteBuffer}.
   *
   * Defaults to true.
   */
  private static final String OFFHEAP_BUFFER_PRIORITIZE_BYTE_BUFFER_CONFIG = "pinot.offheap.prioritize.bytebuffer";

  /**
   * The default {@link PinotBufferFactory} used by all threads that do not define their own factory.
   */
  private static PinotBufferFactory _defaultFactory = createDefaultFactory();
  /**
   * A thread local variable that can be used to customize the {@link PinotBufferFactory} used on tests. This is mostly
   * useful in tests.
   */
  private static final ThreadLocal<PinotBufferFactory> _FACTORY = new ThreadLocal<>();

  /**
   * Change the {@link PinotBufferFactory} used by the current thread.
   *
   * If this method is not called, the default factory configured at startup time will be used.
   *
   * @see #loadDefaultFactory(PinotConfiguration)
   */
  public static void useFactory(PinotBufferFactory factory) {
    _FACTORY.set(factory);
  }

  /**
   * Returns the factory the current thread should use.
   */
  public static PinotBufferFactory getFactory() {
    PinotBufferFactory pinotBufferFactory = _FACTORY.get();
    if (pinotBufferFactory == null) {
      pinotBufferFactory = _defaultFactory;
    }
    return pinotBufferFactory;
  }

  public static PinotBufferFactory createDefaultFactory() {
    return createDefaultFactory(true);
  }

  public static PinotBufferFactory createDefaultFactory(boolean prioritizeByteBuffer) {
    String factoryClassName;
    if (JavaVersion.VERSION < 16) {
      LOGGER.info("Using LArray as buffer on JVM version {}", JavaVersion.VERSION);
      factoryClassName = LArrayPinotBufferFactory.class.getCanonicalName();
    } else {
      LOGGER.info("Using Unsafe as buffer on JVM version {}", JavaVersion.VERSION);
      factoryClassName = UnsafePinotBufferFactory.class.getCanonicalName();
    }
    return createFactory(factoryClassName, prioritizeByteBuffer);
  }

  private static PinotBufferFactory createFactory(String factoryClassName, boolean prioritizeByteBuffer) {
    try {
      LOGGER.info("Instantiating Pinot buffer factory class {}", factoryClassName);
      PinotBufferFactory factory = PluginManager.get().createInstance(factoryClassName);

      if (prioritizeByteBuffer) {
        factory = new SmallWithFallbackPinotBufferFactory(new ByteBufferPinotBufferFactory(), factory);
      }

      return factory;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Configures the default {@link PinotBufferFactory}.
   *
   * This method guarantees that threads that didn't use the factory before this method is called are going to use the
   * new factory. In other words, threads that were already running when this method was called may use other factories.
   * Therefore it is recommended to call this method during Pinot startup.
   */
  public static void loadDefaultFactory(PinotConfiguration configuration) {
    boolean prioritizeByteBuffer = configuration.getProperty(OFFHEAP_BUFFER_PRIORITIZE_BYTE_BUFFER_CONFIG, true);
    String factoryClassName = configuration.getProperty(OFFHEAP_BUFFER_FACTORY_CONFIG);
    if (factoryClassName != null) {
      _defaultFactory = createFactory(factoryClassName, prioritizeByteBuffer);
    } else {
      LOGGER.info("No custom Pinot buffer factory class found in configuration. Using default factory with "
          + "prioritize bytebuffer = {}", prioritizeByteBuffer);
      _defaultFactory = createDefaultFactory(prioritizeByteBuffer);
    }
  }

  /**
   * Allocates a buffer using direct memory.
   * <p>NOTE: The contents of the allocated buffer are not defined.
   *
   * @param size The size of the buffer
   * @param byteOrder The byte order of the buffer (big-endian or little-endian)
   * @param description The description of the buffer
   * @return The buffer allocated
   */
  public static PinotDataBuffer allocateDirect(long size, ByteOrder byteOrder, @Nullable String description) {
    PinotDataBuffer buffer;
    try {
      buffer = getFactory().allocateDirect(size, byteOrder);
    } catch (Exception e) {
      LOGGER.error("Caught exception while allocating direct buffer of size: {} with description: {}", size,
          description, e);
      LOGGER.error("Buffer stats: {}", getBufferStats());
      ALLOCATION_FAILURE_COUNT.getAndIncrement();
      throw e;
    }
    DIRECT_BUFFER_COUNT.getAndIncrement();
    DIRECT_BUFFER_USAGE.getAndAdd(size);
    synchronized (BUFFER_CONTEXT_MAP) {
      BUFFER_CONTEXT_MAP.put(buffer, new BufferContext(BufferContext.Type.DIRECT, size, null, description));
    }
    return buffer;
  }

  /**
   * Allocates a buffer using direct memory and loads a file into the buffer.
   */
  public static PinotDataBuffer loadFile(File file, long offset, long size, ByteOrder byteOrder,
      @Nullable String description)
      throws IOException {
    PinotDataBuffer buffer;
    try {
      buffer = getFactory().readFile(file, offset, size, byteOrder);
    } catch (Exception e) {
      LOGGER.error("Caught exception while loading file: {} from offset: {} of size: {} with description: {}",
          file.getAbsolutePath(), offset, size, description, e);
      LOGGER.error("Buffer stats: {}", getBufferStats());
      ALLOCATION_FAILURE_COUNT.getAndIncrement();
      throw e;
    }
    DIRECT_BUFFER_COUNT.getAndIncrement();
    DIRECT_BUFFER_USAGE.getAndAdd(size);
    synchronized (BUFFER_CONTEXT_MAP) {
      BUFFER_CONTEXT_MAP.put(buffer,
          new BufferContext(BufferContext.Type.DIRECT, size, file.getAbsolutePath().intern(), description));
    }
    return buffer;
  }

  /**
   * Allocates a buffer using direct memory and loads a big-endian file into the buffer.
   */
  @VisibleForTesting
  public static PinotDataBuffer loadBigEndianFile(File file)
      throws IOException {
    return loadFile(file, 0, file.length(), ByteOrder.BIG_ENDIAN, null);
  }

  /**
   * Memory maps a file into a buffer.
   * <p>NOTE: If the file gets extended, the contents of the extended portion of the file are not defined.
   */
  public static PinotDataBuffer mapFile(File file, boolean readOnly, long offset, long size, ByteOrder byteOrder,
      @Nullable String description)
      throws IOException {
    PinotDataBuffer buffer;
    try {
      buffer = getFactory().mapFile(file, readOnly, offset, size, byteOrder);
    } catch (Exception e) {
      LOGGER.error("Caught exception while mapping file: {} from offset: {} of size: {} with description: {}",
          file.getAbsolutePath(), offset, size, description, e);
      LOGGER.error("Buffer stats: {}", getBufferStats());
      ALLOCATION_FAILURE_COUNT.getAndIncrement();
      throw e;
    }
    MMAP_BUFFER_COUNT.getAndIncrement();
    MMAP_BUFFER_USAGE.getAndAdd(size);
    synchronized (BUFFER_CONTEXT_MAP) {
      BUFFER_CONTEXT_MAP
          .put(buffer, new BufferContext(BufferContext.Type.MMAP, size, file.getAbsolutePath().intern(), description));
    }
    return buffer;
  }

  /**
   * Memory maps a read-only big-endian file into a buffer.
   */
  @VisibleForTesting
  public static PinotDataBuffer mapReadOnlyBigEndianFile(File file)
      throws IOException {
    return mapFile(file, true, 0, file.length(), ByteOrder.BIG_ENDIAN, null);
  }

  public static long getDirectBufferCount() {
    return DIRECT_BUFFER_COUNT.get();
  }

  public static long getDirectBufferUsage() {
    return DIRECT_BUFFER_USAGE.get();
  }

  public static long getMmapBufferCount() {
    return MMAP_BUFFER_COUNT.get();
  }

  public static long getMmapBufferUsage() {
    return MMAP_BUFFER_USAGE.get();
  }

  public static long getAllocationFailureCount() {
    return ALLOCATION_FAILURE_COUNT.get();
  }

  @VisibleForTesting
  protected static void cleanStats() {
    DIRECT_BUFFER_COUNT.set(0);
    DIRECT_BUFFER_USAGE.set(0);
    MMAP_BUFFER_COUNT.set(0);
    MMAP_BUFFER_USAGE.set(0);
    ALLOCATION_FAILURE_COUNT.set(0);
  }

  public static List<String> getBufferInfo() {
    synchronized (BUFFER_CONTEXT_MAP) {
      List<String> bufferInfo = new ArrayList<>(BUFFER_CONTEXT_MAP.size());
      for (BufferContext bufferContext : BUFFER_CONTEXT_MAP.values()) {
        bufferInfo.add(bufferContext.toString());
      }
      return bufferInfo;
    }
  }

  private static String getBufferStats() {
    return String
        .format("Direct buffer count: %s, size: %s; Mmap buffer count: %s, size: %s", DIRECT_BUFFER_COUNT.get(),
            DIRECT_BUFFER_USAGE.get(), MMAP_BUFFER_COUNT.get(), MMAP_BUFFER_USAGE.get());
  }

  private volatile boolean _closeable;

  protected PinotDataBuffer(boolean closeable) {
    _closeable = closeable;
  }

  @Override
  public synchronized void close()
      throws IOException {
    if (_closeable) {
      flush();
      release();
      BufferContext bufferContext;
      synchronized (BUFFER_CONTEXT_MAP) {
        bufferContext = BUFFER_CONTEXT_MAP.remove(this);
      }
      if (bufferContext != null) {
        if (bufferContext._type == BufferContext.Type.DIRECT) {
          DIRECT_BUFFER_COUNT.getAndDecrement();
          DIRECT_BUFFER_USAGE.getAndAdd(-bufferContext._size);
        } else {
          MMAP_BUFFER_COUNT.getAndDecrement();
          MMAP_BUFFER_USAGE.getAndAdd(-bufferContext._size);
        }
      }
      _closeable = false;
    }
  }

  public boolean isPrefetchable() {
    return false;
  }

  public void prefetch(long baseOffset, List<ForwardIndexByteRange> byteRanges) {
  }

  public byte getByte(int offset) {
    return getByte((long) offset);
  }

  public abstract byte getByte(long offset);

  public void putByte(int offset, byte value) {
    putByte((long) offset, value);
  }

  public abstract void putByte(long offset, byte value);

  public char getChar(int offset) {
    return getChar((long) offset);
  }

  public abstract char getChar(long offset);

  public void putChar(int offset, char value) {
    putChar((long) offset, value);
  }

  public abstract void putChar(long offset, char value);

  public short getShort(int offset) {
    return getShort((long) offset);
  }

  public abstract short getShort(long offset);

  public void putShort(int offset, short value) {
    putShort((long) offset, value);
  }

  public abstract void putShort(long offset, short value);

  public int getInt(int offset) {
    return getInt((long) offset);
  }

  public abstract int getInt(long offset);

  public void putInt(int offset, int value) {
    putInt((long) offset, value);
  }

  public abstract void putInt(long offset, int value);

  public long getLong(int offset) {
    return getLong((long) offset);
  }

  public abstract long getLong(long offset);

  public void putLong(int offset, long value) {
    putLong((long) offset, value);
  }

  public abstract void putLong(long offset, long value);

  public float getFloat(int offset) {
    return getFloat((long) offset);
  }

  public abstract float getFloat(long offset);

  public void putFloat(int offset, float value) {
    putFloat((long) offset, value);
  }

  public abstract void putFloat(long offset, float value);

  public double getDouble(int offset) {
    return getDouble((long) offset);
  }

  public abstract double getDouble(long offset);

  public void putDouble(int offset, double value) {
    putDouble((long) offset, value);
  }

  public abstract void putDouble(long offset, double value);

  /**
   * Given an array of bytes, copies the content of this object into the array of bytes.
   * The first byte to be copied is the one that could be read with {@code this.getByte(offset)}
   */
  public void copyTo(long offset, byte[] buffer, int destOffset, int size) {
    if (size <= BULK_BYTES_PROCESSING_THRESHOLD) {
      int end = destOffset + size;
      for (int i = destOffset; i < end; i++) {
        buffer[i] = getByte(offset++);
      }
    } else {
      toDirectByteBuffer(offset, size).get(buffer, destOffset, size);
    }
  }

  /**
   * Given an array of bytes, copies the content of this object into the array of bytes.
   * The first byte to be copied is the one that could be read with {@code this.getByte(offset)}
   */
  public void copyTo(long offset, byte[] buffer) {
    copyTo(offset, buffer, 0, buffer.length);
  }

  /**
   * Note: It is the responsibility of the caller to make sure arguments are checked before the methods are called.
   * While some rudimentary checks are performed on the input, the checks are best effort and when performance is an
   * overriding priority, as when methods of this class are optimized by the runtime compiler, some or all checks
   * (if any) may be elided. Hence, the caller must not rely on the checks and corresponding exceptions!
   */
  public void copyTo(long offset, PinotDataBuffer buffer, long destOffset, long size) {
    int pageSize = Integer.MAX_VALUE;
    long alreadyCopied = 0;

    while (size - alreadyCopied > 0L) {
      int step;
      long remaining = size - alreadyCopied;

      if (remaining > pageSize) {
        step = pageSize;
      } else {
        step = (int) remaining;
      }
      ByteBuffer destBb = buffer.toDirectByteBuffer(destOffset + alreadyCopied, step);
      ByteBuffer myView = toDirectByteBuffer(offset + alreadyCopied, step);

      destBb.put(myView);

      alreadyCopied += step;
    }
  }

  /**
   * Given an array of bytes, writes the content in the specified position.
   */
  public void readFrom(long offset, byte[] buffer, int srcOffset, int size) {
    assert offset <= Integer.MAX_VALUE;
    int intOffset = (int) offset;

    if (size <= BULK_BYTES_PROCESSING_THRESHOLD) {
      int end = srcOffset + size;
      for (int i = srcOffset; i < end; i++) {
        putByte(intOffset++, buffer[i]);
      }
    } else {
      toDirectByteBuffer(offset, size).put(buffer, srcOffset, size);
    }
  }

  public void readFrom(long offset, byte[] buffer) {
    readFrom(offset, buffer, 0, buffer.length);
  }

  public void readFrom(long offset, ByteBuffer buffer) {
    toDirectByteBuffer(offset, buffer.remaining()).put(buffer);
  }

  public void readFrom(long offset, File file, long srcOffset, long size)
      throws IOException {
    try (
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        FileChannel fileChannel = raf.getChannel()) {
      int step = Integer.MAX_VALUE / 2;
      while (size > Integer.MAX_VALUE) {
        ByteBuffer bb = toDirectByteBuffer(offset, step);
        fileChannel.read(bb, srcOffset);
        offset += step;
        srcOffset += step;
        size -= step;
      }
      ByteBuffer bb = toDirectByteBuffer(offset, (int) size);
      fileChannel.read(bb, srcOffset);
    }
  }

  public abstract long size();

  public abstract ByteOrder order();

  /**
   * Creates a view of the range [start, end) of this buffer with the given byte order. Calling {@link #flush()} or
   * {@link #close()} has no effect on view.
   */
  public abstract PinotDataBuffer view(long start, long end, ByteOrder byteOrder);

  /**
   * Creates a view of the range [start, end) of this buffer with the current byte order. Calling {@link #flush()} or
   * {@link #close()} has no effect on view.
   */
  public PinotDataBuffer view(long start, long end) {
    return view(start, end, order());
  }

  /**
   * Returns an ByteBuffer with the same content of this buffer.
   *
   * This receiver object and the returned ByteBuffer share the same memory address, but the receiver conserves the
   * ownership. This means that:
   * <ol>
   *   <li>The returned ByteBuffer should not be released (aka freed in C). For example, its cleaner should not be
   *   called. <b>Violations of this rule may produce segmentation faults</b></li>
   *   <li>The returned ByteBuffer should not be used once the receiver is released.
   *   <b>Violations of this rule may produce segmentation faults</b></li>
   *   <li>A write made by either the receiver or the returned ByteBuffer will be seen by the other.</li>
   * </ol>
   *
   * Depending on the implementation, this may be a view (and therefore changes on any buffer will be seen by the other)
   * or a copy (in which case the cost will be higher, but each copy will have their own lifecycle).
   *
   * @param byteOrder The byte order of the returned ByteBuffer. No special treatment is done if the order of the
   *                  receiver buffer is different from the order requested. In other words: if this buffer was written
   *                  in big endian and the direct buffer is requested in little endian, the integers read from each
   *                  buffer will be different.
   */
  public abstract ByteBuffer toDirectByteBuffer(long offset, int size, ByteOrder byteOrder);

  /**
   * Returns an ByteBuffer with the same content of this buffer.
   *
   * This receiver object and the returned ByteBuffer share the same memory address, but the receiver conserves the
   * ownership. This means that:
   * <ol>
   *   <li>The returned ByteBuffer should not be released (aka freed in C). For example, its cleaner should not be
   *   called. <b>Violations of this rule may produce segmentation faults</b></li>
   *   <li>The returned ByteBuffer should not be used once the receiver is released.
   *   <b>Violations of this rule may produce segmentation faults</b></li>
   *   <li>A write made by either the receiver or the returned ByteBuffer will be seen by the other.</li>
   * </ol>
   *
   * Depending on the implementation, this may be a view (and therefore changes on any buffer will be seen by the other)
   * or a copy (in which case the cost will be higher, but each copy will have their own lifecycle).
   *
   */
  // TODO: Most calls to this method are just used to then read the content of the buffer.
  //  This is unnecessary an generates 2-5 unnecessary objects. We should benchmark whether there is some advantage on
  //  transforming this buffer into a IntBuffer/LongBuffer/etc when reading sequentially
  public ByteBuffer toDirectByteBuffer(long offset, int size) {
    return toDirectByteBuffer(offset, size, order());
  }

  public abstract void flush();

  public abstract void release()
      throws IOException;

  public boolean isCloseable() {
    return _closeable;
  }

  protected static void checkLimits(long capacity, long offset, long size) {
    if (offset < 0) {
      throw new IllegalArgumentException("Offset " + offset + " cannot be negative");
    }
    if (size < 0) {
      throw new IllegalArgumentException("Size " + size + " cannot be negative");
    }
    if (offset + size > capacity) {
      throw new IllegalArgumentException("Size (" + size + ") + offset (" + offset + ") exceeds the capacity of "
          + capacity);
    }
  }
}
