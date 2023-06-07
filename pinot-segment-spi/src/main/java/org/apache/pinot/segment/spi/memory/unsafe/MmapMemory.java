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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.function.BiConsumer;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.posix.MSyncFlag;
import net.openhft.posix.PosixAPI;
import org.apache.pinot.segment.spi.utils.JavaVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link Memory} that whose bytes are mapped on a file.
 */
public class MmapMemory implements Memory {
  private static final Logger LOGGER = LoggerFactory.getLogger(MmapMemory.class);

  private static final MapFun MAP_FUN;

  /**
   * The address that correspond to the offset given at creation time.
   *
   * The actual mapping address may be smaller than this value, as usually memory map must start on an address that is
   * page aligned.
   */
  private final long _address;
  /**
   * How many bytes have been requested to be mapped.
   * The actual mapped size may be larger (up to the next page), but the actual mapped size
   * is stored by {@link #_section}.
   */
  private final long _size;
  private final MapSection _section;
  private boolean _closed = false;

  static {
    try {
      Jvm.init();
      MAP_FUN = MapFun.find();
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  public MmapMemory(File file, boolean readOnly, long offset, long size) {
    _size = size;

    try {
      _section = MAP_FUN.map(file, readOnly, offset, size);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    _address = _section.getAddress();
  }

  @Override
  public long getAddress() {
    return _address;
  }

  @Override
  public long getSize() {
    return _size;
  }

  @Override
  public void flush() {
    MSyncFlag mode = MSyncFlag.MS_SYNC;
    PosixAPI.posix().msync(_address, _size, mode);
  }

  @Override
  public void close()
      throws IOException {
    try {
      if (!_closed) {
        synchronized (this) {
          if (!_closed) {
            _section._unmapFun.unmap();
            _closed = true;
          }
        }
      }
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException("Error while calling unmap", e);
    }
  }

  @Override
  protected void finalize()
      throws Throwable {
    if (!_closed) {
      LOGGER.warn("Mmap section of " + _size + " wasn't explicitly closed");
      close();
    }
    super.finalize();
  }

  private static class MapSection {
    public static final MapSection EMPTY = new MapSection(0, () -> {
    });
    private final long _address;
    private final UnmapFun _unmapFun;

    public MapSection(long address, UnmapFun unmapFun) {
      _address = address;
      _unmapFun = unmapFun;
    }

    public long getAddress() {
      return _address;
    }

    public UnmapFun getUnmapFun() {
      return _unmapFun;
    }
  }

  /**
   * This is a factory method that can be used to create {@link MapSection}s.
   *
   * Each JVM may provide different method to map files in memory.
   */
  interface MapFun {

    /**
     * @param file The file to be mapped. If its length is lower than offset + size and the mode is not read only,
     *            the file will be resized to that size.
     * @param offset The offset in the file. Any positive value is valid, even if it is larger than the file size.
     * @param size How many bytes to map.
     * @throws IOException in several situations. For example, if the offset + size is larger than file length and the
     * mode is read only or if the process doesn't have permission to read or modify the file.
     */
    MapSection map(File file, boolean readOnly, long offset, long size) throws IOException;

    static MapFun find()
        throws ClassNotFoundException, NoSuchMethodException {
      List<Finder<? extends MapFun>> candidates = Lists.newArrayList(
          new Map0Fun.ChronicleCore(),
          new Map0Fun.Java11(),
          new Map0Fun.Java17(),
          new Map0Fun.Java20()
      );

      for (Finder<? extends MapFun> candidate : candidates) {
        try {
          return candidate.tryFind();
        } catch (NoSuchMethodException | ClassNotFoundException | AssertionError e) {
          // IGNORE
        }
      }
      throw new NoSuchMethodException("Cannot find how to create memory map files in Java " + JavaVersion.VERSION);
    }
  }

  /**
   * As defined by POSIX, the map0 method requires that the offset is page aligned. Failing to do that may produce
   * segfault errors. This interface is a {@link MapFun} that does some sanitation before calling the map method.
   * They include:
   * <ul>
   *   <li>Grow the file if the last mapped byte is larger than the file length.</li>
   *   <li>Align the offset with the previous page. This means that we need to correct the actual mapped address.</li>
   * </ul>
   */
  interface Map0Fun extends MapFun {

    /**
     * @param pageAlignedOffset It has to be a positive value that is page aligned.
     */
    MapSection map0(FileChannel fc, boolean readOnly, long pageAlignedOffset, long size)
        throws InvocationTargetException, IllegalAccessException, IOException;

    default MapSection map(File file, boolean readOnly, long offset, long size) throws IOException {
      String mode = readOnly ? "r" : "rw";
      try (RandomAccessFile raf = new RandomAccessFile(file, mode); FileChannel fc = raf.getChannel()) {
        if (size == 0) {
          return MapSection.EMPTY;
        }

        long allocationGranule = Unsafer.UNSAFE.pageSize();
        int pagePosition = (int) (offset % allocationGranule);

        // Compute mmap address
        if (!fc.isOpen()) {
          throw new IOException("closed " + file.getPath());
        }

        long fileSize = fc.size();
        if (fileSize < offset + size) {
          // If file size is smaller than the specified size, extend the file size
          raf.seek(offset + size - 1);
          raf.write(0);
        }
        long mapPosition = offset - pagePosition;
        long mapSize = size + pagePosition;

        MapSection map0Section = map0(fc, readOnly, mapPosition, mapSize);
        return new MapSection(map0Section.getAddress() + pagePosition, map0Section.getUnmapFun());
      } catch (InvocationTargetException | IllegalAccessException e) {
        throw new RuntimeException("Cannot map file " + file + " from address " + offset + " with size " + size, e);
      }
    }

    static BiConsumer<Long, Long> tryFindUnmapper()
        throws NoSuchMethodException, ClassNotFoundException {
      Class<?> fileChannelImpl = MmapMemory.class.getClassLoader().loadClass("sun.nio.ch.FileChannelImpl");
      Method unmapMethod = fileChannelImpl.getDeclaredMethod("unmap0", long.class, long.class);
      unmapMethod.setAccessible(true);
      return (address, size) -> {
        try {
          unmapMethod.invoke(null, address, size);
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      };
    }

    /**
     * Instead of looking for the correct map method by our self, this finder delegates on
     * {@link OS#map(FileChannel, FileChannel.MapMode, long, long)} and {@link OS#unmap(long, long)}, which internally
     * does the same thing.
     */
    class ChronicleCore implements Finder<Map0Fun> {
      @Override
      public Map0Fun tryFind() {
        OS.mapAlignment();
        return (fc, readOnly, pageAlignedOffset, size) -> {
          if (size == 0) {
            return MapSection.EMPTY;
          }
          FileChannel.MapMode mapMode = readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE;
          long alignedSize = OS.pageAlign(size);
          long address = OS.map(fc, mapMode, pageAlignedOffset, alignedSize);
          return new MapSection(address, () -> {
            try {
              OS.unmap(address, alignedSize);
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
        };
      }
    }

    class Java11 implements Finder<Map0Fun> {
      @Override
      public Map0Fun tryFind()
          throws NoSuchMethodException, ClassNotFoundException {
        Class<?> fileChannelImpl = MmapMemory.class.getClassLoader().loadClass("sun.nio.ch.FileChannelImpl");

        // this is the usual method in JDK 11
        Method mapMethod = fileChannelImpl.getDeclaredMethod("map0", int.class, long.class, long.class);
        mapMethod.setAccessible(true);

        BiConsumer<Long, Long> unmap0 = tryFindUnmapper();

        return (fc, readOnly, pageAlignedOffset, size) -> {
          // see FileChannelImpl.MAP_RO and MAP_RW
          int iMode = readOnly ? 0 : 1;

          long address = (long) mapMethod.invoke(fc, iMode, pageAlignedOffset, size);

          UnmapFun unmapFun = () -> unmap0.accept(address, size);

          return new MapSection(address, unmapFun);
        };
      }
    }

    class Java17 implements Finder<Map0Fun> {
      @Override
      public Map0Fun tryFind()
          throws NoSuchMethodException, ClassNotFoundException {
        Class<?> fileChannelImpl = MmapMemory.class.getClassLoader().loadClass("sun.nio.ch.FileChannelImpl");

        // https://github.com/openjdk/jdk17/blob/dfacda488bfbe2e11e8d607a6d08527710286982/src/java.base/share/classes/
        // sun/nio/ch/FileChannelImpl.java#L1341
        Method mapMethod = fileChannelImpl.getDeclaredMethod("map0", int.class, long.class, long.class, boolean.class);
        mapMethod.setAccessible(true);

        BiConsumer<Long, Long> unmap0 = tryFindUnmapper();

        return (fc, readOnly, pageAlignedOffset, size) -> {
          // see FileChannelImpl.MAP_RO and MAP_RW
          int iMode = readOnly ? 0 : 1;
          long address = (long) mapMethod.invoke(fc, iMode, pageAlignedOffset, size, false);

          UnmapFun unmapFun = () -> unmap0.accept(address, size);

          return new MapSection(address, unmapFun);
        };
      }
    }

    /**
     * In Java 20 the method used to map already does the alignment corrections, so we could call it with a non-aligned
     * offset. But we need to know the position in the page in order to correct the address, so it is useful to return a
     * Map0Fun instead of a MapFun.
     */
    class Java20 implements Finder<Map0Fun> {
      @Override
      public Map0Fun tryFind()
          throws NoSuchMethodException, ClassNotFoundException {
        Class<?> fileChannelImpl = MmapMemory.class.getClassLoader().loadClass("sun.nio.ch.FileChannelImpl");

        Method mapMethod = fileChannelImpl.getDeclaredMethod("mapInternal", FileChannel.MapMode.class, long.class,
            long.class, int.class, boolean.class);
        mapMethod.setAccessible(true);

        Class<?> unmapperClass = MmapMemory.class.getClassLoader().loadClass("sun.nio.ch.FileChannelImpl$Unmapper");
        Method unmapMethod = unmapperClass.getDeclaredMethod("unmap");
        unmapMethod.setAccessible(true);
        Method addressMethod = unmapperClass.getDeclaredMethod("address");
        addressMethod.setAccessible(true);

        return (fc, readOnly, pageAlignedOffset, size) -> {
          FileChannel.MapMode mapMode = readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE;
          // see https://github.com/openjdk/jdk/blob/cc9f7ad9ce33dc44d335fb7fb5483795c62ba936/src/java.base/share/
          // classes/sun/nio/ch/FileChannelImpl.java#L1223
          int prot = readOnly ? 0 : 1;

          Object unmapper = mapMethod.invoke(fc, mapMode, pageAlignedOffset, size, prot, false);
          long address;
          UnmapFun unmapFun;
          if (unmapper == null) {
            // unmapper may be null if the size is 0 or if the file descriptor is closed while mapInternal was called
            address = 0;
            unmapFun = () -> {
            };
          } else {
            address = (long) addressMethod.invoke(unmapper);
            unmapFun = () -> unmapMethod.invoke(unmapper);
          }

          return new MapSection(address, unmapFun);
        };
      }
    }
  }

  interface UnmapFun {
    void unmap()
        throws InvocationTargetException, IllegalAccessException;
  }

  private interface Finder<C> {
    C tryFind() throws NoSuchMethodException, ClassNotFoundException;
  }
}
