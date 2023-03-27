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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.pinot.segment.spi.utils.JavaVersion;


public class MmapMemory implements Memory {

  private static final MapFun MAP_FUN;

  private final long _address;
  private final long _size;
  private final MapSection _section;

  static {
    try {
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
    // TODO
  }

  @Override
  public void close()
      throws IOException {
    try {
      _section._unmapFun.unmap();
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException("Error while calling unmap", e);
    }
  }

  private static class MapSection {
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
          new Map0Fun.Java11(),
          new Map0Fun.Java17(),
          new MapFun.Java20()
      );

      for (Finder<? extends MapFun> candidate : candidates) {
        try {
          return candidate.tryFind();
        } catch (NoSuchMethodException | ClassNotFoundException e) {
          // IGNORE
        }
      }
      throw new NoSuchMethodException("Cannot find how to create memory map files in Java " + JavaVersion.VERSION);
    }

    class Java20 implements Finder<MapFun> {
      @Override
      public MapFun tryFind()
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

        return (file, readOnly, offset, size) -> {
          FileChannel.MapMode mapMode = readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE;
          // see https://github.com/openjdk/jdk/blob/1330d4eaa54790b468f69e61574b3c5d522be120/src/java.base/share/
          // classes/sun/nio/ch/FileChannelImpl.java#L1361
          int prot = readOnly ? 0 : 1;

          String mode = readOnly ? "r" : "rw";
          try (RandomAccessFile raf = new RandomAccessFile(file, mode); FileChannel fc = raf.getChannel()) {
            Object unmapper = mapMethod.invoke(fc, mapMode, offset, size, prot, false);
            long address = (long) addressMethod.invoke(unmapper);

            UnmapFun unmapFun = () -> unmapMethod.invoke(unmapper);

            return new MapSection(address, unmapFun);
          } catch (InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        };
      }
    }
  }

  /**
   * A {@link MapFun} that actually delegates into a map0 native method included in pre 19 Java releases.
   *
   * Unlike normal map methods, map0 actually has some low level requirements. For example, the offset must be page
   * aligned.
   */
  interface Map0Fun extends MapFun {

    /**
     * @param offset It has to be a positive value that is page aligned.
     */
    MapSection map0(FileChannel fc, boolean readOnly, long offset, long size)
        throws InvocationTargetException, IllegalAccessException;

    default MapSection map(File file, boolean readOnly, long offset, long size) throws IOException {
      String mode = readOnly ? "r" : "rw";
      try (RandomAccessFile raf = new RandomAccessFile(file, mode); FileChannel fc = raf.getChannel()) {
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
          //logger.trace(s"extend file size to ${fc.size}")
        }
        long mapPosition = offset - pagePosition;
        long mapSize = size + pagePosition;
        // A workaround for the error when calling fc.map(MapMode.READ_WRITE, offset, size) with size more than 2GB

        MapSection map0Section = map0(fc, readOnly, mapPosition, mapSize);
        return new MapSection(map0Section.getAddress() + pagePosition, map0Section.getUnmapFun());
      } catch (InvocationTargetException | IllegalAccessException e) {
        throw new RuntimeException("Cannot map ");
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

    class Java11 implements Finder<Map0Fun> {
      @Override
      public Map0Fun tryFind()
          throws NoSuchMethodException, ClassNotFoundException {
        Class<?> fileChannelImpl = MmapMemory.class.getClassLoader().loadClass("sun.nio.ch.FileChannelImpl");

        // this is the usual method in JDK 11
        Method mapMethod = fileChannelImpl.getDeclaredMethod("map0", int.class, long.class, long.class);
        mapMethod.setAccessible(true);

        BiConsumer<Long, Long> unmap0 = tryFindUnmapper();

        return (fc, readOnly, offset, size) -> {
          // see FileChannelImpl.MAP_RO and MAP_RW
          int iMode = readOnly ? 0 : 1;

          long address = (long) mapMethod.invoke(fc, iMode, offset, size);

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

        return (fc, readOnly, offset, size) -> {
          // see FileChannelImpl.MAP_RO and MAP_RW
          int iMode = readOnly ? 0 : 1;
          long address = (long) mapMethod.invoke(fc, iMode, offset, size, false);

          UnmapFun unmapFun = () -> unmap0.accept(address, size);

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
