package com.linkedin.pinot.common.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;


/**
 * Utilities to deal with memory mapped buffers, which may not be portable.
 *
 * @author jfim
 */
public class MmapUtils {
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
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
  }
}
