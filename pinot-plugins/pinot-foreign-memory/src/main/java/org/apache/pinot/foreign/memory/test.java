package org.apache.pinot.foreign.memory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;



public class test {

  public static final int VERSION = 1 ;
//  static {
//    Jvm.init();
//    VERSION = Jvm.majorVersion();
//  }
  public static void main (String[] args){
    System.out.println("yolo");
//    System.out.println("this is java version:" + VERSION);
    String s = "My string";
    MemorySegment nativeText;
    try (Arena arena = Arena.ofConfined()) {
      System.out.println(System.getenv("PINOT_OFFHEAP_SKIP_BYTEBUFFER"));
      System.out.println(System.getenv("PINOT_BUFFER_LIBRARY"));
//

      // Allocate off-heap memory
      nativeText = arena.allocateFrom(s);
//      BigEndianForeignMemoryPinotDataBuffer buffer = new BigEndianForeignMemoryPinotDataBuffer(nativeText, arena);
//      System.out.println(buffer.getOfChar());
//      buffer.close();
//      buffer.flush();
    } catch (Exception ex) {
      System.out.println("this is exception: " + ex);
    }


  }
}
