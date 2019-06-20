package org.apache.pinot.core.io.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.pinot.core.io.writer.impl.MutableOffHeapByteArrayStore;
import org.apache.pinot.core.io.writer.impl.MutableOffHeapByteArrayStore.Buffer;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


public class VarLengthBytesValueReaderWriter implements Closeable, ValueReader {

  private MutableOffHeapByteArrayStore.Buffer _buffer;

  // Used for memory-mapping while loading a segment
  public VarLengthBytesValueReaderWriter(PinotDataBuffer pinotDataBuffer) {
    PinotDataBuffer valuesBuffer = readHedader(pinotDataBuffer);
    _buffer = new Buffer(valuesBuffer);
  }

  // Used to create one while building a segment
  public VarLengthBytesValueReaderWriter(byte[][] arrays, File dictionaryFile, long size) throws IOException {
    long sizeWithHeader = size + getHeaderSize(arrays);
    PinotDataBuffer pinotDataBuffer = PinotDataBuffer
        .mapFile(dictionaryFile, false, 0, size, ByteOrder.BIG_ENDIAN,
            getClass().getSimpleName());
    writeHeader(pinotDataBuffer, arrays);
    _buffer = new Buffer(pinotDataBuffer);
    for (byte[] array : arrays) {
      _buffer.add(array);
    }
  }

  private int getHeaderSize(byte[][] arrays) {
    return 11;  // or whatever computed value for fixed/var sized headers, version etc..
  }

  private void writeHeader(PinotDataBuffer pinotDataBuffer, byte[][] arrays) {
    // write headers here, maybe header has some metadata about values.
  }

  private PinotDataBuffer readHedader(PinotDataBuffer pinotDataBuffer) {
    // slice the pinot databuffer afer reading headers, so that we just have the pinotdatabuffer
    // with the values.
    return null;
  }

  @Override
  public int getInt(int index) {
    return 0;
  }

  @Override
  public long getLong(int index) {
    return 0;
  }

  @Override
  public float getFloat(int index) {
    return 0;
  }

  @Override
  public double getDouble(int index) {
    return 0;
  }

  @Override
  public String getUnpaddedString(int index, int numBytesPerValue, byte paddingByte, byte[] buffer) {
    return null;
  }

  @Override
  public String getPaddedString(int index, int numBytesPerValue, byte[] buffer) {
    return null;
  }

  @Override
  public byte[] getBytes(int index, int numBytesPerValue, byte[] buffer) {
    return new byte[0];
  }

  @Override
  public void close() throws IOException {

  }
}
