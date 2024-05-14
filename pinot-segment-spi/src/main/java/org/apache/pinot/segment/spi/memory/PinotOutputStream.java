package org.apache.pinot.segment.spi.memory;

import com.google.common.primitives.Longs;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;


public abstract class PinotOutputStream extends SeekableOutputStream implements DataOutput {

  @Override
  public void writeBoolean(boolean v) throws IOException {
    write(v ? 1 : 0);
  }

  @Override
  public void writeByte(int v) throws IOException {
    write(v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    writeShort(v);
  }

  @Override
  public void writeChars(String s) throws IOException {
    for (int i = 0; i < s.length(); i++) {
      writeChar(s.charAt(i));
    }
  }

  public void writeDouble(double v) throws IOException {
    writeLong(Double.doubleToLongBits(v));
  }

  public void writeFloat(float v) throws IOException {
    writeInt(Float.floatToIntBits(v));
  }

  public void writeInt(int v) throws IOException {
    write(0xFF & v);
    write(0xFF & (v >> 8));
    write(0xFF & (v >> 16));
    write(0xFF & (v >> 24));
  }

  public void writeLong(long v) throws IOException {
    byte[] bytes = Longs.toByteArray(Long.reverseBytes(v));
    write(bytes, 0, bytes.length);
  }

  public void writeShort(int v) throws IOException {
    write(0xFF & v);
    write(0xFF & (v >> 8));
  }

  /**
   * This method is commonly used in Pinot to write a string with a 4-byte length prefix.
   * <p>
   * <b>Note</b>: This method is incompatible with {@link DataOutput#writeUTF(String)}. It is recommended to use
   * this method instead of the one in {@link DataOutput} to write strings.
   */
  public void writeInt4String(String v) throws IOException {
    writeInt(v.length());
    write(v.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void writeBytes(String s)
      throws IOException {
    new DataOutputStream(this).writeBytes(s);
  }

  @Override
  public void writeUTF(String s)
      throws IOException {
    new DataOutputStream(this).writeUTF(s);
  }

  public void write(DataBuffer input)
      throws IOException {
    write(input, 0, input.size());
  }

  public void write(DataBuffer input, long offset, long length)
      throws IOException {
    byte[] bytes = new byte[4096];
    long currentOffset = offset;
    while (currentOffset < offset + length) {
      int bytesToRead = (int) Math.min(length - (currentOffset - offset), bytes.length);
      input.copyTo(currentOffset, bytes, 0, bytesToRead);
      write(bytes, 0, bytesToRead);
      currentOffset += bytesToRead;
    }
  }
}
