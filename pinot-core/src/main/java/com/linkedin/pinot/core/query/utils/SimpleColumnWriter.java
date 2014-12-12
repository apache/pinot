package com.linkedin.pinot.core.query.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;


public class SimpleColumnWriter {
  public static void main(String[] arg) throws IOException {
    byte[] byteArray = new byte[20000001 * 4];
    ByteBuffer buffer = ByteBuffer.wrap(byteArray);
    for (int i = 0; i < 20000001; ++i) {
      buffer.putInt(i + 1);
    }

    File file = new File("/tmp/met0.simple");
    file.createNewFile();
    OutputStream outputStream = new FileOutputStream(file);
    outputStream.write(byteArray);
    outputStream.close();
  }
}
