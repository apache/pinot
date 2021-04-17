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
package org.apache.pinot.hadoop.io;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;


public class FileHandler {

  private MeteredStream _meter;
  private final File _logFile;
  private String _baseDir;
  private final String _fileName;
  private int _fileCount = 0;
  private final String _extension;
  private final Long _maxSize;

  public FileHandler(String baseDir, String fileName, String extension, long maxSize) {
    _baseDir = baseDir;
    createBaseDir(baseDir);
    _fileName = fileName;
    _extension = extension;
    _logFile = newLogFile();
    _maxSize = maxSize;
  }

  /**
   * A metered stream is a subclass of OutputStream that <ul> <li>forwards all its output to a
   * target stream</li> <li>keeps track of how many bytes have been written copied from {@link
   * java.util.logging.FileHandler}</li> </ul>
   */
  private static class MeteredStream extends OutputStream {
    final OutputStream _out;
    long _written;

    MeteredStream(OutputStream out, long written) {
      _out = out;
      _written = written;
    }

    @Override
    public void write(int b) throws IOException {
      _out.write(b);
      _written++;
    }

    @Override
    public void write(byte[] buff) throws IOException {
      _out.write(buff);
      _written += buff.length;
    }

    @Override
    public void write(byte[] buff, int off, int len) throws IOException {
      _out.write(buff, off, len);
      _written += len;
    }

    @Override
    public void flush() throws IOException {
      _out.flush();
    }

    @Override
    public void close() throws IOException {
      _out.close();
    }

    public long getSize() {
      return _written;
    }
  }

  public void open(boolean append) throws IOException {
    long len = 0;
    if (append) {
      len = _logFile.length();
    }
    FileOutputStream fout = new FileOutputStream(_logFile.toString(), append);
    BufferedOutputStream bout = new BufferedOutputStream(fout);
    _meter = new MeteredStream(bout, len);
  }

  public void write(byte[] b) throws IOException {
    if (getSize() >= _maxSize) {
      rotate();
    }
    _meter.write(b);
  }

  public void close() throws IOException {
    rotate(false);
  }

  public void rotate() throws IOException {
    rotate(true);
  }

  public long getSize() {
    return _meter.getSize();
  }

  /**
   * Rotate the set of output files
   */
  private synchronized void rotate(boolean isNew) throws IOException {
    _meter.flush();
    _meter.close();
    File rotateFile = newRotateFile();
    if (_logFile.exists()) {
      if (rotateFile.exists()) {
        rotateFile.delete();
      }
      _logFile.renameTo(rotateFile);
    }
    if (isNew) {
      open(true);
    }
    _fileCount++;
  }

  private File newRotateFile() {
    return initFile(String.format("%s/%s_%s.%s", _baseDir, _fileName, _fileCount, _extension));
  }

  private File newLogFile() {
    return initFile(String.format("%s/%s.%s", _baseDir, _fileName, _extension));
  }

  private File initFile(String file) {
    File f = new File(file);
    if (f.exists()) {
      f.delete();
    }
    return f;
  }

  private void createBaseDir(String baseDir) {
    File f = new File(baseDir);
    f.mkdirs();
  }
}
