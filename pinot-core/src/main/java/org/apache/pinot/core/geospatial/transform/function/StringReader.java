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
package org.apache.pinot.core.geospatial.transform.function;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.CharBuffer;


/**
 * Re-usable string reader.
 * Note: at the moment it implements only methods required by GeoJsonReader, that is:
 * - read(char[], int, int)
 * - read()
 * - close()
 */
public class StringReader extends Reader {

  private int _length;
  private String _str;
  private int _next = 0;

  public StringReader() {
  }

  public void setString(String str) {
    _str = str;
    _length = str.length();
    _next = 0;
  }

  @Override
  public int read(char[] cbuf, int off, int len)
  throws IOException {
    if (len == 0) {
      return 0;
    }
    if (_next >= _length) {
      return -1;
    }
    int n = Math.min(_length - _next, len);
    _str.getChars(_next, _next + n, cbuf, off);
    _next += n;
    return n;
  }

  @Override
  public int read()
  throws IOException {
    if (_next >= _length) {
      return -1;
    }
    return _str.charAt(_next++);
  }

  @Override
  public int read(char[] cbuf)
  throws IOException {
    return super.read(cbuf);
  }

  @Override
  public int read(CharBuffer target)
  throws IOException {
    return super.read(target);
  }

  @Override
  public void mark(int readAheadLimit)
  throws IOException {
    super.mark(readAheadLimit);
  }

  @Override
  public boolean markSupported() {
    return super.markSupported();
  }

  @Override
  public void close()
  throws IOException {
    _length = 0;
    _str = null;
    _next = 0;
  }

  @Override
  public void reset()
  throws IOException {
    super.reset();
  }

  @Override
  public boolean ready()
  throws IOException {
    return super.ready();
  }

  @Override
  public long skip(long n)
  throws IOException {
    return super.skip(n);
  }

  @Override
  public long transferTo(Writer out)
  throws IOException {
    return super.transferTo(out);
  }
}
