/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.azure.datalake.store;

import java.io.IOException;
import java.io.InputStream;


/**
 * An {@link MockADLFileInputStream} intended for use only in unit testing.
 * By using this class to wrap any other {@link InputStream}, we can very
 * easily mock the methods in {@link ADLStoreClient} that return
 * {@code ADLFileInputStream}s.
 * <p>
 * This implementation essentially forwards all of its methods to the
 * delegate passed to the sole constructor.
 *
 */
public final class MockADLFileInputStream extends ADLFileInputStream {
  private final InputStream _in;

  public MockADLFileInputStream(InputStream in) {
    super(null, null, null);
    _in = in;
  }

  @Override
  public int read() throws IOException {
    return _in.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return _in.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return _in.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    return _in.skip(n);
  }

  @Override
  public int available() throws IOException {
    return _in.available();
  }

  @Override
  public void close() throws IOException {
    _in.close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    _in.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    _in.reset();
  }

  @Override
  public boolean markSupported() {
    return _in.markSupported();
  }
}