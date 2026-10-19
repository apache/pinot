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
package org.apache.pinot.common.compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;
import java.util.Set;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE;
import net.jpountz.lz4.LZ4FrameOutputStream.FLG.Bits;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.CompressorStreamProvider;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.io.output.CloseShieldOutputStream;


/// Adds explicit LZ4 frame encoders to the Commons Compress factory. This provider is stateless and thread-safe;
/// returned streams are not thread-safe. All variants use the Commons reader for existing frame compatibility.
public class Lz4CompressorStreamProvider implements CompressorStreamProvider {
  /// Preserves the existing Commons Compress encoder.
  public static final String LZ4_DEFAULT = "lz4-default";
  public static final String LZ4_FAST = "lz4-fast";
  public static final String LZ4_HC = "lz4-hc";
  private static final Set<String> NAMES = Set.of(LZ4_DEFAULT, LZ4_FAST, LZ4_HC);

  @Override
  public CompressorInputStream createCompressorInputStream(String name, InputStream in, boolean decompressConcatenated)
      throws CompressorException {
    validateName(name);
    return CompressorStreamFactory.getSingleton()
        .createCompressorInputStream(CompressorStreamFactory.LZ4_FRAMED, in, decompressConcatenated);
  }

  @Override
  public <T extends OutputStream> CompressorOutputStream<T> createCompressorOutputStream(String name, T out)
      throws CompressorException {
    validateName(name);
    try {
      return new Lz4OutputStream<>(out, name);
    } catch (IOException e) {
      throw new CompressorException("Could not create LZ4 output stream", e);
    }
  }

  @Override
  public Set<String> getInputStreamCompressorNames() {
    return NAMES;
  }

  @Override
  public Set<String> getOutputStreamCompressorNames() {
    return NAMES;
  }

  private static void validateName(String name)
      throws CompressorException {
    if (name == null || !NAMES.contains(name.toLowerCase(Locale.ROOT))) {
      throw new CompressorException("Unsupported LZ4 compressor: " + name);
    }
  }

  /// Adapts frame encoders to the Commons stream lifecycle, including finish without closing the caller's output.
  private static class Lz4OutputStream<T extends OutputStream> extends CompressorOutputStream<T> {
    private final OutputStream _frameOut;

    private Lz4OutputStream(T out, String name)
        throws IOException {
      super(out);
      OutputStream shield = CloseShieldOutputStream.wrap(out);
      if (LZ4_DEFAULT.equalsIgnoreCase(name)) {
        _frameOut = new FramedLZ4CompressorOutputStream(shield);
      } else {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        _frameOut = new LZ4FrameOutputStream(shield, BLOCKSIZE.SIZE_4MB, -1,
            LZ4_HC.equalsIgnoreCase(name) ? factory.highCompressor() : factory.fastCompressor(),
            XXHashFactory.fastestInstance().hash32(), Bits.BLOCK_INDEPENDENCE, Bits.CONTENT_CHECKSUM);
      }
    }

    @Override
    public void write(int value)
        throws IOException {
      checkOpen();
      _frameOut.write(value);
    }

    @Override
    public void write(byte[] bytes, int offset, int length)
        throws IOException {
      checkOpen();
      _frameOut.write(bytes, offset, length);
    }

    @Override
    public void flush()
        throws IOException {
      if (isFinished()) {
        out().flush();
      } else {
        _frameOut.flush();
      }
    }

    @Override
    public void finish()
        throws IOException {
      if (!isFinished()) {
        _frameOut.close();
        super.finish();
      }
    }

    @Override
    public void close()
        throws IOException {
      try {
        finish();
      } finally {
        super.close();
      }
    }
  }
}
