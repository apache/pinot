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
package org.apache.pinot.segment.local.utils.nativefst;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import com.google.common.base.Preconditions;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.pinot.segment.local.segment.creator.impl.text.NativeTextIndexCreator;
import org.apache.pinot.segment.local.utils.nativefst.utils.RegexpMatcher;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class loads FST index from PinotDataBuffer and creates a FST reader which
 * is used in finding matching results for regexp queries. Since FST index currently
 * stores dict ids as values this class only implements getDictIds method.
 *
 * This class works on top of ImmutableFST.
 *
 */
public class NativeFSTIndexReader implements TextIndexReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeFSTIndexReader.class);

  private final FST _fst;

  public NativeFSTIndexReader(PinotDataBuffer dataBuffer)
      throws IOException {
    // TODO: Implement an InputStream directly on PinotDataBuffer
    /*ByteBuffer byteBuffer = dataBuffer.toDirectByteBuffer(0, (int) dataBuffer.size());
    _fst = FST.read(new ByteBufferInputStream(Collections.singletonList(byteBuffer)), ImmutableFST.class, true);*/
    int fstMagic = dataBuffer.getInt(0);
    Preconditions.checkState(fstMagic == FSTHeader.FST_MAGIC, "Invalid native text index magic header: %s", fstMagic);
    int fstDataLength = dataBuffer.getInt(4);

    long fstDataStartOffset = NativeFSTIndexCreator.HEADER_LENGTH;
    ByteBuffer byteBuffer = dataBuffer.toDirectByteBuffer(fstDataStartOffset, fstDataLength);
    try {
      _fst = FST.read(new ByteBufferInputStream(Collections.singletonList(byteBuffer)), ImmutableFST.class, true, fstDataLength);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    throw new UnsupportedOperationException("NativeFSTIndexReader only supports getDictIds currently");
  }

  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    try {
      RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
      RegexpMatcher.regexMatch(searchQuery, _fst, writer::add);
      return writer.get();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while matching regex: " + searchQuery, e);
    }
  }

  @Override
  public void close()
      throws IOException {
  }
}
