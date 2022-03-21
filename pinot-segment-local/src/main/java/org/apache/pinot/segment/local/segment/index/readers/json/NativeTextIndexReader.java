package org.apache.pinot.segment.local.segment.index.readers.json;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.PrimitiveIterator;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.local.utils.nativefst.FST;
import org.apache.pinot.segment.local.utils.nativefst.ImmutableFST;
import org.apache.pinot.segment.local.utils.nativefst.NativeTextIndexCreator;
import org.apache.pinot.segment.local.utils.nativefst.utils.RegexpMatcher;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class NativeTextIndexReader implements TextIndexReader {
  private final FST _fst;
  private final BitmapInvertedIndexReader _invertedIndex;
  private int _numDocs;

  public NativeTextIndexReader(PinotDataBuffer dataBuffer, int numDocs) {
    _numDocs = numDocs;
    long invertedIndexLength = dataBuffer.getLong(4);
    long fstDataLength = dataBuffer.getLong(12);
    int numBitMaps = dataBuffer.getInt(20);

    long fstDataStartOffset = NativeTextIndexCreator.HEADER_LENGTH;
    long fstDataEndOffset = fstDataStartOffset + fstDataLength;
    ByteBuffer byteBuffer = dataBuffer.toDirectByteBuffer(fstDataStartOffset, (int) fstDataLength);
    byte[] arr = new byte[byteBuffer.remaining()];
   // byteBuffer.get(arr);
    try {
      _fst = FST.read(new ByteBufferInputStream(Collections.singletonList(byteBuffer)), ImmutableFST.class, true);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }

    long invertedIndexEndOffset = fstDataEndOffset + invertedIndexLength;
    _invertedIndex = new BitmapInvertedIndexReader(
        dataBuffer.view(fstDataEndOffset, invertedIndexEndOffset, ByteOrder.BIG_ENDIAN), numBitMaps);
  }

  @Override
  public ImmutableRoaringBitmap getDictIds(String searchQuery) {
    try {
      RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
      RegexpMatcher.regexMatch(searchQuery, _fst, writer::add);
      return writer.get();
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while running query: " + searchQuery, e);
    }
  }

  @Override
  public MutableRoaringBitmap getDocIds(String searchQuery) {
    try {
      RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
      RegexpMatcher.regexMatch(searchQuery, _fst, writer::add);
      ImmutableRoaringBitmap matchingDictIds = writer.get();
      MutableRoaringBitmap matchingDocIds = null;

      for (PrimitiveIterator.OfInt it = matchingDictIds.stream().iterator(); it.hasNext(); ) {
        int dictId = it.next();

        if (dictId >= 0) {
          ImmutableRoaringBitmap docIds = _invertedIndex.getDocIds(dictId);
          if (matchingDocIds == null) {
            matchingDocIds = docIds.toMutableRoaringBitmap();
          } else {
            matchingDocIds.or(docIds);
          }
        }
      }

      return matchingDocIds == null ? new MutableRoaringBitmap() : matchingDocIds;
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while running query: " + searchQuery, e);
    }
  }

  @Override
  public void close()
      throws IOException {
  }
}
