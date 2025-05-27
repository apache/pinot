package org.apache.pinot.segment.local.segment.index.readers.text;

import java.io.IOException;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 * Lucene docIDs are not same as pinot docIDs. The internal implementation
 * of Lucene can change the docIds and they are not guaranteed to be the
 * same as how we expect -- strictly increasing docIDs as the documents
 * are ingested during segment/index creation.
 * This class is used to map the luceneDocId (returned by the search query
 * to the collector) to corresponding pinotDocId.
 */
class DefaultDocIdTranslator implements DocIdTranslator {
  final PinotDataBuffer _buffer;

  DefaultDocIdTranslator(PinotDataBuffer buffer) {
    _buffer = buffer;
  }

  public int getPinotDocId(int luceneDocId) {
    return _buffer.getInt(luceneDocId * Integer.BYTES);
  }

  @Override
  public void close()
      throws IOException {
    _buffer.close();
  }
}
