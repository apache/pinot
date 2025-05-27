package org.apache.pinot.segment.local.segment.index.readers.text;

import java.io.IOException;


/**
 * Doc id translator that does no actual translation and just returns given lucene id.
 * Used when doc ids are the same and translation is not necessary.
 */
class NoOpDocIdTranslator implements DocIdTranslator {
  static final NoOpDocIdTranslator INSTANCE = new NoOpDocIdTranslator();

  private NoOpDocIdTranslator() {
  }

  @Override
  public int getPinotDocId(int luceneDocId) {
    return luceneDocId;
  }

  @Override
  public void close()
      throws IOException {
    // do nothing
  }
}
