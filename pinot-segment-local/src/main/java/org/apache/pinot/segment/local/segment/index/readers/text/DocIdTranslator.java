package org.apache.pinot.segment.local.segment.index.readers.text;

import java.io.Closeable;


public interface DocIdTranslator extends Closeable {
  int getPinotDocId(int luceneDocId);
}
