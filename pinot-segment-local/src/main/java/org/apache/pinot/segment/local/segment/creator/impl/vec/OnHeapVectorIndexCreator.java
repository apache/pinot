package org.apache.pinot.segment.local.segment.creator.impl.vec;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.pinot.spi.data.readers.Vector;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;


public class OnHeapVectorIndexCreator extends BaseVectorIndexCreator {

  public OnHeapVectorIndexCreator(File indexDir, String columnName)
      throws IOException {
    super(indexDir, columnName);
  }

  @Override
  public void seal()
      throws IOException {
    for (Map.Entry<Vector, RoaringBitmapWriter<RoaringBitmap>> entry : _postingListMap.entrySet()) {
      add(entry.getKey(), entry.getValue().get());
    }
    generateIndexFile();
  }
}