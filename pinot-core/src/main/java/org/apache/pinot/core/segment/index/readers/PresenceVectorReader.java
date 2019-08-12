package org.apache.pinot.core.segment.index.readers;

import com.google.common.base.Joiner;
import java.util.Random;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.roaringbitmap.RoaringBitmap;


public class PresenceVectorReader {

  RoaringBitmap _presenceBitmap;
  RoaringBitmap _nullBitmap;

  public PresenceVectorReader(PinotDataBuffer presenceVectorBuffer) {

  }

  public boolean isPresent(int docId) {
    return _presenceBitmap.contains(docId);
  }

  public RoaringBitmap getPresenceVector() {
    return _presenceBitmap;
  }

  public RoaringBitmap getNullVector() {
    return _nullBitmap;
  }



  public static void main(String[] args) {

    int[] nullPercentArray = new int[]{1, 5, 10, 25, 50, 75, 95, 99};

    int[] numDocsArray = new int[]{1_000_000, 10_000_000, 100_000_000};

    for (int numDocs : numDocsArray) {
      Random random = new Random();
      for (int nullPercent : nullPercentArray) {
        RoaringBitmap presenceVector = new RoaringBitmap();
        RoaringBitmap nullVector = new RoaringBitmap();
        for (int i = 0; i < numDocs; i++) {
          if (random.nextInt(100) <= nullPercent) {
            nullVector.add(i);
          } else {
            presenceVector.add(i);
          }
        }
        long start = System.currentTimeMillis();
        nullVector.flip((long) 0, (long) numDocs);
        long end = System.currentTimeMillis();
        System.out.println(Joiner.on("\t")
            .join(numDocs, nullPercent, presenceVector.serializedSizeInBytes(), nullVector.serializedSizeInBytes(),
                (nullVector.getCardinality() * 1.0 / numDocs) * 100, "" + (end - start)));
      }
    }
  }
}
