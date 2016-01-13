package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.TimeRange;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TestMetricIndexEntry {
  private List<MetricIndexEntry> indexEntries;

  @BeforeClass
  public void beforeClass() throws Exception {
    indexEntries = generateEntries();
  }

  @Test
  public void testEncodeDecode() throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(os);

    for (MetricIndexEntry indexEntry : indexEntries) {
      oos.writeObject(indexEntry);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(is);

    for (MetricIndexEntry indexEntry : indexEntries) {
      MetricIndexEntry decodedEntry = (MetricIndexEntry) ois.readObject();
      Assert.assertEquals(decodedEntry, indexEntry);
    }
  }

  private List<MetricIndexEntry> generateEntries() {
    List<MetricIndexEntry> indexEntries = new ArrayList<MetricIndexEntry>();

    UUID fileId = UUID.randomUUID();

    for (int i = 0; i < 10; i++) {
      indexEntries.add(new MetricIndexEntry(UUID.randomUUID(), fileId, i * 1024, 1024,
          new TimeRange((long) i * 4, (long) i * 4 + 3)));
    }

    return indexEntries;
  }
}
