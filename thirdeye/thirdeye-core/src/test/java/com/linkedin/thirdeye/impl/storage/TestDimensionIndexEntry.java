package com.linkedin.thirdeye.impl.storage;

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

public class TestDimensionIndexEntry {
  private List<DimensionIndexEntry> indexEntries;

  @BeforeClass
  public void beforeClass() throws Exception {
    indexEntries = generateEntries();
  }

  @Test
  public void testEncodeDecode() throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(os);

    for (DimensionIndexEntry indexEntry : indexEntries) {
      oos.writeObject(indexEntry);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(is);

    for (DimensionIndexEntry indexEntry : indexEntries) {
      DimensionIndexEntry decodedEntry = (DimensionIndexEntry) ois.readObject();
      Assert.assertEquals(decodedEntry, indexEntry);
    }
  }

  private List<DimensionIndexEntry> generateEntries() {
    List<DimensionIndexEntry> indexEntries = new ArrayList<DimensionIndexEntry>();

    UUID fileId = UUID.randomUUID();

    for (int i = 0; i < 10; i++) {
      indexEntries.add(new DimensionIndexEntry(UUID.randomUUID(), fileId, i * 1024, 1024,
          i * 1024 * 10, 1024 * 10));
    }

    return indexEntries;
  }
}
