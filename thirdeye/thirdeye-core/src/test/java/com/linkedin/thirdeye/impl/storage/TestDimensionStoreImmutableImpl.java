package com.linkedin.thirdeye.impl.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestDimensionStoreImmutableImpl
{
  private StarTreeConfig config;
  private DimensionDictionary dictionary;
  private DimensionStore dimensionStore;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    ObjectMapper objectMapper = new ObjectMapper();
    InputStream inputStream = ClassLoader.getSystemResourceAsStream("sample-dictionary.json");
    dictionary = objectMapper.readValue(inputStream, DimensionDictionary.class);
    inputStream = ClassLoader.getSystemResourceAsStream("sample-config.yml");
    config = StarTreeConfig.decode(inputStream);

    ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

    for (DimensionKey key : generateKeys())
    {
      StorageUtils.addToDimensionStore(config, buffer, key, dictionary);
    }

    buffer.flip();

    dimensionStore = new DimensionStoreImmutableImpl(config, buffer, dictionary);
  }

  @Test
  public void testGetDimensionKeys()
  {
    Assert.assertEquals(dimensionStore.getDimensionKeys(), generateKeys());
  }

  @Test
  public void testFindMatchingKeys_allStar()
  {
    DimensionKey searchKey = new DimensionKey(new String[] {
            StarTreeConstants.STAR,
            StarTreeConstants.STAR,
            StarTreeConstants.STAR
    });

    Map<DimensionKey, Integer> result = dimensionStore.findMatchingKeys(searchKey);

    Assert.assertEquals(result.size(), 15);
  }

  @Test
  public void testFindMatchingKeys_someStar()
  {
    DimensionKey searchKey = new DimensionKey(new String[] {
            "A0",
            StarTreeConstants.STAR,
            StarTreeConstants.STAR
    });

    Map<DimensionKey, Integer> result = dimensionStore.findMatchingKeys(searchKey);

    Assert.assertEquals(result.size(), 4);

    checkLogicalOffsets(result);
  }

  @Test
  public void testFindMatchingKeys_noStar()
  {
    DimensionKey searchKey = new DimensionKey(new String[] {
            "A0",
            "B0",
            "C0"
    });

    Map<DimensionKey, Integer> result = dimensionStore.findMatchingKeys(searchKey);

    Assert.assertEquals(result.size(), 1);

    checkLogicalOffsets(result);
  }

  @Test
  public void testFindMatchingKeys_leastOthers()
  {
    DimensionKey searchKey = new DimensionKey(new String[] {
            "AX",
            "BX",
            "CX"
    }); // expect "?", "BX", "CX"

    Map<DimensionKey, Integer> result = dimensionStore.findMatchingKeys(searchKey);

    Assert.assertEquals(result.size(), 1);

    checkLogicalOffsets(result);
  }

  private void checkLogicalOffsets(Map<DimensionKey, Integer> result)
  {
    List<DimensionKey> keys = generateKeys();

    for (int i = 0; i < keys.size(); i++)
    {
      if (result.containsKey(keys.get(i)))
      {
        Assert.assertEquals(result.get(keys.get(i)), Integer.valueOf(i));
      }
    }
  }

  private static List<DimensionKey> generateKeys()
  {
    List<DimensionKey> keys = new ArrayList<DimensionKey>();

    for (int i = 0; i < 10; i++)
    {
      DimensionKey key = new DimensionKey(new String[]{
              "A" + (i % 3),
              "B" + (i % 6),
              "C" + (i % 9)
      });

      keys.add(key);
    }

    // Add some keys with others
    keys.add(new DimensionKey(new String[]{"?", "?", "CX"}));
    keys.add(new DimensionKey(new String[]{"?", "BX", "?"}));
    keys.add(new DimensionKey(new String[]{"AX", "?", "?"}));
    keys.add(new DimensionKey(new String[]{"?", "?", "?"}));
    keys.add(new DimensionKey(new String[]{"?", "BX", "CX"}));

    return keys;
  }
}
