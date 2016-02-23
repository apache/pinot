package com.linkedin.thirdeye.impl.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class TestDimensionDictionary {
  private StarTreeConfig config;
  private DimensionDictionary dictionary;

  @BeforeClass
  public void beforeClass() throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    InputStream inputStream = ClassLoader.getSystemResourceAsStream("sample-dictionary.json");
    dictionary = objectMapper.readValue(inputStream, DimensionDictionary.class);
    inputStream = ClassLoader.getSystemResourceAsStream("sample-config.yml");
    config = StarTreeConfig.decode(inputStream);
  }

  @Test
  public void testEncodeDecode() throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(os);

    oos.writeObject(dictionary);

    ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
    ObjectInputStream ois = new ObjectInputStream(is);

    DimensionDictionary decodedDictionary = (DimensionDictionary) ois.readObject();
    Assert.assertEquals(decodedDictionary, dictionary);
  }

  @Test
  public void testGetValueId() {
    Assert.assertEquals(dictionary.getValueId("A", "A0"), Integer.valueOf(2));
  }

  @Test
  public void testGetValueIdNoMapping() {
    Assert.assertEquals(dictionary.getValueId("A", "A4").intValue(), StarTreeConstants.OTHER_VALUE);
  }

  @Test
  public void testGetDimensionValue() {
    Assert.assertEquals(dictionary.getDimensionValue("A", 2), "A0");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetDimensionValueNoMapping() {
    dictionary.getDimensionValue("A", 100);
  }

  @Test
  public void testTranslateToInt() {
    DimensionKey dimensionKey = new DimensionKey(new String[] {
        "A0", "B0", "C0"
    });
    int[] intKey = dictionary.translate(config.getDimensions(), dimensionKey);
    Assert.assertEquals(intKey, new int[] {
        2, 2, 2
    });
  }

  @Test
  public void testTranslateToString() {
    int[] intKey = new int[] {
        2, 2, 2
    };
    DimensionKey dimensionKey = dictionary.translate(config.getDimensions(), intKey);
    Assert.assertEquals(dimensionKey.getDimensionValues(), new String[] {
        "A0", "B0", "C0"
    });
  }
}
