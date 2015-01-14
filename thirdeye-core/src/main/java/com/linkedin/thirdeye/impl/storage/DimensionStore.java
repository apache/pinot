package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DimensionStore
{
  private final StarTreeConfig config;
  private final ByteBuffer buffer;
  private final DimensionDictionary dictionary;
  private final Object sync;

  public DimensionStore(StarTreeConfig config, ByteBuffer buffer, DimensionDictionary dictionary)
  {
    this.config = config;
    this.buffer = buffer;
    this.dictionary = dictionary;
    this.sync = new Object();
  }

  public DimensionDictionary getDictionary()
  {
    return dictionary;
  }

  public List<DimensionKey> getDimensionKeys()
  {
    synchronized (sync)
    {
      List<DimensionKey> dimensionKeys = new ArrayList<DimensionKey>();

      buffer.rewind();

      while (buffer.position() < buffer.limit())
      {
        String[] dimensionValues = new String[config.getDimensions().size()];

        for (int i = 0; i < config.getDimensions().size(); i++)
        {
          DimensionSpec dimensionSpec = config.getDimensions().get(i);
          Integer valueId = buffer.getInt();
          String dimensionValue = dictionary.getDimensionValue(dimensionSpec.getName(), valueId);
          dimensionValues[i] = dimensionValue;
        }

        dimensionKeys.add(new DimensionKey(dimensionValues));
      }

      return dimensionKeys;
    }
  }

  public Map<DimensionKey, Integer> findMatchingKeys(DimensionKey dimensionKey)
  {
    synchronized (sync)
    {
      Map<DimensionKey, Integer> matchingKeys = new HashMap<DimensionKey, Integer>();

      int[] translatedKey = dictionary.translate(config.getDimensions(), dimensionKey);
      int[] currentKey = new int[config.getDimensions().size()];

      int idx = 0;

      buffer.rewind();

      while (buffer.position() < buffer.limit())
      {
        boolean matches = true;

        for (int i = 0; i < config.getDimensions().size(); i++)
        {
          Integer valueId = buffer.getInt();

          currentKey[i] = valueId;

          if (translatedKey[i] != valueId && translatedKey[i] != StarTreeConstants.STAR_VALUE)
          {
            matches = false;
          }
        }

        if (matches)
        {
          matchingKeys.put(dictionary.translate(config.getDimensions(), currentKey), idx);
        }

        idx++;
      }

      return matchingKeys;
    }
  }
}
