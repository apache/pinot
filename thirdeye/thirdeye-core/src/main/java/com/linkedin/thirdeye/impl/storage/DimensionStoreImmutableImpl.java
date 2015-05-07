package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DimensionStoreImmutableImpl implements DimensionStore
{
  private final StarTreeConfig config;
  private final ByteBuffer buffer;
  private final DimensionDictionary dictionary;

  public DimensionStoreImmutableImpl(StarTreeConfig config, ByteBuffer buffer, DimensionDictionary dictionary)
  {
    this.config = config;
    this.buffer = buffer;
    this.dictionary = dictionary;
  }

  @Override
  public DimensionDictionary getDictionary()
  {
    return dictionary;
  }

  @Override
  public List<DimensionKey> getDimensionKeys()
  {
    List<DimensionKey> dimensionKeys = new ArrayList<DimensionKey>();

    ByteBuffer tmpBuffer = buffer.duplicate();

    while (tmpBuffer.position() < tmpBuffer.limit())
    {
      String[] dimensionValues = new String[config.getDimensions().size()];

      for (int i = 0; i < config.getDimensions().size(); i++)
      {
        DimensionSpec dimensionSpec = config.getDimensions().get(i);
        Integer valueId = tmpBuffer.getInt();
        String dimensionValue = dictionary.getDimensionValue(dimensionSpec.getName(), valueId);
        dimensionValues[i] = dimensionValue;
      }

      dimensionKeys.add(new DimensionKey(dimensionValues));
    }

    return dimensionKeys;
  }

  @Override
  public Map<DimensionKey, Integer> findMatchingKeys(DimensionKey dimensionKey)
  {
    Map<DimensionKey, Integer> matchingKeys = new HashMap<DimensionKey, Integer>();

    int[] translatedKey = dictionary.translate(config.getDimensions(), dimensionKey);
    int[] currentKey = new int[config.getDimensions().size()];

    int idx = 0;

    ByteBuffer tmpBuffer = buffer.duplicate();

    while (tmpBuffer.position() < tmpBuffer.limit())
    {
      boolean matches = true;

      for (int i = 0; i < config.getDimensions().size(); i++)
      {
        Integer valueId = tmpBuffer.getInt();

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

    // If matching keys is empty, use record with least others!
    if (matchingKeys.isEmpty())
    {
      idx = 0;
      tmpBuffer.rewind();

      int leastNumOthers = config.getDimensions().size() + 1;
      int leastOthersIdx = -1;
      int[] leastOthersKey = null;

      while (tmpBuffer.position() < tmpBuffer.limit())
      {
        boolean matches = true;
        int currentNumOthers = 0;

        for (int i = 0; i < config.getDimensions().size(); i++)
        {
          Integer valueId = tmpBuffer.getInt();

          currentKey[i] = valueId;

          if (translatedKey[i] != valueId
              && valueId != StarTreeConstants.STAR_VALUE
              && valueId != StarTreeConstants.OTHER_VALUE)
          {
            matches = false;
          }

          if (valueId == StarTreeConstants.OTHER_VALUE)
          {
            currentNumOthers++;
          }
        }

        if (matches && currentNumOthers < leastNumOthers)
        {
          leastOthersKey = Arrays.copyOf(currentKey, currentKey.length);
          leastNumOthers = currentNumOthers;
          leastOthersIdx = idx;
        }

        idx++;
      }

      if (leastOthersKey == null)
      {
        throw new IllegalStateException("Could not find alternative dimension combination for " + dimensionKey);
      }

      matchingKeys.put(dictionary.translate(config.getDimensions(), leastOthersKey), leastOthersIdx);
    }

    return matchingKeys;
  }
}
