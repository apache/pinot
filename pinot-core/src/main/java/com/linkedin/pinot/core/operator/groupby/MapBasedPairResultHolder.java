package com.linkedin.pinot.core.operator.groupby;

import com.linkedin.pinot.core.query.utils.Pair;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;


public class MapBasedPairResultHolder implements ResultHolder<Pair> {

  private Long2ObjectOpenHashMap _resultMap;
  private double _defaultValue;

  public MapBasedPairResultHolder(double defaultValue) {
    _defaultValue = defaultValue;
    _resultMap = new Long2ObjectOpenHashMap();
  }

  /**
   * {@inheritDoc}
   * No-op for MapBasedResultHolder.
   *
   * @param maxUniqueKeys
   */
  @Override
  public void ensureCapacity(int maxUniqueKeys) {
  }

  /**
   * {@inheritDoc}
   *
   * @param groupKey
   * @return
   */
  @Override
  public Pair getResult(long groupKey) {
    Pair result = (Pair) _resultMap.get(groupKey);
    return result;
  }

  @Override
  public double getDoubleResult(long groupKey) {
    throw new RuntimeException("Unsupported method 'getResult' (returning double) for class " + getClass().getName());
  }

  /**
   * {@inheritDoc}
   *
   * @param groupKey
   * @param newValue
   */
  @Override
  public void putValueForKey(long groupKey, Pair newValue) {
    _resultMap.put(groupKey, newValue);
  }

  @Override
  public void putValueForKey(long groupKey, double newValue) {
    throw new RuntimeException(
        "Unsupported method 'putValueForKey' (with double input) for class " + getClass().getName());
  }
}
