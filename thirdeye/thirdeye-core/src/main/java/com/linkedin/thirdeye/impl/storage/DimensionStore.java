package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.DimensionKey;

import java.util.List;
import java.util.Map;

/**
 * The dimension keys associated with a {@link com.linkedin.thirdeye.impl.storage.MetricStore}
 */
public interface DimensionStore
{
  /**
   * @return
   *  The dictionary used to map String dimension values to internal storage format
   */
  DimensionDictionary getDictionary();

  /**
   * @return
   *  All of the dimension keys in this store
   */
  List<DimensionKey> getDimensionKeys();

  /**
   * @param dimensionKey
   *  A dimension combination (possibly aggregate level)
   * @return
   *  A map of all dimension keys that match the provided key (i.e. star matches everything)
   *
   *  <p>
   *    If no keys match, this method will find the key with the least number of "other" dimension values.
   *  </p>
   */
  Map<DimensionKey, Integer> findMatchingKeys(DimensionKey dimensionKey);
}
