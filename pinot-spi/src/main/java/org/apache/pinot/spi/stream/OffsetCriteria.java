/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.stream;

import com.google.common.base.Preconditions;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.apache.pinot.spi.utils.TimeUtils;


/**
 * Defines and builds the offset criteria for consumption from a stream
 */
public class OffsetCriteria {

  public static final OffsetCriteria SMALLEST_OFFSET_CRITERIA =
      new OffsetCriteria.OffsetCriteriaBuilder().withOffsetSmallest();

  /**
   * Enumerates the supported offset types
   */
  private enum OffsetType {

    // Consumes from the smallest available offset
    SMALLEST,

    // Consumes from the largest seen offset
    LARGEST,

    // Consumes from the time as provided in the period string
    PERIOD,

    // Consumes from the custom offset criteria */
    CUSTOM

  }

  private OffsetType _offsetType;
  private String _offsetString;

  private OffsetCriteria() {

  }

  private void setOffsetType(OffsetType offsetType) {
    _offsetType = offsetType;
  }

  private void setOffsetString(String offsetString) {
    _offsetString = offsetString;
  }

  /**
   * True if the offset criteria is defined to read from smallest available offset
   * @return
   */
  public boolean isSmallest() {
    return _offsetType != null && _offsetType.equals(OffsetType.SMALLEST);
  }

  /**
   * True if the offset criteria is defined to read from the largest known offset
   * @return
   */
  public boolean isLargest() {
    return _offsetType != null && _offsetType.equals(OffsetType.LARGEST);
  }

  /**
   * True if the offset criteria is defined as a period string
   * @return
   */
  public boolean isPeriod() {
    return _offsetType != null && _offsetType.equals(OffsetType.PERIOD);
  }

  /**
   * True if the offset criteria is defined as a custom format string
   * @return
   */
  public boolean isCustom() {
    return _offsetType != null && _offsetType.equals(OffsetType.CUSTOM);
  }

  /**
   * Getter for offset string
   * @return
   */
  public String getOffsetString() {
    return _offsetString;
  }

  /**
   * Builds an {@link OffsetCriteria}
   */
  public static class OffsetCriteriaBuilder {
    private OffsetCriteria _offsetCriteria;

    public OffsetCriteriaBuilder() {
      _offsetCriteria = new OffsetCriteria();
    }

    /**
     * Builds an {@link OffsetCriteria} with {@link OffsetType} SMALLEST
     * @return
     */
    public OffsetCriteria withOffsetSmallest() {
      _offsetCriteria.setOffsetType(OffsetType.SMALLEST);
      _offsetCriteria.setOffsetString(OffsetType.SMALLEST.toString().toLowerCase());
      return _offsetCriteria;
    }

    /**
     * Builds an {@link OffsetCriteria} with {@link OffsetType} LARGEST
     * @return
     */
    public OffsetCriteria withOffsetLargest() {
      _offsetCriteria.setOffsetType(OffsetType.LARGEST);
      _offsetCriteria.setOffsetString(OffsetType.LARGEST.toString().toLowerCase());
      return _offsetCriteria;
    }

    /**
     * Builds an {@link OffsetCriteria} with {@link OffsetType} PERIOD
     * @param periodString
     * @return
     */
    public OffsetCriteria withOffsetAsPeriod(String periodString) {
      Preconditions.checkNotNull(periodString, "Must provide period string eg. 10d, 4h30m etc");
      _offsetCriteria.setOffsetType(OffsetType.PERIOD);
      _offsetCriteria.setOffsetString(periodString);
      return _offsetCriteria;
    }

    /**
     * Builds an {@link OffsetCriteria} with {@link OffsetType} CUSTOM
     * @param customString
     * @return
     */
    public OffsetCriteria withOffsetCustom(String customString) {
      Preconditions.checkNotNull(customString, "Must provide custom offset criteria string");
      _offsetCriteria.setOffsetType(OffsetType.CUSTOM);
      _offsetCriteria.setOffsetString(customString);
      return _offsetCriteria;
    }

    /**
     * Builds an {@link OffsetCriteria} with the right {@link OffsetType} given the offset string
     * @param offsetString
     * @return
     */
    public OffsetCriteria withOffsetString(String offsetString) {
      Preconditions.checkNotNull(offsetString, "Must provide offset string");

      if (offsetString.equalsIgnoreCase(OffsetType.SMALLEST.toString())) {
        _offsetCriteria.setOffsetType(OffsetType.SMALLEST);
      } else if (offsetString.equalsIgnoreCase(OffsetType.LARGEST.toString())) {
        _offsetCriteria.setOffsetType(OffsetType.LARGEST);
      } else {
        try {
          Long periodToMillis = TimeUtils.convertPeriodToMillis(offsetString);
          if (periodToMillis >= 0) {
            _offsetCriteria.setOffsetType(OffsetType.PERIOD);
          } else {
            _offsetCriteria.setOffsetType(OffsetType.CUSTOM);
          }
        } catch (Exception e) {
          _offsetCriteria.setOffsetType(OffsetType.CUSTOM);
        }
      }
      _offsetCriteria.setOffsetString(offsetString);
      return _offsetCriteria;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    OffsetCriteria that = (OffsetCriteria) o;

    return EqualityUtils.isEqual(_offsetType, that._offsetType)
        && EqualityUtils.isEqual(_offsetString, that._offsetString);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_offsetType);
    result = EqualityUtils.hashCodeOf(result, _offsetString);
    return result;
  }

  @Override
  public String toString() {
    return "OffsetCriteria{" + "_offsetType=" + _offsetType + ", _offsetString='" + _offsetString + '\'' + '}';
  }
}
