/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.creator;

/**
 * An interface to read the column statistics from statistics collectors.
 *
 */
public interface ColumnStatistics {
    /**
     * @return Minimum value of the column
     * @throws Exception
     */
    public Object getMinValue() throws Exception;

    /**
     * @return Maximum value of the column
     * @throws Exception
     */
    public Object getMaxValue() throws Exception;

    /**
     *
     * @return An array of elements that has the unique values for this column, sorted order.
     * @throws Exception
     */
    public Object getUniqueValuesSet() throws Exception;

    /**
     *
     * @return The number of unique values of this column.
     * @throws Exception
     */
    public int getCardinality() throws Exception;

    /**
     *
     * @return For string objects, returns the length of the longest string value. For others, returns -1.
     * @throws Exception
     */
    public int getLengthOfLargestElement() throws Exception;

    /**
     *
     * @return The number of null values in the input for this column.
     */
    public int getNumInputNullValues();

    /**
     *
     * @return total number of entries
     */
    public int getTotalNumberOfEntries();

    /**
     * @return For multi-valued columns, returns the max number of values in a single occurrence of the column, otherwise 0.
     */
    public int getMaxNumberOfMultiValues();

    /**
     * @note
     * @return Returns if any of the values have nulls in the segments.
     */
    public abstract boolean hasNull();
}
