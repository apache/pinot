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
package com.linkedin.pinot.core.query.aggregation.function.quantile;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class PercentileUtil {

    public static double getValueOnPercentile(DoubleArrayList list, byte percentile) {
        return getValueOnQuantile(list, (percentile+0.0)/100);
    }

    /**
     * List will be sorted after passing to this function,
     * so a pre-sorting is not needed.
     *
     * @param list
     * @param quantile
     * @return
     */
    public static double getValueOnQuantile(DoubleArrayList list, double quantile) {
        checkArgument(quantile >= 0 && quantile <= 1, "quantile must be in range [0, 1]");

        Collections.sort(list, new Comparator<Double>() {
            @Override
            public int compare(Double o1, Double o2) {
                return o1.compareTo(o2);
            }
        });

        int index = (int) (list.size()*quantile);
        return list.get(index);
    }
}