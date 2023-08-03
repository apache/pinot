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
package org.apache.pinot.segment.local.function;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.readers.GenericRow;

public abstract class BaseRowMerger implements BiFunctionEvaluator {

    @Override
    public void evaluate(GenericRow previousRow, GenericRow newRow, Map<String, Object> result) {
        Preconditions.checkNotNull(previousRow);
        Preconditions.checkNotNull(newRow);
        // todo reuse array instead of initialising new for every row
        Object[] prevValues = extractColumnValues(previousRow, this.getPrevRowArgs());
        Object[] newValues = extractColumnValues(newRow, this.getNewRowArgs());

        this.evaluate(prevValues, newValues, result);
    }

    private Object[] extractColumnValues(GenericRow row, List<String> prevRowArgs) {
        Object[] values = new Object[prevRowArgs.size()];
        for (int i = 0; i < prevRowArgs.size(); i++) {
            String col = prevRowArgs.get(i);
            values[i] = row.isNullValue(col) ? null : row.getValue(col);
        }
        return values;
    }

}
