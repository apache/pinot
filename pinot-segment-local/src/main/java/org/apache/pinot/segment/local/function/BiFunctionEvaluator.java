/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.readers.GenericRow;

public interface BiFunctionEvaluator {

    public List<String> getPrevRowArgs();

    public List<String> getNewRowArgs();

    /**
     * Evaluate a function which depends on two rows' columns. Use map of (col,value) instead of GenericRow to avoid
     * looking up all the columns of previous row.
     *
     * @param prevValues values from previous record needed by the implementation
     *                   in the same order as {@link BiFunctionEvaluator#getPrevRowArgs()}
     * @param newValues values from new record needed by tge implementation
     *                  in the same order as {@link BiFunctionEvaluator#getNewRowArgs()}
     * @param result result of evaluation as (column, value) map
     */
    public void evaluate(Object[] prevValues, Object[] newValues, Map<String, Object> result);

    /**
     * Evaluate a function which depends on two rows' columns.
     * @param previousRow previous row
     * @param newRow new row
     * @param result of evaluation as (column, value) map
     */
    public void evaluate(GenericRow previousRow, GenericRow newRow, Map<String, Object> result);
}
