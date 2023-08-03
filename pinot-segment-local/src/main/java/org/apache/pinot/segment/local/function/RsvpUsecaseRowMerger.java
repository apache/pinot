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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RsvpUsecaseRowMerger extends BaseRowMerger {

    private static final Logger LOGGER = LoggerFactory.getLogger(RsvpUsecaseRowMerger.class);

    private static final List<String> prevArgs = Arrays.asList("group_id");
    private static final List<String> newArgs = Arrays.asList("group_city");

    @Override
    public List<String> getPrevRowArgs() {
        return prevArgs;
    }

    @Override
    public List<String> getNewRowArgs() {
        return newArgs;
    }

    @Override
    public void evaluate(Object[] prevValues, Object[] newValues, Map<String, Object> result) {
        result.clear();
        try {
            Long old_groupId = (Long) prevValues[0];
            String new_group_city = (String) newValues[0];

            if(old_groupId % 2 == 0) {
                result.put(newArgs.get(0), new_group_city+"_even");
            }
            else {
                result.put(newArgs.get(0), new_group_city+"_odd");
            }
        }
        catch (Exception e) {
            LOGGER.error("Failed to merge rows", e);
        }
    }

}
