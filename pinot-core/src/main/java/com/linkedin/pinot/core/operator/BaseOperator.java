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
package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.trace.TraceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Any other Pinot Operators should extend BaseOperator
 */
public abstract class BaseOperator implements Operator {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseOperator.class);

    @Override
    public final Block nextBlock() {
        long start = System.currentTimeMillis();
        Block ret = getNextBlock();
        long end = System.currentTimeMillis();
        LOGGER.info("Time spent in {}: {}", getOperatorName(), (end - start));
        TraceContext.log(getOperatorName() + "Time", (end - start));
        return ret;
    }

    @Override
    public final Block nextBlock(BlockId BlockId) {
        long start = System.currentTimeMillis();
        Block ret = getNextBlock(BlockId);
        long end = System.currentTimeMillis();
        LOGGER.info("Time spent in {}: {}", getOperatorName(), (end - start));
        TraceContext.log(getOperatorName() + "Time", (end - start));
        return ret;
    }

    public abstract Block getNextBlock();

    public abstract Block getNextBlock(BlockId BlockId);

    public abstract String getOperatorName();

}