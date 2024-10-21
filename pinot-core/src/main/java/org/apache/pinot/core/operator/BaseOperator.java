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
package org.apache.pinot.core.operator;

import com.google.common.base.CaseFormat;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.plan.ExplainInfo;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.trace.InvocationScope;
import org.apache.pinot.spi.trace.Tracing;


/**
 * Any other Pinot Operators should extend BaseOperator
 */
public abstract class BaseOperator<T extends Block> implements Operator<T> {

  @Override
  public final T nextBlock() {
    /* Worker also checks its corresponding runner thread's interruption periodically, worker will abort if it finds
       runner's flag is raised. If the runner thread has already acted upon the flag and reset it, then the runner
       itself will cancel all worker's futures. Therefore, the worker will interrupt even if we only kill the runner
       thread. */
    if (Tracing.ThreadAccountantOps.isInterrupted()) {
      throw new EarlyTerminationException("Interrupted while processing next block");
    }
    try (InvocationScope ignored = Tracing.getTracer().createScope(getClass())) {
      return getNextBlock();
    }
  }

  // Make it protected because we should always call nextBlock()
  protected abstract T getNextBlock();

  @Override
  public ExplainInfo getExplainInfo() {
    ExplainAttributeBuilder attributeBuilder = new ExplainAttributeBuilder();
    explainAttributes(attributeBuilder);
    return new ExplainInfo(getExplainName(), attributeBuilder.build(), getChildrenExplainInfo());
  }

  protected List<ExplainInfo> getChildrenExplainInfo() {
    return getChildOperators().stream()
        .filter(Objects::nonNull)
        .map(Operator::getExplainInfo)
        .collect(Collectors.toList());
  }

  protected String getExplainName() {
    String explainString = toExplainString();
    if (explainString == null) {
      return getClass().getSimpleName();
    }
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, explainString);
  }

  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
  }
}
