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
package com.linkedin.pinot.core.trace;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Trace maintains all trace logs for a specific Runnable or Callable job.
 *
 * It is not thread-safe, so should be protected by a {@link ThreadLocal}.
 */
public class Trace {
  private final AtomicInteger traceIdGen = new AtomicInteger(0);

  private String _traceId;
  private List<String> _key = new ArrayList<String>();
  private List<Object> _value = new ArrayList<Object>();
  private Long _threadId;
  protected final Trace _parent;
  protected final List<Trace> _children = new ArrayList<Trace>();

  public Trace(Long threadId, Trace parent) {
    _threadId = threadId;
    _parent = parent;
    if (parent == null) {
      _traceId = "0";
    } else {
      _traceId = parent._traceId + "_" + parent.traceIdGen.getAndIncrement();
    }
  }

  public void log(String key, Object value) {
    _key.add(key);
    _value.add(value);
  }

  public List<Object> getValue() {
    return _value;
  }

  public List<String> getKey() {
    return _key;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"");
    sb.append(_traceId);
    sb.append("\"");
    sb.append(": ");
    sb.append("{");
    for (int i = 0; i < _key.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("\"");
      sb.append(_key.get(i));
      sb.append("\"");
      sb.append(": ");
      sb.append("\"");
      sb.append(_value.get(i));
      sb.append("\"");
    }
    sb.append("}");
    sb.append("}");
    return sb.toString();
  }

  public String getTraceTree(StringBuilder sb, int level) {
    // a tree-style trace graph
    for (int i = 0; i < level; i++) {
      sb.append("--> ");
    }
    sb.append("[TID: " + _threadId + "] ");
    sb.append(toString());
    sb.append("\n");

    for (Trace child: _children) {
      child.getTraceTree(sb, level+1);
    }

    return sb.toString();
  }
}
