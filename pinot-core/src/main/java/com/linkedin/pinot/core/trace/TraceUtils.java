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

import java.util.concurrent.ConcurrentLinkedDeque;

public class TraceUtils {
    /**
     * Expects a TreeNode collection where each node points to its parent.
     * Builds the full tree and returns the root (which has to have a
     * null parent).
     */
    private static Trace linkToTree(Iterable<Trace> nodes) {
        Trace root = null;

        for (Trace node : nodes) {
            final Trace parent = node._parent;

            // try to detect the root node
            if (parent == null) {
                root = node;
            }
            // add this node to the parent's left slot if it's empty
            else {
                parent._children.add(node);
            }
        }

        return root;
    }

    /**
     * TODO: it is a malformed json, fix it.
     * @param deque
     * @return
     */
    public static String getTraceTree(ConcurrentLinkedDeque<Trace> deque) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        linkToTree(deque).getTraceTree(sb, 0);
        return sb.toString();
    }

    public static String getTraceString(ConcurrentLinkedDeque<Trace> deque) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        int cnt = 0;
        for (Trace trace: deque) {
            if (cnt > 0) {
                sb.append(", ");
            }
            sb.append(trace.toString());
            cnt += 1;
        }
        sb.append("]");
        return sb.toString();
    }
}