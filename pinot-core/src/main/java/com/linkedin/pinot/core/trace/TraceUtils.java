package com.linkedin.pinot.core.trace;

import java.util.concurrent.ConcurrentLinkedDeque;

public class TraceUtils {
    /**
     * Expects a TreeNode collection where each node points to its parent.
     * Builds the full tree and returns the root (which has to have a
     * null parent).
     */
    private static TraceContext.Trace linkToTree(Iterable<TraceContext.Trace> nodes) {
        TraceContext.Trace root = null;

        for (TraceContext.Trace node : nodes) {
            final TraceContext.Trace parent = node._parent;

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
    public static String getTraceTree(ConcurrentLinkedDeque<TraceContext.Trace> deque) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        linkToTree(deque).getTraceTree(sb, 0);
        return sb.toString();
    }

    public static String getTraceString(ConcurrentLinkedDeque<TraceContext.Trace> deque) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        int cnt = 0;
        for (TraceContext.Trace trace: deque) {
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