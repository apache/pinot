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
package org.apache.pinot.query.planner;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.SortNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.StageNodeVisitor;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.planner.stage.ValueNode;


/**
 * A visitor that converts a {@code QueryPlan} into a human-readable string representation.
 *
 * <p>It is currently not used programmatically and cannot be accessed by the user. Instead,
 * it is intended for use in manual debugging (e.g. setting breakpoints and calling QueryPlan#explain()).
 */
public class ExplainPlanStageVisitor implements StageNodeVisitor<StringBuilder, ExplainPlanStageVisitor.Context> {

  private final QueryPlan _queryPlan;

  /**
   * Explains the query plan.
   *
   * @see QueryPlan#explain()
   * @param queryPlan the queryPlan to explain
   * @return a String representation of the query plan tree
   */
  public static String explain(QueryPlan queryPlan) {
    if (queryPlan.getQueryStageMap().isEmpty()) {
      return "EMPTY";
    }

    // the root of a query plan always only has a single node
    ServerInstance rootServer = queryPlan.getStageMetadataMap().get(0).getServerInstances().get(0);
    return explainFrom(queryPlan, queryPlan.getQueryStageMap().get(0), rootServer);
  }

  /**
   * Explains the query plan from a specific point in the subtree, taking {@code rootServer}
   * as the node that is executing this sub-tree. This is helpful for debugging what is happening
   * at a given point in time (for example, printing the tree that will be executed on a
   * local node right before it is executed).
   *
   * @param queryPlan the entire query plan, including non-executed portions
   * @param node the node to begin traversal
   * @param rootServer the server instance that is executing this plan (should execute {@code node})
   *
   * @return a query plan associated with
   */
  public static String explainFrom(QueryPlan queryPlan, StageNode node, ServerInstance rootServer) {
    final ExplainPlanStageVisitor visitor = new ExplainPlanStageVisitor(queryPlan);
    return node
        .visit(visitor, new Context(rootServer, "", "", new StringBuilder()))
        .toString();
  }

  private ExplainPlanStageVisitor(QueryPlan queryPlan) {
    _queryPlan = queryPlan;
  }

  private StringBuilder appendInfo(StageNode node, Context context) {
    int stage = node.getStageId();
    context._builder
        .append(context._prefix)
        .append('[')
        .append(stage)
        .append("]@")
        .append(context._host.getHostname())
        .append(':')
        .append(context._host.getPort())
        .append(' ')
        .append(node.explain());
    return context._builder;
  }

  private StringBuilder visitSimpleNode(StageNode node, Context context) {
    appendInfo(node, context).append('\n');
    return node.getInputs().get(0).visit(this, context.next(false, context._host));
  }

  @Override
  public StringBuilder visitAggregate(AggregateNode node, Context context) {
    return visitSimpleNode(node, context);
  }

  @Override
  public StringBuilder visitFilter(FilterNode node, Context context) {
    return visitSimpleNode(node, context);
  }

  @Override
  public StringBuilder visitJoin(JoinNode node, Context context) {
    appendInfo(node, context).append('\n');
    node.getInputs().get(0).visit(this, context.next(true, context._host));
    node.getInputs().get(1).visit(this, context.next(false, context._host));
    return context._builder;
  }

  @Override
  public StringBuilder visitMailboxReceive(MailboxReceiveNode node, Context context) {
    appendInfo(node, context).append('\n');

    MailboxSendNode sender = (MailboxSendNode) node.getSender();
    int senderStageId = node.getSenderStageId();
    StageMetadata metadata = _queryPlan.getStageMetadataMap().get(senderStageId);
    Map<ServerInstance, Map<String, List<String>>> segments = metadata.getServerInstanceToSegmentsMap();

    Iterator<ServerInstance> iterator = metadata.getServerInstances().iterator();
    while (iterator.hasNext()) {
      ServerInstance serverInstance = iterator.next();
      if (segments.containsKey(serverInstance)) {
        // always print out leaf stages
        sender.visit(this, context.next(iterator.hasNext(), serverInstance));
      } else {
        if (!iterator.hasNext()) {
          // always print out the last one
          sender.visit(this, context.next(false, serverInstance));
        } else {
          // only print short version of the sender node
          appendMailboxSend(sender, context.next(true, serverInstance))
              .append(" (Subtree Omitted)")
              .append('\n');
        }
      }
    }
    return context._builder;
  }

  @Override
  public StringBuilder visitMailboxSend(MailboxSendNode node, Context context) {
    appendMailboxSend(node, context).append('\n');
    return node.getInputs().get(0).visit(this, context.next(false, context._host));
  }

  private StringBuilder appendMailboxSend(MailboxSendNode node, Context context) {
    appendInfo(node, context);

    int receiverStageId = node.getReceiverStageId();
    List<ServerInstance> servers = _queryPlan.getStageMetadataMap().get(receiverStageId).getServerInstances();
    context._builder.append("->");
    String receivers = servers.stream()
        .map(s -> s.getHostname() + ':' + s.getPort())
        .map(s -> "[" + receiverStageId + "]@" + s)
        .collect(Collectors.joining(",", "{", "}"));
    return context._builder.append(receivers);
  }

  @Override
  public StringBuilder visitProject(ProjectNode node, Context context) {
    return visitSimpleNode(node, context);
  }

  @Override
  public StringBuilder visitSort(SortNode node, Context context) {
    return visitSimpleNode(node, context);
  }

  @Override
  public StringBuilder visitTableScan(TableScanNode node, Context context) {
    return appendInfo(node, context)
        .append(' ')
        .append(_queryPlan.getStageMetadataMap()
            .get(node.getStageId())
            .getServerInstanceToSegmentsMap()
            .get(context._host))
        .append('\n');
  }

  @Override
  public StringBuilder visitValue(ValueNode node, Context context) {
    return appendInfo(node, context);
  }

  static class Context {
    final ServerInstance _host;
    final String _prefix;
    final String _childPrefix;
    final StringBuilder _builder;

    Context(ServerInstance host, String prefix, String childPrefix, StringBuilder builder) {
      _host = host;
      _prefix = prefix;
      _childPrefix = childPrefix;
      _builder = builder;
    }

    Context next(boolean hasMoreChildren, ServerInstance host) {
      return new Context(
          host,
          hasMoreChildren ? _childPrefix + "├── " : _childPrefix + "└── ",
          hasMoreChildren ? _childPrefix + "│   " : _childPrefix + "   ",
          _builder
      );
    }
  }
}
