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
package org.apache.pinot.query.planner.logical;

import com.google.common.base.Preconditions;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.pinot.query.planner.plannode.BasePlanNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;


/**
 * This represents a mathematical partition of the stages in a query plan, grouping the stages in sets of disjoint
 * stages.
 *
 * It is important to understand that this class assumes all stages that are stored belong to the same query plan
 * and therefore their stage ids are unique. It also assumes that the same stage instances are being used when
 * methods like {@link #containsStage(MailboxSendNode)} are called.
 *
 * The original reason to have this class was to group equivalent stages together, although it can be used for other
 * purposes.
 *
 * Although the only implementation provided so far ({@link Mutable}) is mutable, the class is designed
 * to be immutable from the outside. This is because it is difficult to manipulate grouped stages directly without
 * breaking the invariants of the class, so it is better to be sure it is not modified after it is calculated.
 */
public abstract class GroupedStages {

  public static final Comparator<MailboxSendNode> STAGE_COMPARATOR = Comparator.comparing(BasePlanNode::getStageId);
  public static final Comparator<SortedSet<MailboxSendNode>> GROUP_COMPARATOR
      = Comparator.comparing(group -> group.first().getStageId());

  public abstract boolean containsStage(MailboxSendNode stage);

  /**
   * Returns the group of equivalent stages that contains the given stage.
   *
   * The set is sorted by the stage id.
   */
  public abstract SortedSet<MailboxSendNode> getGroup(MailboxSendNode stage)
      throws NoSuchElementException;

  /**
   * Returns the leaders of each group.
   *
   * The leader of a group is the stage with the smallest stage id in the group.
   */
  public abstract SortedSet<MailboxSendNode> getLeaders();

  /**
   * Returns the groups.
   *
   * Each set contains the stages that are grouped. These sets are disjoint. The union of these sets is the set of all
   * stages known by this object.
   *
   * The result is sorted by the leader of each group and each group is sorted by the stage id.
   */
  public abstract SortedSet<SortedSet<MailboxSendNode>> getGroups();

  @Override
  public String toString() {
    String content = getGroups().stream()
        .map(group ->
            "[" + group.stream()
                .map(stage -> Integer.toString(stage.getStageId()))
                .collect(Collectors.joining(", ")) + "]"
        )
        .collect(Collectors.joining(", "));

    return "[" + content + "]";
  }

  /**
   * A mutable version of {@link GroupedStages}.
   */
  public static class Mutable extends GroupedStages {
    /**
     * All groups of stages.
     *
     * Although these groups are never empty, a group may contain only one stage if it is not grouped with any other
     * stage.
     */
    private final SortedSet<SortedSet<MailboxSendNode>> _groups = new TreeSet<>(GROUP_COMPARATOR);

    /**
     * Map from stage to the group of stages it belongs to.
     */
    private final IdentityHashMap<MailboxSendNode, SortedSet<MailboxSendNode>> _stageToGroup = new IdentityHashMap<>();

    /**
     * Adds a new group of equivalent stages.
     *
     * @param node The stage that will be the only member of the group.
     * @return this object
     * @throws IllegalArgumentException if the stage was already added.
     */
    public Mutable addNewGroup(MailboxSendNode node) {
      Preconditions.checkArgument(!containsStage(node), "Stage {} was already added", node.getStageId());
      SortedSet<MailboxSendNode> group = new TreeSet<>(STAGE_COMPARATOR);
      group.add(node);
      _groups.add(group);
      _stageToGroup.put(node, group);
      return this;
    }

    /**
     * Adds a stage to an existing group.
     * @param original A stage that is already in the group.
     * @param newNode The stage to be added to the group.
     * @return this object
     */
    public Mutable addToGroup(MailboxSendNode original, MailboxSendNode newNode) {
      Preconditions.checkArgument(!containsStage(newNode), "Stage {} was already added", newNode.getStageId());
      SortedSet<MailboxSendNode> group = getGroup(original);
      group.add(newNode);
      _stageToGroup.put(newNode, group);
      return this;
    }

    @Override
    public SortedSet<MailboxSendNode> getLeaders() {
      return _groups.stream()
          .map(SortedSet::first)
          .collect(Collectors.toCollection(() -> new TreeSet<>(STAGE_COMPARATOR)));
    }

    @Override
    public SortedSet<SortedSet<MailboxSendNode>> getGroups() {
      return _groups;
    }

    @Override
    public boolean containsStage(MailboxSendNode stage) {
      return _stageToGroup.containsKey(stage);
    }

    @Override
    public SortedSet<MailboxSendNode> getGroup(MailboxSendNode stage)
        throws NoSuchElementException {
      SortedSet<MailboxSendNode> group = _stageToGroup.get(stage);
      if (group == null) {
        throw new NoSuchElementException("Stage " + stage.getStageId() + " is unknown by this class");
      }
      return group;
    }

    public Mutable removeStage(MailboxSendNode stage) {
      SortedSet<MailboxSendNode> group = _stageToGroup.remove(stage);
      Preconditions.checkNotNull(group, "Stage {} is not part of this class", stage);
      group.remove(stage);
      if (group.isEmpty()) {
        _groups.remove(group);
      }
      return this;
    }
  }
}
