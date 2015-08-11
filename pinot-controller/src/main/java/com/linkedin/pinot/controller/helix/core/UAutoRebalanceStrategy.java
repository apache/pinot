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
package com.linkedin.pinot.controller.helix.core;

import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.log4j.Logger;

import java.util.*;


public class UAutoRebalanceStrategy {

    private static Logger logger = Logger.getLogger(UAutoRebalanceStrategy.class);

    private final String _resourceName;
    private final List<String> _partitions;
    private final LinkedHashMap<String, Integer> _states;
    private final int _maximumPerNode;
    private final ReplicaPlacementScheme _placementScheme;

    private Map<String, Node> _nodeMap;
    private List<Node> _liveNodesList;
    private Map<Integer, String> _stateMap;

    private Map<Replica, Node> _preferredAssignment;
    private Map<Replica, Node> _existingPreferredAssignment;
    private Map<Replica, Node> _existingNonPreferredAssignment;
    private Set<Replica> _orphaned;

    public UAutoRebalanceStrategy(String resourceName, final List<String> partitions,
                                 final LinkedHashMap<String, Integer> states, int maximumPerNode,
                                 ReplicaPlacementScheme placementScheme) {
        _resourceName = resourceName;
        _partitions = partitions;
        _states = states;
        _maximumPerNode = maximumPerNode;
        if (placementScheme != null) {
            _placementScheme = placementScheme;
        } else {
            _placementScheme = new DefaultPlacementScheme();
        }
    }

    public UAutoRebalanceStrategy(String resourceName, final List<String> partitions,
                                 final LinkedHashMap<String, Integer> states) {
        this(resourceName, partitions, states, Integer.MAX_VALUE, new DefaultPlacementScheme());
    }

    public ZNRecord computePartitionAssignment(final List<String> liveNodes,
                                               final Map<String, Map<String, String>> currentMapping, final List<String> allNodes) {
        int numReplicas = countStateReplicas();
        ZNRecord znRecord = new ZNRecord(_resourceName);
        if (liveNodes.size() == 0) {
            return znRecord;
        }
        int distRemainder = (numReplicas * _partitions.size()) % liveNodes.size();
        int distFloor = (numReplicas * _partitions.size()) / liveNodes.size();
        _nodeMap = new HashMap<String, Node>();
        _liveNodesList = new ArrayList<Node>();

        for (String id : allNodes) {
            Node node = new Node(id);
            node.capacity = 0;
            node.hasCeilingCapacity = false;
            _nodeMap.put(id, node);
        }
        for (int i = 0; i < liveNodes.size(); i++) {
            boolean usingCeiling = false;
            int targetSize = (_maximumPerNode > 0) ? Math.min(distFloor, _maximumPerNode) : distFloor;
            if (distRemainder > 0 && targetSize < _maximumPerNode) {
                targetSize += 1;
                distRemainder = distRemainder - 1;
                usingCeiling = true;
            }
            Node node = _nodeMap.get(liveNodes.get(i));
            node.isAlive = true;
            node.capacity = targetSize;
            node.hasCeilingCapacity = usingCeiling;
            _liveNodesList.add(node);
        }

        // compute states for all replica ids
        _stateMap = generateStateMap();

        // compute the preferred mapping if all nodes were up
        _preferredAssignment = computePreferredPlacement(allNodes);
        // logger.info("preferred mapping:"+ preferredAssignment);
        // from current mapping derive the ones in preferred location
        // this will update the nodes with their current fill status
        _existingPreferredAssignment = computeExistingPreferredPlacement(currentMapping);

        // from current mapping derive the ones not in preferred location
        _existingNonPreferredAssignment = computeExistingNonPreferredPlacement(currentMapping);

        // compute orphaned replicas that are not assigned to any node
        _orphaned = computeOrphaned();
        if (logger.isInfoEnabled()) {
            logger.info("orphan = " + _orphaned);
        }

        moveNonPreferredReplicasToPreferred();

        assignOrphans();

        moveExcessReplicas();

        prepareResult(znRecord);
        return znRecord;
    }

    /**
     * Move replicas assigned to non-preferred nodes if their current node is at capacity
     * and its preferred node is under capacity.
     */
    private void moveNonPreferredReplicasToPreferred() {
        // iterate through non preferred and see if we can move them to the
        // preferred location if the donor has more than it should and stealer has
        // enough capacity
        Iterator<Map.Entry<Replica, Node>> iterator = _existingNonPreferredAssignment.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Replica, Node> entry = iterator.next();
            Replica replica = entry.getKey();
            Node donor = entry.getValue();
            Node receiver = _preferredAssignment.get(replica);
            if (donor.capacity < donor.currentlyAssigned
                    && receiver.capacity > receiver.currentlyAssigned && receiver.canAdd(replica)) {
                donor.currentlyAssigned = donor.currentlyAssigned - 1;
                receiver.currentlyAssigned = receiver.currentlyAssigned + 1;
                donor.nonPreferred.remove(replica);
                receiver.preferred.add(replica);
                donor.newReplicas.remove(replica);
                receiver.newReplicas.add(replica);
                iterator.remove();
            }
        }
    }

    /**
     * Slot in orphaned partitions randomly so as to maintain even load on live nodes.
     */
    private void assignOrphans() {
        // now iterate over nodes and remaining orphaned partitions and assign
        // partitions randomly
        // Better to iterate over orphaned partitions first
        Iterator<Replica> it = _orphaned.iterator();
        while (it.hasNext()) {
            Replica replica = it.next();
            boolean added = false;
            int startIndex = computeRandomStartIndex(replica);
            for (int index = startIndex; index < startIndex + _liveNodesList.size(); index++) {
                Node receiver = _liveNodesList.get(index % _liveNodesList.size());
                if (receiver.capacity > receiver.currentlyAssigned && receiver.canAdd(replica)) {
                    receiver.currentlyAssigned = receiver.currentlyAssigned + 1;
                    receiver.nonPreferred.add(replica);
                    receiver.newReplicas.add(replica);
                    added = true;
                    break;
                }
            }
            if (!added) {
                // try adding the replica by making room for it
                added = assignOrphanByMakingRoom(replica);
            }
            if (added) {
                it.remove();
            }
        }
        if (_orphaned.size() > 0 && logger.isInfoEnabled()) {
            logger.info("could not assign nodes to partitions: " + _orphaned);
        }
    }

    /**
     * If an orphan can't be assigned normally, see if a node can borrow capacity to accept it
     * @param replica The replica to assign
     * @return true if the assignment succeeded, false otherwise
     */
    private boolean assignOrphanByMakingRoom(Replica replica) {
        Node capacityDonor = null;
        Node capacityAcceptor = null;
        int startIndex = computeRandomStartIndex(replica);
        for (int index = startIndex; index < startIndex + _liveNodesList.size(); index++) {
            Node current = _liveNodesList.get(index % _liveNodesList.size());
            if (current.hasCeilingCapacity && current.capacity > current.currentlyAssigned
                    && !current.canAddIfCapacity(replica) && capacityDonor == null) {
                // this node has space but cannot accept the node
                capacityDonor = current;
            } else if (!current.hasCeilingCapacity && current.capacity == current.currentlyAssigned
                    && current.canAddIfCapacity(replica) && capacityAcceptor == null) {
                // this node would be able to accept the replica if it has ceiling capacity
                capacityAcceptor = current;
            }
            if (capacityDonor != null && capacityAcceptor != null) {
                break;
            }
        }
        if (capacityDonor != null && capacityAcceptor != null) {
            // transfer ceiling capacity and add the node
            capacityAcceptor.steal(capacityDonor, replica);
            return true;
        }
        return false;
    }

    /**
     * Move replicas from too-full nodes to nodes that can accept the replicas
     */
    private void moveExcessReplicas() {
        // iterate over nodes and move extra load
        Iterator<Replica> it;
        for (Node donor : _liveNodesList) {
            if (donor.capacity < donor.currentlyAssigned) {
                Collections.sort(donor.nonPreferred);
                it = donor.nonPreferred.iterator();
                while (it.hasNext()) {
                    Replica replica = it.next();
                    int startIndex = computeRandomStartIndex(replica);
                    for (int index = startIndex; index < startIndex + _liveNodesList.size(); index++) {
                        Node receiver = _liveNodesList.get(index % _liveNodesList.size());
                        if (receiver.canAdd(replica)) {
                            receiver.currentlyAssigned = receiver.currentlyAssigned + 1;
                            receiver.nonPreferred.add(replica);
                            donor.currentlyAssigned = donor.currentlyAssigned - 1;
                            it.remove();
                            break;
                        }
                    }
                    if (donor.capacity >= donor.currentlyAssigned) {
                        break;
                    }
                }
                if (donor.capacity < donor.currentlyAssigned) {
                    logger.warn("Could not take partitions out of node:" + donor.id);
                }
            }
        }
    }

    /**
     * Update a ZNRecord with the results of the rebalancing.
     * @param znRecord
     */
    private void prepareResult(ZNRecord znRecord) {
        // The map fields are keyed on partition name to a pair of node and state, i.e. it
        // indicates that the partition with given state is served by that node
        //
        // The list fields are also keyed on partition and list all the nodes serving that partition.
        // This is useful to verify that there is no node serving multiple replicas of the same
        // partition.
        Map<String, List<String>> newPreferences = new TreeMap<String, List<String>>();
        for (String partition : _partitions) {
            znRecord.setMapField(partition, new TreeMap<String, String>());
            znRecord.setListField(partition, new ArrayList<String>());
            newPreferences.put(partition, new ArrayList<String>());
        }

        // for preference lists, the rough priority that we want is:
        // [existing preferred, existing non-preferred, non-existing preferred, non-existing
        // non-preferred]
        for (Node node : _liveNodesList) {
            for (Replica replica : node.preferred) {
                if (node.newReplicas.contains(replica)) {
                    newPreferences.get(replica.partition).add(node.id);
                } else {
                    znRecord.getListField(replica.partition).add(node.id);
                }
            }
        }
        for (Node node : _liveNodesList) {
            for (Replica replica : node.nonPreferred) {
                if (node.newReplicas.contains(replica)) {
                    newPreferences.get(replica.partition).add(node.id);
                } else {
                    znRecord.getListField(replica.partition).add(node.id);
                }
            }
        }
        normalizePreferenceLists(znRecord.getListFields(), newPreferences);

        // generate preference maps based on the preference lists
        for (String partition : _partitions) {
            List<String> preferenceList = znRecord.getListField(partition);
            int i = 0;
            for (String participant : preferenceList) {
                znRecord.getMapField(partition).put(participant, _stateMap.get(i));
                i++;
            }
        }
    }

    /**
     * Adjust preference lists to reduce the number of same replicas on an instance. This will
     * separately normalize two sets of preference lists, and then append the results of the second
     * set to those of the first. This basically ensures that existing replicas are automatically
     * preferred.
     * @param preferenceLists map of (partition --> list of nodes)
     * @param newPreferences map containing node preferences not consistent with the current
     *          assignment
     */
    private void normalizePreferenceLists(Map<String, List<String>> preferenceLists,
                                          Map<String, List<String>> newPreferences) {

        Map<String, Map<String, Integer>> nodeReplicaCounts =
                new HashMap<String, Map<String, Integer>>();
        for (String partition : preferenceLists.keySet()) {
            normalizePreferenceList(preferenceLists.get(partition), nodeReplicaCounts);
        }
        for (String partition : newPreferences.keySet()) {
            normalizePreferenceList(newPreferences.get(partition), nodeReplicaCounts);
            preferenceLists.get(partition).addAll(newPreferences.get(partition));
        }
    }

    /**
     * Adjust a single preference list for replica assignment imbalance
     * @param preferenceList list of node names
     * @param nodeReplicaCounts map of (node --> state --> count)
     */
    private void normalizePreferenceList(List<String> preferenceList,
                                         Map<String, Map<String, Integer>> nodeReplicaCounts) {
        List<String> newPreferenceList = new ArrayList<String>();
        int replicas = Math.min(countStateReplicas(), preferenceList.size());

        // make this a LinkedHashSet to preserve iteration order
        Set<String> notAssigned = new LinkedHashSet<String>(preferenceList);
        for (int i = 0; i < replicas; i++) {
            String state = _stateMap.get(i);
            String node = getMinimumNodeForReplica(state, notAssigned, nodeReplicaCounts);
            newPreferenceList.add(node);
            notAssigned.remove(node);
            Map<String, Integer> counts = nodeReplicaCounts.get(node);
            counts.put(state, counts.get(state) + 1);
        }
        preferenceList.clear();
        preferenceList.addAll(newPreferenceList);
    }

    /**
     * Get the node which hosts the fewest of a given replica
     * @param state the state
     * @param nodes nodes to check
     * @param nodeReplicaCounts current assignment of replicas
     * @return the node most willing to accept the replica
     */
    private String getMinimumNodeForReplica(String state, Set<String> nodes,
                                            Map<String, Map<String, Integer>> nodeReplicaCounts) {
        String minimalNode = null;
        int minimalCount = Integer.MAX_VALUE;
        for (String node : nodes) {
            int count = getReplicaCountForNode(state, node, nodeReplicaCounts);
            if (count < minimalCount) {
                minimalCount = count;
                minimalNode = node;
            }
        }
        return minimalNode;
    }

    /**
     * Safe check for the number of replicas of a given id assiged to a node
     * @param state the state to assign
     * @param node the node to check
     * @param nodeReplicaCounts a map of node to replica id and counts
     * @return the number of currently assigned replicas of the given id
     */
    private int getReplicaCountForNode(String state, String node,
                                       Map<String, Map<String, Integer>> nodeReplicaCounts) {
        if (!nodeReplicaCounts.containsKey(node)) {
            Map<String, Integer> replicaCounts = new HashMap<String, Integer>();
            replicaCounts.put(state, 0);
            nodeReplicaCounts.put(node, replicaCounts);
            return 0;
        }
        Map<String, Integer> replicaCounts = nodeReplicaCounts.get(node);
        if (!replicaCounts.containsKey(state)) {
            replicaCounts.put(state, 0);
            return 0;
        }
        return replicaCounts.get(state);
    }

    /**
     * Compute the subset of the current mapping where replicas are not mapped according to their
     * preferred assignment.
     * @param currentMapping Current mapping of replicas to nodes
     * @return The current assignments that do not conform to the preferred assignment
     */
    private Map<Replica, Node> computeExistingNonPreferredPlacement(
            Map<String, Map<String, String>> currentMapping) {
        Map<Replica, Node> existingNonPreferredAssignment = new TreeMap<Replica, Node>();
        int count = countStateReplicas();
        for (String partition : currentMapping.keySet()) {
            Map<String, String> nodeStateMap = currentMapping.get(partition);
            nodeStateMap.keySet().retainAll(_nodeMap.keySet());
            for (String nodeId : nodeStateMap.keySet()) {
                Node node = _nodeMap.get(nodeId);
                boolean skip = false;
                for (Replica replica : node.preferred) {
                    if (replica.partition.equals(partition)) {
                        skip = true;
                        break;
                    }
                }
                if (skip) {
                    continue;
                }
                // check if its in one of the preferred position
                for (int replicaId = 0; replicaId < count; replicaId++) {
                    Replica replica = new Replica(partition, replicaId);
                    if (!_preferredAssignment.containsKey(replica)) {

                        logger.info("partitions: " + _partitions);
                        logger.info("currentMapping.keySet: " + currentMapping.keySet());
                        throw new IllegalArgumentException("partition: " + replica + " is in currentMapping but not in partitions");
                    }

                    if (_preferredAssignment.get(replica).id != node.id
                            && !_existingPreferredAssignment.containsKey(replica)
                            && !existingNonPreferredAssignment.containsKey(replica)) {
                        existingNonPreferredAssignment.put(replica, node);
                        node.nonPreferred.add(replica);

                        break;
                    }
                }
            }
        }
        return existingNonPreferredAssignment;
    }

    /**
     * Get a live node index to try first for a replica so that each possible start index is
     * roughly uniformly assigned.
     * @param replica The replica to assign
     * @return The starting node index to try
     */
    private int computeRandomStartIndex(final Replica replica) {
        return (replica.hashCode() & 0x7FFFFFFF) % _liveNodesList.size();
    }

    /**
     * Get a set of replicas not currently assigned to any node
     * @return Unassigned replicas
     */
    private Set<Replica> computeOrphaned() {
        Set<Replica> orphanedPartitions = new TreeSet<Replica>(_preferredAssignment.keySet());
        for (Replica r : _existingPreferredAssignment.keySet()) {
            if (orphanedPartitions.contains(r)) {
                orphanedPartitions.remove(r);
            }
        }
        for (Replica r : _existingNonPreferredAssignment.keySet()) {
            if (orphanedPartitions.contains(r)) {
                orphanedPartitions.remove(r);
            }
        }

        return orphanedPartitions;
    }

    /**
     * Determine the replicas already assigned to their preferred nodes
     * @param currentMapping Current assignment of replicas to nodes
     * @return Assignments that conform to the preferred placement
     */
    private Map<Replica, Node> computeExistingPreferredPlacement(
            final Map<String, Map<String, String>> currentMapping) {
        Map<Replica, Node> existingPreferredAssignment = new TreeMap<Replica, Node>();
        int count = countStateReplicas();
        for (String partition : currentMapping.keySet()) {
            Map<String, String> nodeStateMap = currentMapping.get(partition);
            nodeStateMap.keySet().retainAll(_nodeMap.keySet());
            for (String nodeId : nodeStateMap.keySet()) {
                Node node = _nodeMap.get(nodeId);
                node.currentlyAssigned = node.currentlyAssigned + 1;
                // check if its in one of the preferred position
                for (int replicaId = 0; replicaId < count; replicaId++) {
                    Replica replica = new Replica(partition, replicaId);
                    if (_preferredAssignment.containsKey(replica)
                            && !existingPreferredAssignment.containsKey(replica)
                            && _preferredAssignment.get(replica).id == node.id) {
                        existingPreferredAssignment.put(replica, node);
                        node.preferred.add(replica);
                        break;
                    }
                }
            }
        }

        return existingPreferredAssignment;
    }

    /**
     * Given a predefined set of all possible nodes, compute an assignment of replicas to
     * nodes that evenly assigns all replicas to nodes.
     * @param allNodes Identifiers to all nodes, live and non-live
     * @return Preferred assignment of replicas
     */
    private Map<Replica, Node> computePreferredPlacement(final List<String> allNodes) {
        Map<Replica, Node> preferredMapping;
        preferredMapping = new HashMap<Replica, Node>();
        int partitionId = 0;
        int numReplicas = countStateReplicas();
        int count = countStateReplicas();
        for (String partition : _partitions) {
            for (int replicaId = 0; replicaId < count; replicaId++) {
                Replica replica = new Replica(partition, replicaId);
                String nodeName =
                        _placementScheme.getLocation(partitionId, replicaId, _partitions.size(), numReplicas,
                                allNodes);
                preferredMapping.put(replica, _nodeMap.get(nodeName));
            }
            partitionId = partitionId + 1;
        }
        return preferredMapping;
    }

    /**
     * Counts the total number of replicas given a state-count mapping
     * @return
     */
    private int countStateReplicas() {
        int total = 0;
        for (Integer count : _states.values()) {
            total += count;
        }
        return total;
    }

    /**
     * Compute a map of replica ids to state names
     * @return Map: replica id -> state name
     */
    private Map<Integer, String> generateStateMap() {
        int replicaId = 0;
        Map<Integer, String> stateMap = new HashMap<Integer, String>();
        for (String state : _states.keySet()) {
            Integer count = _states.get(state);
            for (int i = 0; i < count; i++) {
                stateMap.put(replicaId, state);
                replicaId++;
            }
        }
        return stateMap;
    }

    /**
     * A Node is an entity that can serve replicas. It has a capacity and knowledge
     * of replicas assigned to it, so it can decide if it can receive additional replicas.
     */
    class Node {
        public int currentlyAssigned;
        public int capacity;
        public boolean hasCeilingCapacity;
        private final String id;
        boolean isAlive;
        private final List<Replica> preferred;
        private final List<Replica> nonPreferred;
        private final Set<Replica> newReplicas;

        public Node(String id) {
            preferred = new ArrayList<Replica>();
            nonPreferred = new ArrayList<Replica>();
            newReplicas = new TreeSet<Replica>();
            currentlyAssigned = 0;
            isAlive = false;
            this.id = id;
        }

        /**
         * Check if this replica can be legally added to this node
         * @param replica The replica to test
         * @return true if the assignment can be made, false otherwise
         */
        public boolean canAdd(Replica replica) {
            if (currentlyAssigned >= capacity) {
                return false;
            }
            return canAddIfCapacity(replica);
        }

        /**
         * Check if this replica can be legally added to this node, provided that it has enough
         * capacity.
         * @param replica The replica to test
         * @return true if the assignment can be made, false otherwise
         */
        public boolean canAddIfCapacity(Replica replica) {
            if (!isAlive) {
                return false;
            }
            for (Replica r : preferred) {
                if (r.partition.equals(replica.partition)) {
                    return false;
                }
            }
            for (Replica r : nonPreferred) {
                if (r.partition.equals(replica.partition)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Receive a replica by stealing capacity from another Node
         * @param donor The node that has excess capacity
         * @param replica The replica to receive
         */
        public void steal(Node donor, Replica replica) {
            donor.hasCeilingCapacity = false;
            donor.capacity--;
            hasCeilingCapacity = true;
            capacity++;
            currentlyAssigned++;
            nonPreferred.add(replica);
            newReplicas.add(replica);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("##########\nname=").append(id).append("\npreferred:").append(preferred.size())
                    .append("\nnonpreferred:").append(nonPreferred.size());
            return sb.toString();
        }
    }

    /**
     * A Replica is a combination of a partition of the resource, the state the replica is in
     * and an identifier signifying a specific replica of a given partition and state.
     */
    class Replica implements Comparable<Replica> {
        private String partition;
        private int replicaId; // this is a partition-relative id
        private String format;

        public Replica(String partition, int replicaId) {
            this.partition = partition;
            this.replicaId = replicaId;
            this.format = this.partition + "|" + this.replicaId;
        }

        @Override
        public String toString() {
            return format;
        }

        @Override
        public boolean equals(Object that) {
            if (that instanceof Replica) {
                return this.format.equals(((Replica) that).format);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return this.format.hashCode();
        }

        @Override
        public int compareTo(Replica that) {
            if (that instanceof Replica) {
                return this.format.compareTo(that.format);
            }
            return -1;
        }
    }

    /**
     * Interface for providing a custom approach to computing a replica's affinity to a node.
     */
    public interface ReplicaPlacementScheme {
        /**
         * Initialize global state
         * @param manager The instance to which this placement is associated
         */
        public void init(final HelixManager manager);

        /**
         * Given properties of this replica, determine the node it would prefer to be served by
         * @param partitionId The current partition
         * @param replicaId The current replica with respect to the current partition
         * @param numPartitions The total number of partitions
         * @param numReplicas The total number of replicas per partition
         * @param nodeNames A list of identifiers of all nodes, live and non-live
         * @return The name of the node that would prefer to serve this replica
         */
        public String getLocation(int partitionId, int replicaId, int numPartitions, int numReplicas,
                                  final List<String> nodeNames);
    }

    /**
     * Compute preferred placements based on a default strategy that assigns replicas to nodes as
     * evenly as possible while avoiding placing two replicas of the same partition on any node.
     */
    public static class DefaultPlacementScheme implements ReplicaPlacementScheme {
        @Override
        public void init(final HelixManager manager) {
            // do nothing since this is independent of the manager
        }

        @Override
        public String getLocation(int partitionId, int replicaId, int numPartitions, int numReplicas,
                                  final List<String> nodeNames) {
            int index;
            if (nodeNames.size() > numPartitions) {
                // assign replicas in partition order in case there are more nodes than partitions
                index = (partitionId + replicaId * numPartitions) % nodeNames.size();
            } else if (nodeNames.size() == numPartitions) {
                // need a replica offset in case the sizes of these sets are the same
                index =
                        ((partitionId + replicaId * numPartitions) % nodeNames.size() + replicaId)
                                % nodeNames.size();
            } else {
                // in all other cases, assigning a replica at a time for each partition is reasonable
                    index = (partitionId + replicaId) % nodeNames.size();
            }
            return nodeNames.get(index);
        }
    }
//
//    public static void main(String[] args) {
//         List<String> partitions = new ArrayList<String>();
//        partitions.add("flights_0");
//        partitions.add("flights_1");
//
//        LinkedHashMap<String,Integer> stateCountMap = new LinkedHashMap<String, Integer>();
//        stateCountMap.put("ONLINE",1);
//
//        UAutoRebalanceStrategy uAutoRebalanceStrategy = new UAutoRebalanceStrategy("flights_OFFLINE",partitions,stateCountMap,1,new DefaultPlacementScheme());
//
//        List<String> liveNodes = new ArrayList<String>();
//        liveNodes.add("Server_192.168.11.204_8094");
//
//        List<String> allNodes = new ArrayList<String>(liveNodes);
//
//        Map<String,Map<String,String>> currentHashMap = new HashMap<String, Map<String, String>>();
//        Map<String,String> partMap = new HashMap<String, String>();
//        partMap.put("Server_192.168.11.204_8094","ONLINE");
//        currentHashMap.put("flights_0",partMap);
//
//        ZNRecord znRecord = uAutoRebalanceStrategy.computePartitionAssignment(liveNodes, currentHashMap, allNodes);
//
//        System.out.println(znRecord.getMapFields());
//
//    }
}

