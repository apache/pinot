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

import org.apache.commons.collections.map.HashedMap;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.util.ConstraintBasedAssignment;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.*;
import org.apache.log4j.Logger;

import java.util.*;

/**
 *
 */

public class UAutoRebalancer implements Rebalancer, MappingCalculator {


    private HelixManager _manager;
    private UAutoRebalanceStrategy _algorithm;

    private static final Logger LOG = Logger.getLogger(UAutoRebalancer.class);

    @Override
    public ResourceAssignment computeBestPossiblePartitionState(ClusterDataCache cache, IdealState idealState, Resource resource, CurrentStateOutput currentStateOutput) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Processing resource:" + resource.getResourceName());
        }
        String stateModelDefName = idealState.getStateModelDefRef();
        StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
        ResourceAssignment partitionMapping = new ResourceAssignment(resource.getResourceName());
        for (Partition partition : resource.getPartitions()) {
            Map<String, String> currentStateMap =
                    currentStateOutput.getCurrentStateMap(resource.getResourceName(), partition);
            Set<String> disabledInstancesForPartition =
                    cache.getDisabledInstancesForPartition(partition.toString());
            List<String> preferenceList =
                    ConstraintBasedAssignment.getPreferenceList(cache, partition, idealState, stateModelDef);
            Map<String, String> bestStateForPartition =
                    ConstraintBasedAssignment.computeAutoBestStateForPartition(cache, stateModelDef,
                            preferenceList, currentStateMap, disabledInstancesForPartition,
                            idealState.isEnabled());
            partitionMapping.addReplicaMap(partition, bestStateForPartition);
        }
        return partitionMapping;

    }

    @Override
    public void init(HelixManager manager) {
        this._manager = manager;
        this._algorithm = null;
    }

    @Override
    public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState, CurrentStateOutput currentStateOutput, ClusterDataCache clusterData) {
        LOG.info("######resourceName :"+resourceName);
        LOG.info("######current idealstate :" + currentIdealState.toString());
        LOG.info("#######current state output :" + currentStateOutput.toString());
        LOG.info("#########cluster datecache : " + clusterData.toString());

        List<String> partitions = new ArrayList<String>(currentIdealState.getPartitionSet());
        String stateModelName = currentIdealState.getStateModelDefRef();
        StateModelDefinition stateModelDef = clusterData.getStateModelDef(stateModelName);
        Map<String, LiveInstance> liveInstance = clusterData.getLiveInstances();
        String replicas = currentIdealState.getReplicas();
        LOG.info("######Partitions:" + partitions.toString() + ";live Instance:" + liveInstance + ";Replicas:" + replicas);
        LinkedHashMap<String, Integer> stateCountMap = new LinkedHashMap<String, Integer>();
        stateCountMap = stateCount(stateModelDef, liveInstance.size(), Integer.parseInt(replicas));
        List<String> liveNodes = new ArrayList<String>(liveInstance.keySet());
        List<String> allNodes = new ArrayList<String>(clusterData.getInstanceConfigMap().keySet());
        Map<String, Map<String, String>> currentMapping =
                currentMapping(currentStateOutput, resourceName, partitions, stateCountMap);

        // If there are nodes tagged with resource name, use only those nodes
        Set<String> taggedNodes = new HashSet<String>();
        Set<String> taggedLiveNodes = new HashSet<String>();
        if (currentIdealState.getInstanceGroupTag() != null) {
            for (String instanceName : allNodes) {
                if (clusterData.getInstanceConfigMap().get(instanceName)
                        .containsTag(currentIdealState.getInstanceGroupTag())) {
                    taggedNodes.add(instanceName);
                    if (liveInstance.containsKey(instanceName)) {
                        taggedLiveNodes.add(instanceName);
                    }
                }
            }
            if (!taggedLiveNodes.isEmpty()) {
                // live nodes exist that have this tag
                if (LOG.isInfoEnabled()) {
                    LOG.info("found the following participants with tag "
                            + currentIdealState.getInstanceGroupTag() + " for " + resourceName + ": "
                            + taggedLiveNodes);
                }
            } else if (taggedNodes.isEmpty()) {
                // no live nodes and no configured nodes have this tag
                LOG.warn("Resource " + resourceName + " has tag " + currentIdealState.getInstanceGroupTag()
                        + " but no configured participants have this tag");
            } else {
                // configured nodes have this tag, but no live nodes have this tag
                LOG.warn("Resource " + resourceName + " has tag " + currentIdealState.getInstanceGroupTag()
                        + " but no live participants have this tag");
            }
            allNodes = new ArrayList<String>(taggedNodes);
            liveNodes = new ArrayList<String>(taggedLiveNodes);
        }

        // sort node lists to ensure consistent preferred assignments
        Collections.sort(allNodes);
        Collections.sort(liveNodes);

        int maxPartition = currentIdealState.getMaxPartitionsPerInstance();

        if (LOG.isInfoEnabled()) {
            LOG.info("currentMapping: " + currentMapping);
            LOG.info("stateCountMap: " + stateCountMap);
            LOG.info("liveNodes: " + liveNodes);
            LOG.info("allNodes: " + allNodes);
            LOG.info("maxPartition: " + maxPartition);
        }

        ZNRecord newMapping = null;
        List<String> liveServers = new ArrayList<String>();
        List<String> allServers = new ArrayList<String>();
        List<String> liveBrokers = new ArrayList<String>();
        List<String> allBrokers = new ArrayList<String>();

        for (String tmp:liveNodes){
            if (tmp.startsWith("Broker_")){
                liveBrokers.add(tmp);
            }else{
                liveServers.add(tmp);
            }
        }
        for (String tmp:allNodes){
            if (tmp.startsWith("Broker_")){
                allBrokers.add(tmp);
            }else{
                allServers.add(tmp);
            }
        }

        if (resourceName.equals("brokerResource")){
            newMapping = calculateNewMapping(resourceName,partitions,stateCountMap,maxPartition,liveBrokers,currentMapping,allBrokers);
        } else{
            newMapping = calculateNewMapping(resourceName,partitions,stateCountMap,maxPartition,liveServers,currentMapping,allServers);
        }

//        AutoRebalanceStrategy.ReplicaPlacementScheme placementScheme = new AutoRebalanceStrategy.DefaultPlacementScheme();
//        placementScheme.init(_manager);
//        _algorithm =
//                new AutoRebalanceStrategy(resourceName, partitions, stateCountMap, maxPartition,
//                        placementScheme);
//        ZNRecord newMapping =
//                _algorithm.computePartitionAssignment(liveNodes, currentMapping, allNodes);

        if (LOG.isInfoEnabled()) {
            LOG.info("newMapping: " + newMapping);
        }
        if (isTheSameMapping(currentIdealState.getRecord().getMapFields(),newMapping.getMapFields())){
            LOG.info("The same mapping return currentIdealState");
            return currentIdealState;
        }
        IdealState newIdealState = new IdealState(resourceName);
        newIdealState.getRecord().setSimpleFields(currentIdealState.getRecord().getSimpleFields());
        newIdealState.getRecord().setMapFields(newMapping.getMapFields());
        newIdealState.getRecord().setListFields(newMapping.getListFields());
        LOG.info("#########new ideal state:"+newIdealState.toString());
        return newIdealState;
    }

    public static boolean isTheSameMapping(Map oldMapping,Map nMapping){
        if (oldMapping.size()!=nMapping.size())return false;
        Map<String, Object> total = new HashedMap(oldMapping);
        total.putAll(nMapping);
        if (total.size()!=oldMapping.size()) return false;
        Set<Boolean> result = new HashSet<Boolean>();
        for (Object oKey: oldMapping.keySet()){
            Object oValue = oldMapping.get(oKey);
            if (oValue instanceof String){
                result.add(oValue.equals(nMapping.get(oValue)));
            }else if (oValue instanceof Map){
                result.add(isTheSameMapping((Map)oValue,(Map)nMapping.get(oKey)));
            }
        }
        if (result.size()!=1) return false;
        return true;
    }

    public  ZNRecord calculateNewMapping(String resourceName,List<String> partitions,LinkedHashMap<String, Integer> stateCountMap,int maxPartition,
                                         List<String> liveNodes,Map<String, Map<String, String>> currentMapping,List<String> allNodes) {
        UAutoRebalanceStrategy.ReplicaPlacementScheme placementScheme = new UAutoRebalanceStrategy.DefaultPlacementScheme();
        placementScheme.init(_manager);
        _algorithm =
                new UAutoRebalanceStrategy(resourceName, partitions, stateCountMap, maxPartition,
                        placementScheme);
        ZNRecord newMapping =
                _algorithm.computePartitionAssignment(liveNodes, currentMapping, allNodes);
        return newMapping;
    }

    public static LinkedHashMap<String, Integer> stateCount(StateModelDefinition stateModelDef,
                                                            int liveNodesNb, int totalReplicas) {
        LinkedHashMap<String, Integer> stateCountMap = new LinkedHashMap<String, Integer>();
        List<String> statesPriorityList = stateModelDef.getStatesPriorityList();

        int replicas = totalReplicas;
        for (String state : statesPriorityList) {
            String num = stateModelDef.getNumInstancesPerState(state);
            if ("N".equals(num)) {
                stateCountMap.put(state, liveNodesNb);
            } else if ("R".equals(num)) {
                // wait until we get the counts for all other states
                continue;
            } else {
                int stateCount = -1;
                try {
                    stateCount = Integer.parseInt(num);
                } catch (Exception e) {
                    // LOG.error("Invalid count for state: " + state + ", count: " + num +
                    // ", use -1 instead");
                }

                if (stateCount > 0) {
                    stateCountMap.put(state, stateCount);
                    replicas -= stateCount;
                }
            }
        }

        // get state count for R
        for (String state : statesPriorityList) {
            String num = stateModelDef.getNumInstancesPerState(state);
            if ("R".equals(num)) {
                stateCountMap.put(state, replicas);
                // should have at most one state using R
                break;
            }
        }
        return stateCountMap;
    }

    public static Map<String, Map<String, String>> currentMapping(CurrentStateOutput currentStateOutput,
                                                            String resourceName, List<String> partitions, Map<String, Integer> stateCountMap) {

        Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();

        for (String partition : partitions) {
            Map<String, String> curStateMap =
                    currentStateOutput.getCurrentStateMap(resourceName, new Partition(partition));
            map.put(partition, new HashMap<String, String>());
            for (String node : curStateMap.keySet()) {
                String state = curStateMap.get(node);
                map.get(partition).put(node, state);
            }

            Map<String, String> pendingStateMap =
                    currentStateOutput.getPendingStateMap(resourceName, new Partition(partition));
            for (String node : pendingStateMap.keySet()) {
                String state = pendingStateMap.get(node);
                map.get(partition).put(node, state);
            }
        }
        return map;
    }
}
