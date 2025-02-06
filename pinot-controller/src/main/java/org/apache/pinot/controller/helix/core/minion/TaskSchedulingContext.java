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
package org.apache.pinot.controller.helix.core.minion;

import java.util.Set;


/**
 * Wrapper class to manage all the inputs passed to schedule a task on minion.
 * Tasks will be scheduled based on the combination on tables, databases and taskTypes passed
 * <p>Y -> contains elements
 * <p>N -> is null or empty
 * <table>
 *   <tr>
 *     <th> tablesToSchedule </th> <th> databasesToSchedule </th> <th> tasksToSchedule </th>
 *     <th> {@link PinotTaskManager} behavior </th>
 *   </tr>
 *   <tr>
 *     <td> N </td> <td> N </td> <td> N </td>
 *     <td>schedule all the configured tasks on all tables</td>
 *   </tr>
 *   <tr>
 *     <td> Y </td> <td> N </td> <td> N </td>
 *     <td>schedule all the configured tasks on tables in tablesToSchedule</td>
 *   </tr>
 *   <tr>
 *     <td> N </td> <td> Y </td> <td> N </td>
 *     <td>schedule all the configured tasks on all tables under the databases in databasesToSchedule</td>
 *   </tr>
 *   <tr>
 *     <td> N </td> <td> N </td> <td> Y </td>
 *     <td>schedule tasksToSchedule on all tables</td>
 *   </tr>
 *   <tr>
 *     <td> N </td> <td> Y </td> <td> Y </td>
 *     <td>schedule tasksToSchedule on all tables under the databases in databasesToSchedule</td>
 *   </tr>
 *   <tr>
 *     <td> Y </td> <td> N </td> <td> Y </td>
 *     <td>schedule tasksToSchedule on tables in tablesToSchedule</td>
 *   </tr>
 *   <tr>
 *     <td> Y </td> <td> Y </td> <td> N </td>
 *     <td>schedule all the configured tasks on tables in tablesToSchedule
 *     and also on all tables under the databases in databasesToSchedule</td>
 *   </tr>
 *   <tr>
 *     <td> Y </td> <td> Y </td> <td> Y </td>
 *     <td>schedule tasksToSchedule on tables in tablesToSchedule and also
 *     on all tables under the databases in databasesToSchedule</td>
 *   </tr>
 * </table>
 *
 * In short empty tasksToSchedule will schedule tasks for all types and
 * empty tablesToSchedule and databasesToSchedule will schedule tasks for all tables
 *
 */
public class TaskSchedulingContext {
  private Set<String> _tablesToSchedule;
  private Set<String> _tasksToSchedule;
  private Set<String> _databasesToSchedule;
  private String _triggeredBy;
  private String _minionInstanceTag;
  private boolean _isLeader;

  public Set<String> getTablesToSchedule() {
    return _tablesToSchedule;
  }

  public TaskSchedulingContext setTablesToSchedule(Set<String> tablesToSchedule) {
    _tablesToSchedule = tablesToSchedule;
    return this;
  }

  public Set<String> getTasksToSchedule() {
    return _tasksToSchedule;
  }

  public TaskSchedulingContext setTasksToSchedule(Set<String> tasksToSchedule) {
    _tasksToSchedule = tasksToSchedule;
    return this;
  }

  public Set<String> getDatabasesToSchedule() {
    return _databasesToSchedule;
  }

  public TaskSchedulingContext setDatabasesToSchedule(Set<String> databasesToSchedule) {
    _databasesToSchedule = databasesToSchedule;
    return this;
  }

  public String getTriggeredBy() {
    return _triggeredBy;
  }

  public TaskSchedulingContext setTriggeredBy(String triggeredBy) {
    _triggeredBy = triggeredBy;
    return this;
  }

  public String getMinionInstanceTag() {
    return _minionInstanceTag;
  }

  public TaskSchedulingContext setMinionInstanceTag(String minionInstanceTag) {
    _minionInstanceTag = minionInstanceTag;
    return this;
  }

  public boolean isLeader() {
    return _isLeader;
  }

  public TaskSchedulingContext setLeader(boolean leader) {
    _isLeader = leader;
    return this;
  }
}
