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

/// **View generation — task scheduling.**
///
/// Controller-side minion task scheduler.  Walks the catalog of materialized views, decides
/// which time windows of the MV need to be (re)materialized (cold-start, watermark advance,
/// drift recovery), and generates `PinotTaskConfig` records that the minion picks up and
/// runs through the executor in [org.apache.pinot.materializedview.executor].
///
/// The scheduler updates the runtime znode in [org.apache.pinot.materializedview.metadata]
/// when tasks succeed; the broker rewrite engine in
/// [org.apache.pinot.materializedview.rewrite] reads that znode to know which time ranges
/// are queryable from the MV.
package org.apache.pinot.materializedview.scheduler;
