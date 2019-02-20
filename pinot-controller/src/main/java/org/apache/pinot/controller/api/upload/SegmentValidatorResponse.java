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
package org.apache.pinot.controller.api.upload;

import java.util.List;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.controller.validation.StorageQuotaChecker;


public class SegmentValidatorResponse {
  private final TableConfig _offlineTableConfig;
  private final ZNRecord _segmentMetadataZnRecord;
  private final List<String> _assignedInstances;
  private final StorageQuotaChecker.QuotaCheckerResponse _quotaCheckerResponse;

  public SegmentValidatorResponse(TableConfig offlineTableConfig, ZNRecord segmentMetadataZnRecord, List<String> assignedInstances, StorageQuotaChecker.QuotaCheckerResponse quotaCheckerResponse) {
    _offlineTableConfig = offlineTableConfig;
    _segmentMetadataZnRecord = segmentMetadataZnRecord;
    _assignedInstances = assignedInstances;
    _quotaCheckerResponse = quotaCheckerResponse;
  }

  public TableConfig getOfflineTableConfig() {
    return _offlineTableConfig;
  }

  public ZNRecord getSegmentMetadataZnRecord() {
    return _segmentMetadataZnRecord;
  }

  public List<String> getAssignedInstances() {
    return _assignedInstances;
  }

  public StorageQuotaChecker.QuotaCheckerResponse getQuotaCheckerResponse() {
    return _quotaCheckerResponse;
  }
}
