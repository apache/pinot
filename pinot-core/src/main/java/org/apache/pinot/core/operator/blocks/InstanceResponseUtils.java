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
package org.apache.pinot.core.operator.blocks;

import java.util.Collection;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datatable.DataTableBuilderUtils;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.blocks.results.MetadataResultsBlock;


public final class InstanceResponseUtils {
  private InstanceResponseUtils() {
    // do not instantiate.
  }

  public static InstanceResponseBlock getEmptyResponse() {
    return new InstanceResponseBlock();
  }

  public static InstanceResponseBlock getEmptyResponseBlock(DataSchema explainResultSchema) {
    return new InstanceResponseBlock(new MetadataResultsBlock(explainResultSchema), null);
  }

  public static InstanceResponseBlock getExceptionBlock(ProcessingException exception) {
    return new InstanceResponseBlock(new ExceptionResultsBlock(exception), null);
  }

  public static DataTable toDataTable(InstanceResponseBlock instanceResponseBlock) {
    DataTable dataTable;
    try {
      if (instanceResponseBlock.getBaseResultsBlock() == null) {
        dataTable = DataTableBuilderUtils.buildEmptyDataTable();
        if (MapUtils.isNotEmpty(instanceResponseBlock.getExceptionMap())) {
          for (Map.Entry<Integer, String> e : instanceResponseBlock.getExceptionMap().entrySet()) {
            dataTable.addException(e.getKey(), e.getValue());
          }
        }
        if (MapUtils.isNotEmpty(instanceResponseBlock.getInstanceResponseMetadata())) {
          dataTable.getMetadata().putAll(instanceResponseBlock.getInstanceResponseMetadata());
        }
      } else {
         dataTable = instanceResponseBlock.getBaseResultsBlock().getDataTable(instanceResponseBlock.getQueryContext());
         dataTable.getMetadata().putAll(instanceResponseBlock.getInstanceResponseMetadata());
      }
      return dataTable;
    } catch (ProcessingException pe) {
      return toDataTable(getExceptionBlock(pe));
    } catch (Exception e) {
      return toDataTable(getExceptionBlock(QueryException.UNKNOWN_ERROR));
    }
  }

  public static Collection<Object[]> toRows(InstanceResponseBlock instanceResponseBlock) {
    try {
      return instanceResponseBlock.getBaseResultsBlock().getRows(instanceResponseBlock.getQueryContext());
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while building data table", e);
    }
  }

  public static DataTable getDataSchema(InstanceResponseBlock instanceResponseBlock) {
    try {
      return instanceResponseBlock.getBaseResultsBlock().getDataTable(instanceResponseBlock.getQueryContext());
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while building data table", e);
    }
  }
}
