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

import React, { useState, useEffect } from 'react';
import { flatten, uniq } from 'lodash';
import CustomizedTables from './Table';
import { PinotTableDetails, TableData } from 'Models';
import { getQueryTables, getTenantTable } from '../requests';
import Utils from '../utils/Utils';
import PinotMethodUtils from '../utils/PinotMethodUtils';

type BaseProps = {
  title: string;
  baseUrl: string;
  onTableNamesLoaded?: Function;
};

type InstanceProps = BaseProps & {
  tenants?: never;
  instance?: string;
};

type TenantProps = BaseProps & {
  tenants?: string[];
  instance?: never;
};

type Props = InstanceProps | TenantProps;

const TableTooltipData = [
  null,
  'Uncompressed size of all data segments with replication',
  'Estimated size of all data segments with replication, in case any servers are not reachable for actual size',
  null,
  'GOOD if all replicas of all segments are up',
];

export const AsyncPinotTables = ({
  title,
  instance,
  tenants,
  baseUrl,
  onTableNamesLoaded,
}: Props) => {
  const columnHeaders = [
    'Table Name',
    'Reported Size',
    'Estimated Size',
    'Number of Segments',
    'Status',
  ];
  const [tableData, setTableData] = useState<TableData>(
    Utils.getLoadingTableData(columnHeaders)
  );

  const fetchTenantData = async (tenants: string[]) => {
    Promise.all(
      tenants.map((tenant) => {
        return getTenantTable(tenant);
      })
    ).then((results) => {
      let allTableDetails = results.map((result) => {
        return result.data.tables.map((tableName) => {
          const tableDetails: PinotTableDetails = {
            name: tableName,
            estimated_size: null,
            reported_size: null,
            segment_status: null,
            number_of_segments: null,
          };
          return tableDetails;
        });
      });
      const allTableDetailsFlattened: PinotTableDetails[] = flatten(
        allTableDetails
      );

      const loadingTableData: TableData = {
        columns: columnHeaders,
        records: allTableDetailsFlattened.map((td) =>
          Utils.pinotTableDetailsFormat(td)
        ),
      };
      setTableData(loadingTableData);
      if (onTableNamesLoaded) {
        onTableNamesLoaded();
      }

      results.forEach((result) => {
        fetchAllTableDetails(result.data.tables);
      });
    });
  };

  const fetchInstanceTenants = async (instance: string): Promise<string[]> => {
    return PinotMethodUtils.getInstanceDetails(instance).then(
      (instanceDetails) => {
        const tenants = instanceDetails.tags
          .filter((tag) => {
            return (
              tag.search('_BROKER') !== -1 ||
              tag.search('_REALTIME') !== -1 ||
              tag.search('_OFFLINE') !== -1
            );
          })
          .map((tag) => {
            return Utils.splitStringByLastUnderscore(tag)[0];
          });
        return uniq(tenants);
      }
    );
  };

  const fetchAllTablesData = async () => {
    Promise.all([getQueryTables('realtime'), getQueryTables('offline')]).then(
      (results) => {
        const realtimeTables = results[0].data.tables;
        const offlineTables = results[1].data.tables;
        const allTables = realtimeTables.concat(offlineTables);
        setTableData({
          columns: columnHeaders,
          records: allTables.map((tableName) => {
            const tableDetails: PinotTableDetails = {
              name: tableName,
              estimated_size: null,
              reported_size: null,
              segment_status: null,
              number_of_segments: null,
            };
            return Utils.pinotTableDetailsFormat(tableDetails);
          }),
        });
        if (onTableNamesLoaded) {
          onTableNamesLoaded();
        }
        fetchAllTableDetails(allTables);
      }
    );
  };

  const fetchAllTableDetails = async (tables: string[]) => {
    return tables.forEach((tableName) => {
      PinotMethodUtils.getSegmentCountAndStatus(tableName).then(
        ({ segment_count, segment_status }) => {
          setTableData((prevState) => {
            const newRecords = [...prevState.records];
            const index = newRecords.findIndex(
              (record) => record[0] === tableName
            );
            newRecords[index] = Utils.pinotTableDetailsFormat({
              ...Utils.pinotTableDetailsFromArray(newRecords[index]),
              number_of_segments: segment_count,
              segment_status: segment_status,
            });
            return { ...prevState, records: newRecords };
          });
        }
      );

      PinotMethodUtils.getTableSizes(tableName).then(
        ({ reported_size, estimated_size }) => {
          setTableData((prevState) => {
            const newRecords = [...prevState.records];
            const index = newRecords.findIndex(
              (record) => record[0] === tableName
            );
            newRecords[index] = Utils.pinotTableDetailsFormat({
              ...Utils.pinotTableDetailsFromArray(newRecords[index]),
              estimated_size: estimated_size,
              reported_size: reported_size,
            });
            return { ...prevState, records: newRecords };
          });
        }
      );
    });
  };

  useEffect(() => {
    if (instance) {
      fetchInstanceTenants(instance).then((tenants) => {
        fetchTenantData(tenants);
      });
    } else if (tenants) {
      fetchTenantData(tenants);
    } else {
      fetchAllTablesData();
    }
  }, [instance, tenants]);

  return (
    <CustomizedTables
      title={title}
      data={tableData}
      tooltipData={TableTooltipData}
      addLinks
      baseURL={baseUrl}
      showSearchBox={true}
      inAccordionFormat={true}
    />
  );
};

export default AsyncPinotTables;
