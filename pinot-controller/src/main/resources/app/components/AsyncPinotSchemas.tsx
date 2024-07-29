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
import { TableData } from 'Models';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import Loading from './Loading';
import CustomizedTables from './Table';
import { getSchemaList } from '../requests';
import Utils from '../utils/Utils';

type Props = {
  onSchemaNamesLoaded?: Function;
};

export const AsyncPinotSchemas = ({ onSchemaNamesLoaded }: Props) => {
  const [schemaDetails, setSchemaDetails] = useState<TableData>(
    Utils.getLoadingTableData(PinotMethodUtils.allSchemaDetailsColumnHeader)
  );

  const fetchSchemas = async () => {
    getSchemaList().then((result) => {
      if (onSchemaNamesLoaded) {
        onSchemaNamesLoaded(result.data);
      }

      const schemas = result.data;
      const records = schemas.map((schema) => [
        ...[schema],
        ...Array(PinotMethodUtils.allSchemaDetailsColumnHeader.length - 1)
          .fill(null)
          .map((_) => Loading),
      ]);
      setSchemaDetails({
        columns: PinotMethodUtils.allSchemaDetailsColumnHeader,
        records: records,
      });

      // Schema details are typically fast to fetch, so we fetch them all at once.
      PinotMethodUtils.getAllSchemaDetails(schemas).then((result) => {
        setSchemaDetails(result);
      });
    });
  };

  useEffect(() => {
    fetchSchemas();
  }, []);

  return (
    <CustomizedTables
      title="Schemas"
      data={schemaDetails}
      showSearchBox={true}
      inAccordionFormat={true}
      addLinks
      baseURL="/tenants/schema/"
    />
  );
};
