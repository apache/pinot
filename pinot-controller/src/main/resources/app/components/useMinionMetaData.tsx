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

import React, { useEffect, useState } from 'react';
import { makeStyles } from '@material-ui/core';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import SimpleAccordion from './SimpleAccordion';
import PinotMethodUtils from '../utils/PinotMethodUtils';

const jsonoptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
  theme: 'default',
  readOnly: true
};

const useStyles = makeStyles(() => ({
  queryOutput: {
    border: '1px solid #BDCCD9',
    '& .CodeMirror': { height: 532 },
  },
}));


export default function useMinionMetadata(props) {
  const classes = useStyles();
  const { taskType, tableName } = props;
  const [metadata, setMetadata] = useState({});

  const fetchData = async () => {
    const metadataRes = await PinotMethodUtils.getMinionMetaData(tableName, taskType);
    setMetadata(metadataRes);
  };

  useEffect(() => {
    fetchData();
  }, []);

  return {
    metadata,
    setMetadata,
    content: (
      <SimpleAccordion
        headerTitle="Minion Metadata"
        showSearchBox={false}
      >
        <CodeMirror
          options={jsonoptions}
          value={JSON.stringify(metadata || {}, null, '  ')}
          className={classes.queryOutput}
          autoCursor={false}
        />
      </SimpleAccordion>
    )
  };
}