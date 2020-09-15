/* eslint-disable no-nested-ternary */
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

import React, {  } from 'react';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript'
import { makeStyles } from '@material-ui/core';

type Props = {
  data: Object,
  isEditable?: Object,
  returnCodemirrorValue?: Function
};

const useStyles = makeStyles((theme) => ({
  codeMirror: {
    '& .CodeMirror': { height: 600, border: '1px solid #BDCCD9', fontSize: '13px' },
  }
}));

const CustomCodemirror = ({data, isEditable, returnCodemirrorValue}: Props) => {
  const classes = useStyles();

  const jsonoptions = {
    lineNumbers: true,
    mode: 'application/json',
    styleActiveLine: true,
    gutters: ['CodeMirror-lint-markers'],
    lint: true,
    theme: 'default',
    readOnly: !isEditable
  };

  return (
    <CodeMirror
      options={jsonoptions}
      value={JSON.stringify(data, null , 2)}
      className={classes.codeMirror}
      autoCursor={false}
      onChange={(editor, data, value) => {
        returnCodemirrorValue && returnCodemirrorValue(value);
      }}
    />
  );
};

export default CustomCodemirror;