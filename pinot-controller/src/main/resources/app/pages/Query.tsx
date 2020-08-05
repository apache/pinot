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

import React, { useEffect, useState } from 'react';
import { makeStyles } from '@material-ui/core/styles';
import { Grid, Checkbox, Button } from '@material-ui/core';
import Alert from '@material-ui/lab/Alert';
import FileCopyIcon from '@material-ui/icons/FileCopy';
import { TableData } from 'Models';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';
import 'codemirror/mode/sql/sql';
import 'codemirror/addon/hint/show-hint';
import 'codemirror/addon/hint/sql-hint';
import 'codemirror/addon/hint/show-hint.css';
import NativeCodeMirror from 'codemirror';
import _ from 'lodash';
import FormControlLabel from '@material-ui/core/FormControlLabel';
import Switch from '@material-ui/core/Switch';
import exportFromJSON from 'export-from-json';
import Utils from '../utils/Utils';
import AppLoader from '../components/AppLoader';
import CustomizedTables from '../components/Table';
import QuerySideBar from '../components/Query/QuerySideBar';
import TableToolbar from '../components/TableToolbar';
import SimpleAccordion from '../components/SimpleAccordion';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import '../styles/styles.css';

const useStyles = makeStyles((theme) => ({
  title: {
    flexGrow: 1,
    paddingLeft: '20px',
  },
  rightPanel: {},
  codeMirror: {
    '& .CodeMirror': {
      height: 100,
      border: '1px solid #BDCCD9',
      fontSize: '13px',
    },
  },
  queryOutput: {
    '& .CodeMirror': { height: 430, border: '1px solid #BDCCD9' },
  },
  btn: {
    margin: '10px 10px 0 0',
    height: 30,
  },
  checkBox: {
    margin: '20px 0',
  },
  actionBtns: {
    margin: '20px 0',
    height: 50,
  },
  runNowBtn: {
    marginLeft: 'auto',
    paddingLeft: '74px',
  },
  sqlDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: '20px',
  },
  sqlError: {
    whiteSpace: 'pre-wrap',
  },
}));

const jsonoptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
  lint: true,
  theme: 'default',
  readOnly: true,
};

const sqloptions = {
  lineNumbers: true,
  mode: 'text/x-sql',
  styleActiveLine: true,
  lint: true,
  theme: 'default',
  indentWithTabs: true,
  smartIndent: true,
  lineWrapping: true,
  extraKeys: { "'@'": 'autocomplete' },
};

const QueryPage = () => {
  const classes = useStyles();
  const [fetching, setFetching] = useState(true);
  const [queryLoader, setQueryLoader] = useState(false);
  const [tableList, setTableList] = useState<TableData>({
    columns: [],
    records: [],
  });

  const [tableSchema, setTableSchema] = useState<TableData>({
    columns: [],
    records: [],
  });
  const [resultData, setResultData] = useState<TableData>({
    columns: [],
    records: [],
  });

  const [selectedTable, setSelectedTable] = useState('');

  const [inputQuery, setInputQuery] = useState('');

  const [outputResult, setOutputResult] = useState('');

  const [resultError, setResultError] = useState('');

  const [queryStats, setQueryStats] = useState<TableData>({
    columns: [],
    records: [],
  });

  const [checked, setChecked] = React.useState({
    tracing: false,
    querySyntaxPQL: false,
    showResultJSON: false,
  });

  const [copyMsg, showCopyMsg] = React.useState(false);

  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setChecked({ ...checked, [event.target.name]: event.target.checked });
  };

  const handleOutputDataChange = (editor, data, value) => {
    setInputQuery(value);
  };

  const handleRunNow = async (query?: string) => {
    setQueryLoader(true);
    let url;
    let params;
    if (checked.querySyntaxPQL) {
      url = 'pql';
      params = JSON.stringify({
        pql: query || inputQuery.trim(),
        trace: checked.tracing,
      });
    } else {
      url = 'sql';
      params = JSON.stringify({
        sql: query || inputQuery.trim(),
        trace: checked.tracing,
      });
    }

    const results = await PinotMethodUtils.getQueryResults(
      params,
      url,
      checked
    );
    setResultError(results.error || '');
    setResultData(results.result || { columns: [], records: [] });
    setQueryStats(results.queryStats || { columns: [], records: [] });
    setOutputResult(JSON.stringify(results.data, null, 2) || '');
    setQueryLoader(false);
  };

  const fetchSQLData = async (tableName) => {
    const result = await PinotMethodUtils.getTableSchemaData(tableName, false);
    setTableSchema(result);

    const query = `select * from ${tableName} limit 10`;
    setInputQuery(query);
    setSelectedTable(tableName);
    handleRunNow(query);
  };

  const downloadData = (exportType) => {
    const data = Utils.tableFormat(resultData);
    const fileName = 'Pinot Data Explorer';

    exportFromJSON({ data, fileName, exportType });
  };

  const copyToClipboard = () => {
    // Create an auxiliary hidden input
    const aux = document.createElement('input');

    // Get the text from the element passed into the input
    aux.setAttribute('value', JSON.stringify(resultData));

    // Append the aux input to the body
    document.body.appendChild(aux);

    // Highlight the content
    aux.select();

    // Execute the copy command
    document.execCommand('copy');

    // Remove the input from the body
    document.body.removeChild(aux);

    showCopyMsg(true);

    setTimeout(() => {
      showCopyMsg(false);
    }, 3000);
  };

  const fetchData = async () => {
    const result = await PinotMethodUtils.getQueryTablesList();
    setTableList(result);
    setFetching(false);
  };

  useEffect(() => {
    fetchData();
  }, []);

  const handleSqlHints = (cm: NativeCodeMirror.Editor) => {
    const tableNames = [];
    tableList.records.forEach((obj, i) => {
      tableNames.push(obj[i]);
    });
    const columnNames = tableSchema.records.map((obj) => {
      return obj[0];
    });
    const hintOptions = [];
    const defaultHint = (NativeCodeMirror as any).hint.sql(cm);

    Array.prototype.push.apply(hintOptions, Utils.generateCodeMirrorOptions(tableNames, 'TABLE'));
    Array.prototype.push.apply(hintOptions, Utils.generateCodeMirrorOptions(columnNames, 'COLUMNS'));

    const cur = cm.getCursor();
    const curLine = cm.getLine(cur.line);
    let start = cur.ch;
    let end = start;
    // eslint-disable-next-line no-plusplus
    while (end < curLine.length && /[\w$]/.test(curLine.charAt(end))) ++end;
    // eslint-disable-next-line no-plusplus
    while (start && /[\w$]/.test(curLine.charAt(start - 1))) --start;
    const curWord = start !== end && curLine.slice(start, end);
    const regex = new RegExp(`^${  curWord}`, 'i');

    const finalList =  (!curWord ? hintOptions : hintOptions.filter(function (item) {
      return item.displayText.match(regex);
    })).sort();

    Array.prototype.push.apply(defaultHint.list, finalList);

    return defaultHint;
  };

  return fetching ? (
    <AppLoader />
  ) : (
    <>
      <Grid item>
        <QuerySideBar
          tableList={tableList}
          fetchSQLData={fetchSQLData}
          tableSchema={tableSchema}
          selectedTable={selectedTable}
        />
      </Grid>
      <Grid
        item
        xs
        style={{
          padding: 20,
          backgroundColor: 'white',
          maxHeight: 'calc(100vh - 70px)',
          overflowY: 'auto',
        }}
      >
        <Grid container>
          <Grid item xs={12} className={classes.rightPanel}>
            <div className={classes.sqlDiv}>
              <TableToolbar name="SQL Editor" showSearchBox={false} />
              <CodeMirror
                options={{
                  ...sqloptions,
                  hintOptions: {
                    hint: handleSqlHints,
                  },
                }}
                value={inputQuery}
                onChange={handleOutputDataChange}
                className={classes.codeMirror}
                autoCursor={false}
              />
            </div>

            <Grid container className={classes.checkBox}>
              <Grid item xs={2}>
                <Checkbox
                  name="tracing"
                  color="primary"
                  onChange={handleChange}
                  checked={checked.tracing}
                />
                Tracing
              </Grid>

              <Grid item xs={2}>
                <Checkbox
                  name="querySyntaxPQL"
                  color="primary"
                  onChange={handleChange}
                  checked={checked.querySyntaxPQL}
                />
                Query Syntax: PQL
              </Grid>

              <Grid item xs={3} className={classes.runNowBtn}>
                <Button
                  variant="contained"
                  color="primary"
                  onClick={() => handleRunNow()}
                >
                  Run Query
                </Button>
              </Grid>
            </Grid>

            {queryLoader ? (
              <AppLoader />
            ) : (
              <>
                {resultError ? (
                  <Alert severity="error" className={classes.sqlError}>
                    {resultError}
                  </Alert>
                ) : (
                  <>
                    {queryStats.records.length ? (
                      <Grid item xs style={{ backgroundColor: 'white' }}>
                        <CustomizedTables
                          title="Query Response Stats"
                          data={queryStats}
                          showSearchBox={true}
                          inAccordionFormat={true}
                        />
                      </Grid>
                    ) : null}

                    <Grid item xs style={{ backgroundColor: 'white' }}>
                      {resultData.records.length ? (
                        <>
                          <Grid container className={classes.actionBtns}>
                            <Button
                              variant="contained"
                              color="primary"
                              size="small"
                              className={classes.btn}
                              onClick={() => downloadData('xls')}
                            >
                              Excel
                            </Button>
                            <Button
                              variant="contained"
                              color="primary"
                              size="small"
                              className={classes.btn}
                              onClick={() => downloadData('csv')}
                            >
                              CSV
                            </Button>
                            <Button
                              variant="contained"
                              color="primary"
                              size="small"
                              className={classes.btn}
                              onClick={() => copyToClipboard()}
                            >
                              Copy
                            </Button>
                            {copyMsg ? (
                              <Alert
                                icon={<FileCopyIcon fontSize="inherit" />}
                                severity="info"
                              >
                                Copied {resultData.records.length} rows to
                                Clipboard
                              </Alert>
                            ) : null}

                            <FormControlLabel
                              control={
                                <Switch
                                  checked={checked.showResultJSON}
                                  onChange={handleChange}
                                  name="showResultJSON"
                                  color="primary"
                                />
                              }
                              label="Show JSON format"
                              className={classes.runNowBtn}
                            />
                          </Grid>
                          {!checked.showResultJSON ? (
                            <CustomizedTables
                              title="Query Result"
                              data={resultData}
                              isPagination
                              isSticky={true}
                              showSearchBox={true}
                              inAccordionFormat={true}
                            />
                          ) : resultData.records.length ? (
                            <SimpleAccordion
                              headerTitle="Query Result (JSON Format)"
                              showSearchBox={false}
                            >
                              <CodeMirror
                                options={jsonoptions}
                                value={outputResult}
                                className={classes.queryOutput}
                                autoCursor={false}
                              />
                            </SimpleAccordion>
                          ) : null}
                        </>
                      ) : null}
                    </Grid>
                  </>
                )}
              </>
            )}
          </Grid>
        </Grid>
      </Grid>
    </>
  );
};

export default QueryPage;
