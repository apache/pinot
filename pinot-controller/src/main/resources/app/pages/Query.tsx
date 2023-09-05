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
import { Grid, Checkbox, Button, FormControl, Input, InputLabel, Box } from '@material-ui/core';
import Alert from '@material-ui/lab/Alert';
import FileCopyIcon from '@material-ui/icons/FileCopy';
import { SqlException, TableData } from 'Models';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';
import 'codemirror/mode/sql/sql';
import 'codemirror/addon/hint/show-hint';
import 'codemirror/addon/hint/sql-hint';
import 'codemirror/addon/hint/show-hint.css';
import NativeCodeMirror from 'codemirror';
import { forEach, uniqBy, range as _range } from 'lodash';
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
import {Resizable} from "re-resizable";
import { useHistory, useLocation } from 'react-router';

const useStyles = makeStyles((theme) => ({
  title: {
    flexGrow: 1,
    paddingLeft: '20px',
  },
  rightPanel: {},
  codeMirror: {
    height: '100%',
    '& .CodeMirror': {
      height: '100%',
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
    height: '100%',
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: '20px',
    paddingBottom: '48px',
  },
  sqlError: {
    whiteSpace: 'pre-wrap',
    overflow: "auto"
  },
  timeoutControl: {
    bottom: 10
  }
}));

const jsonoptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
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

const sqlFuntionsList = [
  'COUNT', 'MIN', 'MAX', 'SUM', 'AVG', 'MINMAXRANGE', 'DISTINCTCOUNT', 'DISTINCTCOUNTBITMAP',
  'SEGMENTPARTITIONEDDISTINCTCOUNT', 'DISTINCTCOUNTHLL', 'DISTINCTCOUNTRAWHLL', 'FASTHLL',
  'DISTINCTCOUNTTHETASKETCH', 'DISTINCTCOUNTRAWTHETASKETCH', 'COUNTMV', 'MINMV', 'MAXMV',
  'SUMMV', 'AVGMV', 'MINMAXRANGEMV', 'DISTINCTCOUNTMV', 'DISTINCTCOUNTBITMAPMV', 'DISTINCTCOUNTHLLMV',
  'DISTINCTCOUNTRAWHLLMV', 'DISTINCT', 'ST_UNION'];

const responseStatCols = [
  'timeUsedMs',
  'numDocsScanned',
  'totalDocs',
  'numServersQueried',
  'numServersResponded',
  'numSegmentsQueried',
  'numSegmentsProcessed',
  'numSegmentsMatched',
  'numConsumingSegmentsQueried',
  'numEntriesScannedInFilter',
  'numEntriesScannedPostFilter',
  'numGroupsLimitReached',
  'partialResponse',
  'minConsumingFreshnessTimeMs',
  'offlineThreadCpuTimeNs',
  'realtimeThreadCpuTimeNs',
  'offlineSystemActivitiesCpuTimeNs',
  'realtimeSystemActivitiesCpuTimeNs',
  'offlineResponseSerializationCpuTimeNs',
  'realtimeResponseSerializationCpuTimeNs',
  'offlineTotalCpuTimeNs',
  'realtimeTotalCpuTimeNs'
];

// A custom hook that builds on useLocation to parse the query string
function useQuery() {
  const { search } = useLocation();

  return React.useMemo(() => new URLSearchParams(search), [search]);
}

const QueryPage = () => {
  const classes = useStyles();
  const history = useHistory();
  let queryParam = useQuery();
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

  const [inputQuery, setInputQuery] = useState(queryParam.get('query') || '');

  const [queryTimeout, setQueryTimeout] = useState(Number(queryParam.get('timeout') || '') || '');

  const [outputResult, setOutputResult] = useState('');

  const [resultError, setResultError] = useState<SqlException[] | string>([]);

  const [queryStats, setQueryStats] = useState<TableData>({
    columns: [],
    records: [],
  });

  const [warnings, setWarnings] = useState<Array<string>>([]);

  const [checked, setChecked] = React.useState({
    tracing: queryParam.get('tracing') === 'true',
    useMSE: queryParam.get('useMSE') === 'true',
    showResultJSON: false,
  });

  const queryExecuted = React.useRef(false);
  const [boolFlag, setBoolFlag] = useState(false);

  const [copyMsg, showCopyMsg] = React.useState(false);

  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setChecked({ ...checked, [event.target.name]: event.target.checked });
  };

  const handleOutputDataChange = (editor, data, value) => {
    setInputQuery(value);
  };

  const handleQueryInterfaceKeyDown = (editor, event) => {
    // Map Cmd + Enter KeyPress to executing the query
    if (event.metaKey == true && event.keyCode == 13) {
      handleRunNow(editor.getValue());
    }
    // Map Cmd + / KeyPress to toggle commenting the query
    if (event.metaKey == true && event.keyCode == 191) {
      handleComment(editor);
    }
  }

  const handleComment = (cm: NativeCodeMirror.Editor) => {
    const selections = cm.listSelections();
    if (!selections) {
      return;
    }
    const query = cm.getValue();
    const querySplit = query.split(/\r?\n/);
    forEach(selections, (range) => {
      // anchor and head are based on where the selection starts/ends, but for the purpose
      // of determining the line number range of the selection, we need start/end in order.
      const start = Math.min(range.anchor.line, range.head.line);
      let end = Math.max(range.anchor.line, range.head.line);

      const isSingleLineSelection = start === end;
      const isLastLineFirstChar = (range.anchor.line === end && range.anchor.ch === 0) ||
          (range.head.line === end && range.head.ch === 0);
      // If the selection is on the last line and the first character, we do not comment that line.
      // This happens if you are using shift + down to select lines.
      if (isLastLineFirstChar && !isSingleLineSelection) {
        end = end - 1;
      }
      const isEntireSelectionCommented = _range(start, end + 1).every((line) => {
        return querySplit[line].startsWith("--") || querySplit[line].trim().length === 0;
      });

      for (let line = start; line <= end; line++) {
        const lineIsCommented = querySplit[line].startsWith("--");
        const lineIsEmpty = querySplit[line].trim().length === 0;
        if (isEntireSelectionCommented) {
          // If the entire range is commented, then we uncomment all the lines
          if (lineIsCommented) {
            querySplit[line] = querySplit[line].replace(/^--\s*/, '');
          }
        }
        else {
          // If the range is not commented, then we comment all the uncommented lines
          if (!lineIsEmpty && !lineIsCommented) {
            querySplit[line] = `-- ${querySplit[line]}`;
          }
        }
      }
    });
    setInputQuery(querySplit.join("\n"));
  }

  const handleRunNow = async (query?: string) => {
    setQueryLoader(true);
    queryExecuted.current = true;
    let params;
    let queryOptions = [];
    if(queryTimeout){
      queryOptions.push(`timeoutMs=${queryTimeout}`);
    }
    if(checked.useMSE){
      queryOptions.push(`useMultistageEngine=true`);
    }
    const finalQuery = `${query || inputQuery.trim()}`;
    params = JSON.stringify({
      sql: `${finalQuery}`,
      trace: checked.tracing,
      queryOptions: `${queryOptions.join(";")}`,
    });

    if(finalQuery !== ''){
      queryParam.set('query', finalQuery);
      queryParam.set('tracing', checked.tracing.toString());
      queryParam.set('useMSE', checked.useMSE.toString());
      if(queryTimeout !== undefined && queryTimeout !== ''){
        queryParam.set('timeout', queryTimeout.toString());
      }
      history.push({
        pathname: '/query',
        search: `?${queryParam.toString()}`
      })
    }

    const results = await PinotMethodUtils.getQueryResults(params);
    setResultError(results.exceptions || []);
    setResultData(results.result || { columns: [], records: [] });
    setQueryStats(results.queryStats || { columns: responseStatCols, records: [] });
    setOutputResult(JSON.stringify(results.data, null, 2) || '');
    setWarnings(extractWarnings(results));
    setQueryLoader(false);
    queryExecuted.current = false;
  };

  const extractWarnings = (result) => {
    const warnings: Array<string> = [];
    const numSegmentsPrunedInvalid = result.data.numSegmentsPrunedInvalid;
    if (numSegmentsPrunedInvalid) {
      warnings.push(`There are ${numSegmentsPrunedInvalid} invalid segment/s. This usually means that they were `
         + `created with an older schema. `
         + `Please reload the table in order to refresh these segments to the new schema.`);
    }
    return warnings;
  }

  const fetchSQLData = async (tableName) => {
    setQueryLoader(true);
    const result = await PinotMethodUtils.getTableSchemaData(tableName);
    const tableSchema = Utils.syncTableSchemaData(result, false);
    setTableSchema(tableSchema);

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
    const result = await PinotMethodUtils.getQueryTablesList({bothType: false});
    setTableList(result);
    setFetching(false);
  };

  useEffect(() => {
    fetchData();
    if(inputQuery){
      handleRunNow(inputQuery);
    }
  }, []);

  useEffect(()=>{
    const query = queryParam.get('query');
    if(!queryExecuted.current && query){
      setInputQuery(query);
      setChecked({
        tracing: queryParam.get('tracing') === 'true',
        useMSE: queryParam.get('useMse') === 'true',
        showResultJSON: checked.showResultJSON,
      });
      setQueryTimeout(Number(queryParam.get('timeout') || '') || '');
      setBoolFlag(!boolFlag);
    }
  }, [queryParam]);

  useEffect(()=>{
    const query = queryParam.get('query');
    if(!queryExecuted.current && query){
      handleRunNow();
    }
  }, [boolFlag]);

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
    Array.prototype.push.apply(hintOptions, Utils.generateCodeMirrorOptions(sqlFuntionsList, 'FUNCTION'));

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

    defaultHint.list = uniqBy(defaultHint.list, 'text');
    return defaultHint;
  };

  const sqlEditorTooltip = "This editor supports auto-completion feature. Type @ in the editor to see the list of SQL keywords, functions, table name and column names."

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
          queryLoader={queryLoader}
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
            <Resizable
                defaultSize={{
                  width: '100%',
                  height: 148,
                }}
                minHeight={148}
                maxWidth={'100%'}
                maxHeight={'50vh'}
                enable={{bottom: true}}>
              <div className={classes.sqlDiv}>
                <TableToolbar name="SQL Editor" showSearchBox={false} showTooltip={true} tooltipText={sqlEditorTooltip} />
                <CodeMirror
                  options={{
                    ...sqloptions,
                    hintOptions: {
                      hint: handleSqlHints,
                    },
                  }}
                  value={inputQuery}
                  onChange={handleOutputDataChange}
                  onKeyDown={handleQueryInterfaceKeyDown}
                  className={classes.codeMirror}
                  autoCursor={false}
                />
              </div>
            </Resizable>

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

              <Grid item xs={3}>
                <Checkbox
                    name="useMSE"
                    color="primary"
                    onChange={handleChange}
                    checked={checked.useMSE}
                />
                Use Multi-Stage Engine
              </Grid>

              <Grid item xs={3}>
                <FormControl fullWidth={true} className={classes.timeoutControl}>
                  <InputLabel htmlFor="my-input">Timeout (in Milliseconds)</InputLabel>
                  <Input id="my-input" type="number" value={queryTimeout} onChange={(e)=> setQueryTimeout(Number(e.target.value) || '')}/>
                </FormControl>
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
                {queryStats.columns.length ? (
                    <Grid item xs style={{ backgroundColor: 'white' }}>
                      <CustomizedTables
                          title="Query Response Stats"
                          data={queryStats}
                          showSearchBox={true}
                          inAccordionFormat={true}
                      />
                    </Grid>
                ) : null}

                {
                  warnings.map(warn =>
                                   <Alert severity="warning" className={classes.sqlError}>
                                     {warn}
                                   </Alert>
                  )
                }
        
                {/* Sql result errors */}
                {resultError && typeof resultError === "string" && (
                  <Alert severity="error" className={classes.sqlError}>
                    {resultError}
                  </Alert>
                )}

                {resultError && typeof resultError === "object" && resultError.length && (
                  <>
                    {
                      resultError.map((error) => (
                        <Box style={{paddingBottom: "16px"}}>
                          <Alert className={classes.sqlError} severity="error">{error.message}</Alert>
                        </Box>
                      ))
                    }
                  </>
                  )
                }
        
                <Grid item xs style={{ backgroundColor: 'white' }}>
                  {resultData.columns.length ? (
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
                          isSticky={true}
                          showSearchBox={true}
                          inAccordionFormat={true}
                        />
                      ) : resultData.columns.length ? (
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
          </Grid>
        </Grid>
      </Grid>
    </>
  );
};

export default QueryPage;
