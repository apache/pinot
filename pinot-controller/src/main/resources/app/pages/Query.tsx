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
import { TableData, SQLResult } from 'Models';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';
import 'codemirror/mode/sql/sql';
import _ from 'lodash';
import exportFromJSON from 'export-from-json';
import Utils from '../utils/Utils';
import { getQueryTables, getTableSchema, getQueryResult } from '../requests';
import AppLoader from '../components/AppLoader';
import CustomizedTables from '../components/Table';
import QuerySideBar from '../components/Query/QuerySideBar';
import EnhancedTableToolbar from '../components/EnhancedTableToolbar';

const useStyles = makeStyles((theme) => ({
  title: {
    flexGrow: 1,
    paddingLeft: '20px',
  },
  rightPanel: {
    padding: '20px',
  },
  codeMirror: {
    '& .CodeMirror': { height: 100, border: '1px solid #BDCCD9' },
  },
  queryOutput: {
    border: '1px solid #BDCCD9',
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
  }
}));

const jsonoptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
  lint: true,
  theme: 'default'
};

const sqloptions = {
  lineNumbers: true,
  mode: 'sql',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
  lint: true,
  theme: 'default'
};

const QueryPage = () => {
  const classes = useStyles();
  const [fetching, setFetching] = useState(true);
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

  const [checked, setChecked] = React.useState({
    tracing: false,
    querySyntaxPQL: false,
  });

  const [copyMsg, showCopyMsg] = React.useState(false);

  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setChecked({ ...checked, [event.target.name]: event.target.checked });
  };

  const handleOutputDataChange = (editor, data, value) => {
    setInputQuery(value);
  };

  const getAsObject = (str: SQLResult) => {
    if (typeof str === 'string' || str instanceof String) {
      return JSON.parse(JSON.stringify(str));
    }
    return str;
  };

  const handleRunNow = (query?: string) => {
    setFetching(true);
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

    getQueryResult(params, url).then(({ data }) => {
      let queryResponse = null;

      queryResponse = getAsObject(data);

      let dataArray = [];
      let columnList = [];
      if (checked.querySyntaxPQL === true) {
        if (queryResponse) {
          if (queryResponse.selectionResults) {
            // Selection query
            columnList = queryResponse.selectionResults.columns;
            dataArray = queryResponse.selectionResults.results;
          } else if (!queryResponse.aggregationResults[0]?.groupByResult) {
            // Simple aggregation query
            columnList = _.map(
              queryResponse.aggregationResults,
              (aggregationResult) => {
                return { title: aggregationResult.function };
              }
            );

            dataArray.push(
              _.map(queryResponse.aggregationResults, (aggregationResult) => {
                return aggregationResult.value;
              })
            );
          } else if (queryResponse.aggregationResults[0]?.groupByResult) {
            // Aggregation group by query
            // TODO - Revisit
            const columns = queryResponse.aggregationResults[0].groupByColumns;
            columns.push(queryResponse.aggregationResults[0].function);
            columnList = _.map(columns, (columnName) => {
              return columnName;
            });

            dataArray = _.map(
              queryResponse.aggregationResults[0].groupByResult,
              (aggregationGroup) => {
                const row = aggregationGroup.group;
                row.push(aggregationGroup.value);
                return row;
              }
            );
          }
        }
      } else if (queryResponse.resultTable?.dataSchema?.columnNames?.length) {
        columnList = queryResponse.resultTable.dataSchema.columnNames;
        dataArray = queryResponse.resultTable.rows;
      }

      setResultData({
        columns: columnList,
        records: dataArray,
      });
      setFetching(false);

      setOutputResult(JSON.stringify(data, null, 2));
    });
  };

  const fetchSQLData = (tableName) => {
    getTableSchema(tableName).then(({ data }) => {
      setTableSchema({
        columns: ['column', 'type'],
        records: data.dimensionFieldSpecs.map((field) => {
          return [field.name, field.dataType];
        }),
      });
    });

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

  useEffect(() => {
    getQueryTables().then(({ data }) => {
      setTableList({
        columns: ['Tables'],
        records: data.tables.map((table) => {
          return [table];
        }),
      });
      setFetching(false);
    });
  }, []);

  return fetching ? (
    <AppLoader />
  ) : (
    <>
      <Grid item>
        <QuerySideBar
          tableList={tableList}
          fetchSQLData={fetchSQLData}
          tableSchema={tableSchema}
        />
      </Grid>
      <Grid item xs style={{ padding: 20, backgroundColor: 'white', maxHeight: 'calc(100vh - 70px)', overflowY: 'auto' }}>
        <Grid container>
          <Grid item xs={12} className={classes.rightPanel}>
            <div className={classes.sqlDiv}>
              <EnhancedTableToolbar name="SQL Editor" showSearchBox={false} />
              <CodeMirror
                options={sqloptions}
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
                        Copied {resultData.records.length} rows to Clipboard
                      </Alert>
                    ) : null}
                  </Grid>
                  <CustomizedTables
                    title={selectedTable}
                    data={resultData}
                    isPagination
                    isSticky={true}
                  />
                </>
              ) : null}
            </Grid>

            {resultData.records.length ? (
              <CodeMirror
                options={jsonoptions}
                value={outputResult}
                className={classes.queryOutput}
                autoCursor={false}
              />
            ) : null}
          </Grid>
        </Grid>
      </Grid>
    </>
  );
};

export default QueryPage;
