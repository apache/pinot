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

import React, { useEffect, useMemo, useState } from 'react';
import { makeStyles } from '@material-ui/core/styles';
import {
  Box,
  Button,
  Checkbox,
  FormControlLabel,
  Grid,
  MenuItem,
  Paper,
  TextField,
  Typography
} from '@material-ui/core';
import Alert from '@material-ui/lab/Alert';
import RefreshIcon from '@material-ui/icons/Refresh';
import { SqlException, TableData } from 'Models';
import CustomizedTables from '../components/Table';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import Utils from '../utils/Utils';

const DEFAULT_COLUMNS = [
  'timestampMs',
  'requestId',
  'tableName',
  'brokerId',
  'clientIp',
  'queryEngine',
  'timeMs',
  'numDocsScanned',
  'numEntriesScannedInFilter',
  'numEntriesScannedPostFilter',
  'partialResult',
  'query'
];

const DEFAULT_SELECT_LIST = DEFAULT_COLUMNS.join(', ');
const DEFAULT_LIMIT = 200;
const DEFAULT_LOOKBACK_MINUTES = 60;

type QueryLogFilters = {
  tableName: string;
  searchText: string;
  minLatency: string;
  maxLatency: string;
  requestId: string;
  clientIp: string;
  limit: string;
  lookbackMinutes: string;
  queryEngine: string;
  partialOnly: boolean;
};

const defaultFilters: QueryLogFilters = {
  tableName: '',
  searchText: '',
  minLatency: '',
  maxLatency: '',
  requestId: '',
  clientIp: '',
  limit: DEFAULT_LIMIT.toString(),
  lookbackMinutes: DEFAULT_LOOKBACK_MINUTES.toString(),
  queryEngine: '',
  partialOnly: false
};

const useStyles = makeStyles((theme) => ({
  root: {
    maxWidth: 1600
  },
  formPaper: {
    padding: theme.spacing(3),
    marginBottom: theme.spacing(3)
  },
  sqlPaper: {
    padding: theme.spacing(2),
    marginBottom: theme.spacing(3),
    backgroundColor: '#f5f7f9'
  },
  sqlPreview: {
    fontFamily: 'monospace',
    whiteSpace: 'pre-wrap',
    wordBreak: 'break-word',
    marginTop: theme.spacing(1)
  },
  actions: {
    marginTop: theme.spacing(2),
    display: 'flex',
    justifyContent: 'flex-end',
    gap: theme.spacing(2)
  },
  lastRefreshed: {
    marginBottom: theme.spacing(1)
  }
}));

const escapeLiteral = (value: string): string => value.replace(/'/g, "''");

const QueryLogsPage = () => {
  const classes = useStyles();
  const [tableOptions, setTableOptions] = useState<string[]>([]);
  const [filters, setFilters] = useState<QueryLogFilters>({ ...defaultFilters });
  const [resultTable, setResultTable] = useState<TableData>({
    columns: DEFAULT_COLUMNS,
    records: [],
    isLoading: false
  });
  const [exceptions, setExceptions] = useState<SqlException[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<number | null>(null);

  useEffect(() => {
    let mounted = true;
    PinotMethodUtils.getQueryTablesList({ bothType: false })
      .then((data) => {
        if (!mounted) {
          return;
        }
        const names = data.records.map((record) => String(record[0])).sort();
        setTableOptions(names);
      })
      .catch(() => setTableOptions([]));

    runQueryLogs(defaultFilters);

    return () => {
      mounted = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleInputChange = (field: keyof QueryLogFilters) => (event) => {
    const value = event.target.value;
    setFilters((prev) => ({
      ...prev,
      [field]: value
    }));
  };

  const handleCheckboxChange = (field: keyof QueryLogFilters) => (event) => {
    const { checked } = event.target;
    setFilters((prev) => ({
      ...prev,
      [field]: checked
    }));
  };

  const parseLimit = (limitValue: string) => {
    const parsed = parseInt(limitValue, 10);
    if (Number.isNaN(parsed) || parsed <= 0) {
      return DEFAULT_LIMIT;
    }
    return Math.min(parsed, 1000);
  };

  const parseLookback = (value: string) => {
    const parsed = parseInt(value, 10);
    if (Number.isNaN(parsed) || parsed <= 0) {
      return 0;
    }
    return parsed;
  };

  const buildSql = (activeFilters: QueryLogFilters) => {
    const clauses: string[] = [];

    if (activeFilters.tableName) {
      clauses.push(`tableName = '${escapeLiteral(activeFilters.tableName)}'`);
    }
    if (activeFilters.queryEngine) {
      clauses.push(`queryEngine = '${escapeLiteral(activeFilters.queryEngine)}'`);
    }
    if (activeFilters.clientIp.trim()) {
      clauses.push(`clientIp = '${escapeLiteral(activeFilters.clientIp.trim())}'`);
    }
    if (activeFilters.partialOnly) {
      clauses.push('partialResult = true');
    }

    if (activeFilters.requestId.trim()) {
      const numericRequestId = Number(activeFilters.requestId.trim());
      if (!Number.isNaN(numericRequestId)) {
        clauses.push(`requestId = ${numericRequestId}`);
      }
    }

    if (activeFilters.minLatency.trim()) {
      const minLatency = Number(activeFilters.minLatency.trim());
      if (!Number.isNaN(minLatency)) {
        clauses.push(`timeMs >= ${minLatency}`);
      }
    }
    if (activeFilters.maxLatency.trim()) {
      const maxLatency = Number(activeFilters.maxLatency.trim());
      if (!Number.isNaN(maxLatency)) {
        clauses.push(`timeMs <= ${maxLatency}`);
      }
    }

    if (activeFilters.searchText.trim()) {
      const searchValue = escapeLiteral(activeFilters.searchText.trim());
      clauses.push(`query LIKE '%${searchValue}%'`);
    }

    const lookbackMinutes = parseLookback(activeFilters.lookbackMinutes);
    if (lookbackMinutes > 0) {
      const cutoff = Date.now() - lookbackMinutes * 60 * 1000;
      clauses.push(`timestampMs >= ${Math.max(Math.floor(cutoff), 0)}`);
    }

    const whereClause = clauses.length ? ` WHERE ${clauses.join(' AND ')}` : '';
    const limitClause = ` LIMIT ${parseLimit(activeFilters.limit)}`;

    return `SELECT ${DEFAULT_SELECT_LIST} FROM system.query_log${whereClause} ORDER BY timestampMs DESC${limitClause}`;
  };

  const sqlPreview = useMemo(() => buildSql(filters), [filters]);

  const formatRecords = (records: Array<Array<string | number | boolean>>) => {
    return records.map((row) => {
      const clone = [...row];
      if (clone.length) {
        const timestamp = Number(clone[0]);
        if (!Number.isNaN(timestamp)) {
          clone[0] = Utils.formatTime(timestamp, 'YYYY-MM-DD HH:mm:ss.SSS');
        }
      }
      return clone;
    });
  };

  const runQueryLogs = async (overrideFilters?: QueryLogFilters) => {
    const activeFilters = overrideFilters || filters;
    const sql = buildSql(activeFilters);
    setResultTable((prev) => ({
      ...prev,
      isLoading: true
    }));
    setError(null);
    try {
      const payload = { sql, trace: 'false' };
      const response = await PinotMethodUtils.getQueryLogResults(payload);
      const columns =
        response.result?.columns && response.result.columns.length
          ? response.result.columns
          : DEFAULT_COLUMNS;
      const records = response.result?.records ? formatRecords(response.result.records) : [];
      setResultTable({
        columns,
        records,
        isLoading: false
      });
      setExceptions(response.exceptions || []);
      setLastUpdated(Date.now());
    } catch (err) {
      const message =
        err?.response?.data?.error ||
        err?.message ||
        'Failed to fetch query logs. Please check controller logs for details.';
      setError(message);
      setExceptions([]);
      setResultTable((prev) => ({
        ...prev,
        isLoading: false
      }));
    }
  };

  const handleSubmit = (event) => {
    event.preventDefault();
    runQueryLogs();
  };

  const handleReset = () => {
    setFilters({ ...defaultFilters });
    runQueryLogs(defaultFilters);
  };

  return (
    <div className={classes.root}>
      <Typography variant="h4" gutterBottom>
        Query Logs
      </Typography>
      <Typography variant="body1" paragraph>
        Inspect queries recently served by all brokers. Use the filters below to locate a specific request,
        investigate slow queries, or focus on a particular table or client.
      </Typography>

      <Paper className={classes.formPaper}>
        <form onSubmit={handleSubmit}>
          <Grid container spacing={2}>
            <Grid item xs={12} sm={6} md={4}>
              <TextField
                label="Table name"
                value={filters.tableName}
                onChange={handleInputChange('tableName')}
                select
                fullWidth
                helperText="Filter by table"
              >
                <MenuItem value="">
                  <em>All tables</em>
                </MenuItem>
                {tableOptions.map((table) => (
                  <MenuItem key={table} value={table}>
                    {table}
                  </MenuItem>
                ))}
              </TextField>
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <TextField
                label="Query contains"
                value={filters.searchText}
                onChange={handleInputChange('searchText')}
                fullWidth
                placeholder="WHERE clause, column, etc."
              />
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <TextField
                label="Request ID"
                value={filters.requestId}
                onChange={handleInputChange('requestId')}
                fullWidth
                placeholder="Exact requestId"
              />
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <TextField
                label="Client IP"
                value={filters.clientIp}
                onChange={handleInputChange('clientIp')}
                fullWidth
              />
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <TextField
                label="Min latency (ms)"
                value={filters.minLatency}
                onChange={handleInputChange('minLatency')}
                fullWidth
                type="number"
                inputProps={{ min: 0 }}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <TextField
                label="Max latency (ms)"
                value={filters.maxLatency}
                onChange={handleInputChange('maxLatency')}
                fullWidth
                type="number"
                inputProps={{ min: 0 }}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <TextField
                label="Query engine"
                value={filters.queryEngine}
                onChange={handleInputChange('queryEngine')}
                select
                fullWidth
              >
                <MenuItem value="">
                  <em>All</em>
                </MenuItem>
                <MenuItem value="singleStage">singleStage</MenuItem>
                <MenuItem value="multiStage">multiStage</MenuItem>
              </TextField>
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <TextField
                label="Lookback (minutes)"
                value={filters.lookbackMinutes}
                onChange={handleInputChange('lookbackMinutes')}
                fullWidth
                type="number"
                helperText="0 to disable time filter"
                inputProps={{ min: 0 }}
              />
            </Grid>
            <Grid item xs={12} sm={6} md={4}>
              <TextField
                label="Result limit"
                value={filters.limit}
                onChange={handleInputChange('limit')}
                fullWidth
                type="number"
                helperText="Max 1000 rows"
                inputProps={{ min: 1, max: 1000 }}
              />
            </Grid>
            <Grid item xs={12}>
              <FormControlLabel
                control={
                  <Checkbox
                    color="primary"
                    checked={filters.partialOnly}
                    onChange={handleCheckboxChange('partialOnly')}
                  />
                }
                label="Show only partial responses"
              />
            </Grid>
          </Grid>
          <div className={classes.actions}>
            <Button onClick={handleReset}>Reset</Button>
            <Button type="submit" color="primary" variant="contained" startIcon={<RefreshIcon />}>
              Run
            </Button>
          </div>
        </form>
      </Paper>

      <Paper className={classes.sqlPaper}>
        <Typography variant="subtitle2">Generated SQL</Typography>
        <Box className={classes.sqlPreview}>{sqlPreview}</Box>
      </Paper>

      {lastUpdated && (
        <Typography variant="body2" className={classes.lastRefreshed}>
          Last refreshed at {Utils.formatTime(lastUpdated, 'YYYY-MM-DD HH:mm:ss z')}
        </Typography>
      )}

      {error && (
        <Box marginBottom={2}>
          <Alert severity="error">{error}</Alert>
        </Box>
      )}

      {exceptions.map((exception, idx) => (
        <Box marginBottom={2} key={`${exception.message}-${idx}`}>
          <Alert severity="warning">
            {exception.errorCode !== null ? `[${exception.errorCode}] ` : ''}
            {exception.message}
          </Alert>
        </Box>
      ))}

      <CustomizedTables
        title="Query log results"
        data={resultTable}
        showSearchBox
        highlightBackground
        isSticky
      />
    </div>
  );
};

export default QueryLogsPage;
