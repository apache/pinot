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

import React, { useState, useEffect, useCallback } from 'react';
import {
  Grid,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Typography,
  makeStyles,
  Button,
  Input,
  FormControlLabel,
  ButtonGroup,
  Box
} from '@material-ui/core';
import Alert from '@material-ui/lab/Alert';
import FileCopyIcon from '@material-ui/icons/FileCopy';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';
import { getTimeSeriesQueryResult } from '../../requests';
import { useHistory, useLocation } from 'react-router';
import TableToolbar from '../TableToolbar';
import { Resizable } from 're-resizable';
import SimpleAccordion from '../SimpleAccordion';
import TimeseriesChart from './TimeseriesChart';
import MetricStatsTable from './MetricStatsTable';
import { parseTimeseriesResponse, isPrometheusFormat } from '../../utils/TimeseriesUtils';
import { ChartSeries } from 'Models';
import { MAX_SERIES_LIMIT } from '../../utils/ChartConstants';

const useStyles = makeStyles((theme) => ({
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
    '& .CodeMirror': {
      height: 430,
      border: '1px solid #BDCCD9',
      '& .CodeMirror-lines, & .CodeMirror-code': {
        wordWrap: 'break-word',
        whiteSpace: 'pre-wrap',
      },
    },
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
    paddingLeft: '10px',
  },
  sqlDiv: {
    height: '100%',
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: '20px',
    paddingBottom: '48px',
  },
  sqlError: {
    whiteSpace: 'pre',
    overflow: "auto"
  },
  formControl: {
    margin: theme.spacing(1),
    minWidth: 200,
    '& .MuiInputLabel-root': {
      fontSize: '0.875rem',
      transform: 'translate(0, 1.5px) scale(1)',
      '&.MuiInputLabel-shrink': {
        transform: 'translate(0, -6px) scale(1)',
      },
    },
    '& .MuiInputBase-input': {
      padding: '8px 12px',
    },
  },
  gridItem: {
    padding: theme.spacing(1),
    minWidth: 0,
  },

}));

// Extract warning component
const TruncationWarning: React.FC<{ totalSeries: number; truncatedSeries: number }> = ({
  totalSeries,
  truncatedSeries
}) => {
  if (totalSeries <= truncatedSeries) return null;

  return (
    <Alert severity="warning" style={{ marginBottom: '16px' }}>
      <Typography variant="body2">
        Large dataset detected: Showing first {truncatedSeries} of {totalSeries} series for visualization.
        Switch to JSON view to see the complete dataset.
      </Typography>
    </Alert>
  );
};

// Extract view toggle component
const ViewToggle: React.FC<{
  viewType: 'json' | 'chart';
  onViewChange: (view: 'json' | 'chart') => void;
  isChartDisabled: boolean;
  onCopy: () => void;
  copyMsg: boolean;
  classes: any;
}> = ({ viewType, onViewChange, isChartDisabled, onCopy, copyMsg, classes }) => (
  <Grid container className={classes.actionBtns} alignItems="center" justify="space-between">
    <Grid item>
      <ButtonGroup color="primary" size="small">
        <Button
          onClick={() => onViewChange('chart')}
          variant={viewType === 'chart' ? "contained" : "outlined"}
          disabled={isChartDisabled}
        >
          Chart
        </Button>
        <Button
          onClick={() => onViewChange('json')}
          variant={viewType === 'json' ? "contained" : "outlined"}
        >
          JSON
        </Button>
      </ButtonGroup>
    </Grid>
    <Grid item>
      <Button
        variant="contained"
        color="primary"
        size="small"
        className={classes.btn}
        onClick={onCopy}
      >
        Copy
      </Button>
      {copyMsg && (
        <Alert
          icon={<FileCopyIcon fontSize="inherit" />}
          severity="info"
        >
          Copied results to Clipboard
        </Alert>
      )}
    </Grid>
  </Grid>
);

const jsonoptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
  theme: 'default',
  readOnly: true,
  lineWrapping: true,
  wordWrap: 'break-word',
};

const SUPPORTED_QUERY_LANGUAGES = [
  { value: 'm3ql', label: 'M3QL' },
];

interface TimeseriesQueryConfig {
  queryLanguage: string;
  query: string;
  startTime: string;
  endTime: string;
  timeout: number;
}

const TimeseriesQueryPage = () => {
  const classes = useStyles();
  const history = useHistory();
  const location = useLocation();

  const getCurrentTimestamp = () => Math.floor(Date.now() / 1000).toString();
  const getOneMinuteAgoTimestamp = () => Math.floor((Date.now() - 60 * 1000) / 1000).toString();

  const [config, setConfig] = useState<TimeseriesQueryConfig>({
    queryLanguage: 'm3ql',
    query: '',
    startTime: getOneMinuteAgoTimestamp(),
    endTime: getCurrentTimestamp(),
    timeout: 60000,
  });

  const [rawOutput, setRawOutput] = useState<string>('');
  const [rawData, setRawData] = useState<any>(null);
  const [chartSeries, setChartSeries] = useState<ChartSeries[]>([]);
  const [truncatedChartSeries, setTruncatedChartSeries] = useState<ChartSeries[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>('');
  const [shouldAutoExecute, setShouldAutoExecute] = useState<boolean>(false);
  const [copyMsg, showCopyMsg] = React.useState(false);
  const [viewType, setViewType] = useState<'json' | 'chart'>('chart');
  const [selectedMetric, setSelectedMetric] = useState<string | null>(null);


  // Update config when URL parameters change
  useEffect(() => {
    const urlParams = new URLSearchParams(location.search);
    const newConfig = {
      queryLanguage: urlParams.get('language') || 'm3ql',
      query: urlParams.get('query') || '',
      startTime: urlParams.get('start') || getOneMinuteAgoTimestamp(),
      endTime: urlParams.get('end') || getCurrentTimestamp(),
      timeout: parseInt(urlParams.get('timeout') || '60000'),
    };

    setConfig(newConfig);

    // Auto-execute if we have a query and either start or end time
    if (newConfig.query && (newConfig.startTime || newConfig.endTime)) {
      setShouldAutoExecute(true);
    }
  }, [location.search]);

  // Auto-execute query when shouldAutoExecute is true
  useEffect(() => {
    if (shouldAutoExecute && config.query && !isLoading) {
      setShouldAutoExecute(false);
      handleExecuteQuery();
    }
  }, [shouldAutoExecute, config.query, isLoading]);

  const updateURL = useCallback((newConfig: TimeseriesQueryConfig) => {
    const params = new URLSearchParams();
    params.set('language', newConfig.queryLanguage);
    if (newConfig.query) params.set('query', newConfig.query);
    if (newConfig.startTime) params.set('start', newConfig.startTime);
    if (newConfig.endTime) params.set('end', newConfig.endTime);
    if (newConfig.timeout && newConfig.timeout !== 60000) {
      params.set('timeout', newConfig.timeout.toString());
    }

    const newURL = params.toString() ? `?${params.toString()}` : '';
    history.push({
      pathname: location.pathname,
      search: newURL
    });
  }, [history, location.pathname]);

  const handleConfigChange = (field: keyof TimeseriesQueryConfig, value: any) => {
    setConfig(prev => ({ ...prev, [field]: value }));
  };

  const handleQueryChange = (editor: any, data: any, value: string) => {
    setConfig(prev => ({ ...prev, query: value }));
  };

  const handleQueryInterfaceKeyDown = (editor: any, event: any) => {
    const modifiedEnabled = event.metaKey == true || event.ctrlKey == true;

    // Map (Cmd/Ctrl) + Enter KeyPress to executing the query
    if (modifiedEnabled && event.keyCode == 13) {
      handleExecuteQuery();
    }
  };

  const handleQueryInterfaceKeyDownRef = React.useRef(handleQueryInterfaceKeyDown);

  useEffect(() => {
    handleQueryInterfaceKeyDownRef.current = handleQueryInterfaceKeyDown;
  }, [handleQueryInterfaceKeyDown]);

  // Extract data processing logic
  const processQueryResponse = useCallback((parsedData: any) => {
    setRawData(parsedData);
    setRawOutput(JSON.stringify(parsedData, null, 2));

    // Parse timeseries data for chart and stats
    if (isPrometheusFormat(parsedData)) {
      const series = parseTimeseriesResponse(parsedData);
      setChartSeries(series);

      // Create truncated series for visualization (limit to MAX_SERIES_LIMIT)
      const truncatedSeries = series.slice(0, MAX_SERIES_LIMIT);
      setTruncatedChartSeries(truncatedSeries);
    } else {
      setChartSeries([]);
      setTruncatedChartSeries([]);
    }
  }, []);

  const handleExecuteQuery = useCallback(async () => {
    if (!config.query.trim()) {
      setError('Please enter a query');
      return;
    }

    updateURL(config);
    setIsLoading(true);
    setError('');
    setRawOutput('');

    try {
      const requestPayload = {
        language: config.queryLanguage,
        query: config.query,
        start: config.startTime,
        end: config.endTime,
        step: '1m',
        trace: false,
        queryOptions: `timeoutMs=${config.timeout}`
      };

      const response = await getTimeSeriesQueryResult(requestPayload);

      // Handle response data - it might be stringified JSON or already an object
      const parsedData = typeof response.data === 'string'
        ? JSON.parse(response.data)
        : response.data;

      processQueryResponse(parsedData);
    } catch (error) {
      console.error('Error executing timeseries query:', error);
      const errorMessage = error.response?.data?.message || error.message || 'Unknown error occurred';
      setError(`Query execution failed: ${errorMessage}`);
    } finally {
      setIsLoading(false);
    }
  }, [config, updateURL, processQueryResponse]);

  const copyToClipboard = () => {
    const aux = document.createElement('input');
    aux.setAttribute('value', rawOutput);
    document.body.appendChild(aux);
    aux.select();
    document.execCommand('copy');
    document.body.removeChild(aux);
    showCopyMsg(true);
    setTimeout(() => showCopyMsg(false), 3000);
  };

  return (
    <Grid container>
      <Grid item xs={12} className={classes.rightPanel}>
        <Resizable
          defaultSize={{ width: '100%', height: 148 }}
          minHeight={148}
          maxWidth={'100%'}
          maxHeight={'50vh'}
          enable={{bottom: true}}>
          <div className={classes.sqlDiv}>
            <TableToolbar
              name="Timeseries Query Editor"
              showSearchBox={false}
              showTooltip={true}
              tooltipText="This editor supports timeseries queries. Enter your M3QL query here."
            />
            <CodeMirror
              value={config.query}
              onChange={handleQueryChange}
              options={{
                lineNumbers: true,
                mode: 'text/plain',
                theme: 'default',
                lineWrapping: true,
                indentWithTabs: true,
                smartIndent: true,
              }}
              className={classes.codeMirror}
              autoCursor={false}
              onKeyDown={(editor, event) => handleQueryInterfaceKeyDownRef.current(editor, event)}
            />
          </div>
        </Resizable>

        <Grid container className={classes.checkBox} spacing={2} alignItems="center" justify="space-between">
          <Grid item xs={12} sm={6} md={2} className={classes.gridItem}>
            <FormControl fullWidth={true} className={classes.formControl}>
              <InputLabel>Query Language</InputLabel>
              <Select
                value={config.queryLanguage}
                onChange={(e) => handleConfigChange('queryLanguage', e.target.value)}
              >
                {SUPPORTED_QUERY_LANGUAGES.map((lang) => (
                  <MenuItem key={lang.value} value={lang.value}>
                    {lang.label}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>

          <Grid item xs={12} md={2} className={classes.gridItem}>
            <FormControl fullWidth={true} className={classes.formControl}>
              <InputLabel>Start Time (Unix timestamp)</InputLabel>
              <Input
                type="text"
                value={config.startTime}
                onChange={(e) => handleConfigChange('startTime', e.target.value)}
                placeholder={getOneMinuteAgoTimestamp()}
              />
            </FormControl>
          </Grid>

          <Grid item xs={12} md={2} className={classes.gridItem}>
            <FormControl fullWidth={true} className={classes.formControl}>
              <InputLabel>End Time (Unix timestamp)</InputLabel>
              <Input
                type="text"
                value={config.endTime}
                onChange={(e) => handleConfigChange('endTime', e.target.value)}
                placeholder={getCurrentTimestamp()}
              />
            </FormControl>
          </Grid>

          <Grid item xs={12} sm={6} md={2} className={classes.gridItem}>
            <FormControl fullWidth={true} className={classes.formControl}>
              <InputLabel>Timeout (Milliseconds)</InputLabel>
              <Input
                type="text"
                value={config.timeout}
                onChange={(e) => handleConfigChange('timeout', parseInt(e.target.value) || 60000)}
              />
            </FormControl>
          </Grid>

          <Grid item xs={12} sm={12} md={2} className={classes.gridItem} style={{ display: 'flex', justifyContent: 'center' }}>
            <Button
              variant="contained"
              color="primary"
              onClick={handleExecuteQuery}
              disabled={isLoading || !config.query.trim()}
              endIcon={<span style={{fontSize: '0.8em', lineHeight: 1}}>{navigator.platform.includes('Mac') ? '⌘↵' : 'Ctrl+↵'}</span>}
            >
              {isLoading ? 'Running Query...' : 'Run Query'}
            </Button>
          </Grid>
        </Grid>

        {error && (
          <Alert severity="error" className={classes.sqlError}>
            {error}
          </Alert>
        )}

        {rawOutput && (
          <Grid item xs style={{ backgroundColor: 'white' }}>
                         <ViewToggle
               viewType={viewType}
               onViewChange={setViewType}
               isChartDisabled={truncatedChartSeries.length === 0}
               onCopy={copyToClipboard}
               copyMsg={copyMsg}
               classes={classes}
             />

                                    {viewType === 'chart' && (
              <SimpleAccordion
                headerTitle="Timeseries Chart & Statistics"
                showSearchBox={false}
              >
                {truncatedChartSeries.length > 0 ? (
                  <>
                    <TruncationWarning
                      totalSeries={chartSeries.length}
                      truncatedSeries={truncatedChartSeries.length}
                    />
                    <TimeseriesChart
                      series={truncatedChartSeries}
                      height={500}
                      selectedMetric={selectedMetric}
                    />
                    <MetricStatsTable
                      series={truncatedChartSeries}
                      selectedMetric={selectedMetric}
                      onMetricSelect={setSelectedMetric}
                    />
                  </>
                ) : (
                  <Box p={3} textAlign="center">
                    <Typography variant="h6" color="textSecondary" gutterBottom>
                      No Chart Data Available
                    </Typography>
                    <Typography variant="body2" color="textSecondary">
                      The query response is not in Prometheus-compatible format or contains no timeseries data.
                      <br />
                      Switch to JSON view to see the raw response.
                    </Typography>
                  </Box>
                )}
              </SimpleAccordion>
            )}

            {viewType === 'json' && (
              <SimpleAccordion
                headerTitle="Query Result (JSON Format)"
                showSearchBox={false}
              >
                <CodeMirror
                  options={jsonoptions}
                  value={rawOutput}
                  className={classes.queryOutput}
                  autoCursor={false}
                />
              </SimpleAccordion>
            )}
          </Grid>
        )}
      </Grid>
    </Grid>
  );
};

export default TimeseriesQueryPage;
