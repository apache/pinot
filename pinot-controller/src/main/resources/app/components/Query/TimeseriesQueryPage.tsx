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
  Select,
  MenuItem,
  Typography,
  makeStyles,
  Button,
  Input,
  ButtonGroup,
  Box,
  Checkbox
} from '@material-ui/core';
import Alert from '@material-ui/lab/Alert';
import FileCopyIcon from '@material-ui/icons/FileCopy';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';
import { getTimeSeriesLanguages } from '../../requests';
import { useHistory, useLocation } from 'react-router';
import TableToolbar from '../TableToolbar';
import { Resizable } from 're-resizable';
import SimpleAccordion from '../SimpleAccordion';
import TimeseriesChart from './TimeseriesChart';
import MetricStatsTable from './MetricStatsTable';
import { parseTimeseriesResponse, isBrokerFormat } from '../../utils/TimeseriesUtils';
import { ChartSeries, TableData } from 'Models';
import { DEFAULT_SERIES_LIMIT } from '../../utils/ChartConstants';
import CustomizedTables from '../Table';
import PinotMethodUtils from '../../utils/PinotMethodUtils';

// Constants
const EDITOR_MIN_HEIGHT = 220;
const EDITOR_DEFAULT_WIDTH_PERCENT = 75;
const EDITOR_MIN_WIDTH_PERCENT = 30;
const EDITOR_MAX_WIDTH_PERCENT = 80;
const CONTROL_INPUT_HEIGHT = 36;

// Helper functions (outside component to avoid recreation)
const getCurrentTimestamp = () => Math.floor(Date.now() / 1000).toString();
const getOneMinuteAgoTimestamp = () => Math.floor((Date.now() - 60 * 1000) / 1000).toString();
const isMac = () => /Mac|iPhone|iPad|iPod/.test(navigator.userAgent);

interface CodeMirrorEditor {
  getValue: () => string;
  setValue: (value: string) => void;
}

interface CodeMirrorChangeData {
  from: { line: number; ch: number };
  to: { line: number; ch: number };
  text: string[];
  removed: string[];
  origin: string;
}

interface KeyboardEvent {
  keyCode: number;
  metaKey: boolean;
  ctrlKey: boolean;
}

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
  queryControlsRow: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'stretch',
    margin: '20px 0',
  },
  queryControlItem: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
  },
  queryControlLabel: {
    fontSize: '0.875rem',
    color: 'rgba(0, 0, 0, 0.54)',
    marginBottom: theme.spacing(1),
    whiteSpace: 'nowrap',
  },
  queryControlInput: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
  },
  editorRow: {
    display: 'flex',
    width: '100%',
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
  queryOptionsPanel: {
    flex: 1,
    marginLeft: theme.spacing(1),
    minWidth: '18%',
  },
  controlSelect: {
    minWidth: 120,
    height: CONTROL_INPUT_HEIGHT,
  },
  controlInputStart: {
    minWidth: 130,
  },
  controlInputEnd: {
    minWidth: 130,
  },
  controlInputTimeout: {
    minWidth: 80,
  },
  controlInputCheckbox: {
    height: CONTROL_INPUT_HEIGHT,
    display: 'flex',
    alignItems: 'center',
  },
  controlButton: {
    height: CONTROL_INPUT_HEIGHT,
  },
  queryControlLabelHidden: {
    visibility: 'hidden',
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
  seriesLimitContainer: {
    display: 'flex',
    justifyContent: 'flex-end',
    alignItems: 'center',
    padding: theme.spacing(1),
    borderTop: `1px solid ${theme.palette.divider}`,
    backgroundColor: theme.palette.grey[50],
  },
  seriesLimitInput: {
    width: 50,
    marginLeft: theme.spacing(2),
    '& .MuiInputBase-input': { padding: '8px 12px', fontSize: '1rem' },
  },
  timeseriesAccordionDetails: { overflow: 'hidden !important' },

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
        Truncation warning: Showing first {truncatedSeries} of {totalSeries} series for visualization.
        Switch to JSON view to see the complete dataset or increase the max series limit.
      </Typography>
    </Alert>
  );
};

// Extract view toggle component
const ViewToggle: React.FC<{
  viewType: 'json' | 'chart';
  onViewChange: (view: 'json' | 'chart') => void;
  isChartDisabled: boolean;
  isExplainMode: boolean;
  onCopy: () => void;
  copyMsg: boolean;
  classes: ReturnType<typeof useStyles>;
}> = ({ viewType, onViewChange, isChartDisabled, isExplainMode, onCopy, copyMsg, classes }) => (
  <Grid container className={classes.actionBtns} alignItems="center" justify="space-between">
    <Grid item>
      <ButtonGroup color="primary" size="small">
        <Button
          onClick={() => onViewChange('chart')}
          variant={viewType === 'chart' ? "contained" : "outlined"}
          disabled={isChartDisabled}
        >
          {isExplainMode ? 'Result' : 'Chart'}
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

interface TimeseriesQueryConfig {
  queryLanguage: string;
  query: string;
  startTime: string;
  endTime: string;
  timeout: number;
  queryOptions: string;
  explainPlan: boolean;
}

const TimeseriesQueryPage = () => {
  const classes = useStyles();
  const history = useHistory();
  const location = useLocation();

  const [config, setConfig] = useState<TimeseriesQueryConfig>({
    queryLanguage: 'm3ql',
    query: '',
    startTime: getOneMinuteAgoTimestamp(),
    endTime: getCurrentTimestamp(),
    timeout: 60000,
    queryOptions: '',
    explainPlan: false,
  });

  const [supportedLanguages, setSupportedLanguages] = useState<Array<string>>([]);
  const [languagesLoading, setLanguagesLoading] = useState(true);

  const [rawOutput, setRawOutput] = useState<string>('');
  const [chartSeries, setChartSeries] = useState<ChartSeries[]>([]);
  const [totalSeriesCount, setTotalSeriesCount] = useState<number>(0);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [error, setError] = useState<string>('');
  const [shouldAutoExecute, setShouldAutoExecute] = useState<boolean>(false);
  const [copyMsg, showCopyMsg] = React.useState(false);
  const [viewType, setViewType] = useState<'json' | 'chart'>('chart');
  const [selectedMetric, setSelectedMetric] = useState<string | null>(null);
  const [seriesLimitInput, setSeriesLimitInput] = useState<string>(DEFAULT_SERIES_LIMIT.toString());
  const [editorHeight, setEditorHeight] = useState<number>(EDITOR_MIN_HEIGHT);
  const [editorWidthPercent, setEditorWidthPercent] = useState<number>(EDITOR_DEFAULT_WIDTH_PERCENT);
  const editorContainerRef = React.useRef<HTMLDivElement | null>(null);
  const [resultTable, setResultTable] = useState<TableData>({
    columns: [],
    records: [],
  });
  const [queryStats, setQueryStats] = useState<TableData>({
    columns: [],
    records: [],
  });
  const [executedExplainMode, setExecutedExplainMode] = useState<boolean>(false);

  // Fetch supported languages from controller configuration
  useEffect(() => {
    const fetchLanguages = async () => {
      try {
        setLanguagesLoading(true);
        const response = await getTimeSeriesLanguages();
        const languages = response.data || [];

        setSupportedLanguages(languages);
      } catch (error) {
        console.error('Error fetching timeseries languages:', error);
        setSupportedLanguages([]);
      } finally {
        setLanguagesLoading(false);
      }
    };
    fetchLanguages();
  }, []);

  // Update config when URL parameters change
  useEffect(() => {
    const urlParams = new URLSearchParams(location.search);
    const newConfig = {
      queryLanguage: urlParams.get('language') || 'm3ql',
      query: urlParams.get('query') || '',
      startTime: urlParams.get('start') || getOneMinuteAgoTimestamp(),
      endTime: urlParams.get('end') || getCurrentTimestamp(),
      timeout: parseInt(urlParams.get('timeout') || '60000'),
      queryOptions: urlParams.get('queryOptions') || '',
      explainPlan: urlParams.get('mode') === 'explain',
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
    if (newConfig.queryOptions && newConfig.queryOptions.trim().length > 0) {
      params.set('queryOptions', newConfig.queryOptions.trim());
    }
    if (newConfig.explainPlan) {
      params.set('mode', 'explain');
    }

    const newURL = params.toString() ? `?${params.toString()}` : '';
    history.push({
      pathname: location.pathname,
      search: newURL
    });
  }, [history, location.pathname]);

  const handleConfigChange = (field: keyof TimeseriesQueryConfig, value: string | number | boolean) => {
    setConfig(prev => ({ ...prev, [field]: value }));
  };

  const handleQueryChange = (editor: CodeMirrorEditor, data: CodeMirrorChangeData, value: string) => {
    setConfig(prev => ({ ...prev, query: value }));
  };

  const handleQueryOptionsChange = (editor: CodeMirrorEditor, data: CodeMirrorChangeData, value: string) => {
    setConfig(prev => ({ ...prev, queryOptions: value }));
  };

  const handleQueryInterfaceKeyDown = (editor: CodeMirrorEditor, event: KeyboardEvent) => {
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

  const handleExecuteQuery = useCallback(async () => {
    if (!config.query.trim()) {
      setError('Please enter a query');
      return;
    }

    let queryOptionsObject: Record<string, string | number | boolean> = {};
    if (config.queryOptions.trim().length > 0) {
      try {
        const parsedOptions = JSON.parse(config.queryOptions);
        if (parsedOptions && typeof parsedOptions === 'object' && !Array.isArray(parsedOptions)) {
          queryOptionsObject = parsedOptions;
        } else {
          setError('Query options must be a JSON object');
          return;
        }
      } catch (parseError) {
        setError('Query options must be valid JSON');
        return;
      }
    }
    if (config.timeout && queryOptionsObject.timeoutMs === undefined) {
      queryOptionsObject.timeoutMs = config.timeout;
    }

    updateURL(config);
    setIsLoading(true);
    setError('');
    setRawOutput('');
    setSelectedMetric(null);
    setChartSeries([]);
    setTotalSeriesCount(0);
    setQueryStats({ columns: [], records: [] });
    setResultTable({ columns: [], records: [] });

    try {
      const requestPayload = {
        language: config.queryLanguage,
        query: config.query,
        start: config.startTime,
        end: config.endTime,
        step: '1m',
        trace: false,
        queryOptions: queryOptionsObject,
        ...(config.explainPlan ? { mode: 'explain' } : {}),
      };

      // Call the API and process with the shared broker response processor
      const results = await PinotMethodUtils.getTimeseriesQueryResults(requestPayload);

      // Set raw output
      setRawOutput(JSON.stringify(results.data, null, 2));

      // Set query stats (already extracted by the utility)
      setQueryStats(results.queryStats || { columns: [], records: [] });
      setResultTable(results.result || { columns: [], records: [] });

      // Handle exceptions/errors
      if (results.exceptions && results.exceptions.length > 0) {
        const errorMsg = results.exceptions.map((e: any) => e.message || e.toString()).join('; ');
        setError(errorMsg);
        setChartSeries([]);
        setTotalSeriesCount(0);
        return;
      }

      // Parse timeseries data for visualization
      if (isBrokerFormat(results.data)) {
        const series = parseTimeseriesResponse(results.data);
        setTotalSeriesCount(series.length);

        // Create truncated series for visualization
        const limit = parseInt(seriesLimitInput, 10);
        const effectiveLimit = !isNaN(limit) && limit > 0 ? limit : DEFAULT_SERIES_LIMIT;

        const truncatedSeries = series.slice(0, effectiveLimit);
        setChartSeries(truncatedSeries);
      } else {
        setChartSeries([]);
        setTotalSeriesCount(0);
      }

      // Set the executed explain mode state after successful query
      setExecutedExplainMode(config.explainPlan);
    } catch (error) {
      console.error('Error executing timeseries query:', error);
      const errorMessage = error.response?.data?.message || error.message || 'Unknown error occurred';
      setError(`Query execution failed: ${errorMessage}`);
    } finally {
      setIsLoading(false);
    }
  }, [config, updateURL, seriesLimitInput]);

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
      {/* Banner for no enabled languages */}
      {!languagesLoading && supportedLanguages.length === 0 && (
        <Grid item xs={12}>
          <Alert severity="warning" style={{ marginBottom: '16px' }}>
            <strong>No timeseries languages enabled.</strong> Please configure timeseries languages in your controller, broker and server configurations using the <code>pinot.timeseries.languages</code> property.
          </Alert>
        </Grid>
      )}

      <Grid item xs={12} className={classes.rightPanel}>
        <div className={classes.editorRow} ref={editorContainerRef}>
          <Resizable
            size={{ width: `${editorWidthPercent}%`, height: editorHeight }}
            minHeight={EDITOR_MIN_HEIGHT}
            minWidth={`${EDITOR_MIN_WIDTH_PERCENT}%`}
            maxWidth={`${EDITOR_MAX_WIDTH_PERCENT}%`}
            maxHeight={'50vh'}
            enable={{ bottom: true, right: true, bottomRight: true }}
            onResize={(event, direction, ref) => {
              setEditorHeight(ref.offsetHeight);
              if (editorContainerRef.current && (direction === 'right' || direction === 'bottomRight')) {
                const containerWidth = editorContainerRef.current.offsetWidth;
                const newPercent = Math.min(EDITOR_MAX_WIDTH_PERCENT, Math.max(EDITOR_MIN_WIDTH_PERCENT, (ref.offsetWidth / containerWidth) * 100));
                setEditorWidthPercent(newPercent);
              }
            }}
          >
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
                  readOnly: supportedLanguages.length === 0,
                }}
                className={classes.codeMirror}
                autoCursor={false}
                onKeyDown={(editor, event) => handleQueryInterfaceKeyDownRef.current(editor, event)}
              />
            </div>
          </Resizable>
          <div className={`${classes.sqlDiv} ${classes.queryOptionsPanel}`} style={{ height: editorHeight }}>
            <TableToolbar
              name="Query Options (JSON)"
              showSearchBox={false}
              showTooltip={true}
              tooltipText={
                <span>
                  Enter query options as a JSON map of strings, e.g. {`{"enableNullHandling": "true"}`}. Please find the list of supported query options in the{' '}
                  <a
                    href="https://docs.pinot.apache.org/users/user-guide-query/query-options"
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{ color: '#90caf9', textDecoration: 'underline' }}
                  >
                    documentation
                  </a>.
                </span>
              }
            />
            <CodeMirror
              value={config.queryOptions}
              onChange={handleQueryOptionsChange}
              options={{
                lineNumbers: true,
                mode: 'application/json',
                theme: 'default',
                lineWrapping: true,
                indentWithTabs: true,
                smartIndent: true,
                readOnly: supportedLanguages.length === 0,
              }}
              className={classes.codeMirror}
              autoCursor={false}
            />
          </div>
        </div>

        <div className={classes.queryControlsRow}>
          <div className={classes.queryControlItem}>
            <Typography className={classes.queryControlLabel}>Query Language</Typography>
            <div className={classes.queryControlInput}>
              <Select
                value={config.queryLanguage}
                onChange={(e) => handleConfigChange('queryLanguage', e.target.value as string)}
                disabled={languagesLoading || supportedLanguages.length === 0}
                variant="outlined"
                className={classes.controlSelect}
              >
                {supportedLanguages.map((lang) => (
                  <MenuItem key={lang} value={lang}>
                    {lang}
                  </MenuItem>
                ))}
              </Select>
            </div>
          </div>

          <div className={classes.queryControlItem}>
            <Typography className={classes.queryControlLabel}>Start Time (Unix)</Typography>
            <div className={classes.queryControlInput}>
              <Input
                type="text"
                value={config.startTime}
                onChange={(e) => handleConfigChange('startTime', e.target.value as string)}
                placeholder={getOneMinuteAgoTimestamp()}
                disabled={supportedLanguages.length === 0}
                className={classes.controlInputStart}
              />
            </div>
          </div>

          <div className={classes.queryControlItem}>
            <Typography className={classes.queryControlLabel}>End Time (Unix)</Typography>
            <div className={classes.queryControlInput}>
              <Input
                type="text"
                value={config.endTime}
                onChange={(e) => handleConfigChange('endTime', e.target.value as string)}
                placeholder={getCurrentTimestamp()}
                disabled={supportedLanguages.length === 0}
                className={classes.controlInputEnd}
              />
            </div>
          </div>

          <div className={classes.queryControlItem}>
            <Typography className={classes.queryControlLabel}>Timeout (ms)</Typography>
            <div className={classes.queryControlInput}>
              <Input
                type="text"
                value={config.timeout}
                onChange={(e) => handleConfigChange('timeout', parseInt(e.target.value as string) || 60000)}
                disabled={supportedLanguages.length === 0}
                className={classes.controlInputTimeout}
              />
            </div>
          </div>

          <div className={classes.queryControlItem}>
            <Typography className={classes.queryControlLabel}>Explain Plan</Typography>
            <div className={`${classes.queryControlInput} ${classes.controlInputCheckbox}`}>
              <Checkbox
                checked={config.explainPlan}
                onChange={(e) => handleConfigChange('explainPlan', e.target.checked)}
                color="primary"
                size="small"
                disabled={supportedLanguages.length === 0}
              />
            </div>
          </div>

          <div className={classes.queryControlItem}>
            <Typography className={`${classes.queryControlLabel} ${classes.queryControlLabelHidden}`}>Run</Typography>
            <div className={classes.queryControlInput}>
              <Button
                variant="contained"
                color="primary"
                onClick={handleExecuteQuery}
                disabled={isLoading || !config.query.trim() || supportedLanguages.length === 0}
                endIcon={<span style={{ fontSize: '0.8em', lineHeight: 1 }}>{isMac() ? '⌘↵' : 'Ctrl+↵'}</span>}
                className={classes.controlButton}
              >
                {isLoading ? 'Running...' : 'Run Query'}
              </Button>
            </div>
          </div>
        </div>

        {queryStats.columns.length > 0 && (
          <Grid item xs style={{ backgroundColor: 'white' }}>
            <CustomizedTables
              title="Query Response Stats"
              data={queryStats}
              showSearchBox={true}
              inAccordionFormat={true}
            />
          </Grid>
        )}

        {rawOutput && (
          <Grid item xs style={{ backgroundColor: 'white' }}>
            <ViewToggle
              viewType={viewType}
              onViewChange={setViewType}
              isChartDisabled={executedExplainMode ? false : chartSeries.length === 0}
              isExplainMode={executedExplainMode}
              onCopy={copyToClipboard}
              copyMsg={copyMsg}
              classes={classes}
            />

            {error && (
              <Alert severity="error" className={classes.sqlError}>
                {error}
              </Alert>
            )}

            {viewType === 'chart' && executedExplainMode && (
              <CustomizedTables
                title="Query Result"
                data={resultTable}
                showSearchBox={true}
                inAccordionFormat={true}
              />
            )}

            {viewType === 'chart' && !executedExplainMode && (
              <SimpleAccordion
                headerTitle="Timeseries Chart & Statistics"
                showSearchBox={false}
                detailsContainerClass={classes.timeseriesAccordionDetails}
              >
                {chartSeries.length > 0 ? (
                  <>
                    <TruncationWarning
                      totalSeries={totalSeriesCount}
                      truncatedSeries={chartSeries.length}
                    />
                    <TimeseriesChart
                      series={chartSeries}
                      height={500}
                      selectedMetric={selectedMetric}
                    />
                    <MetricStatsTable
                      series={chartSeries}
                      selectedMetric={selectedMetric}
                      onMetricSelect={setSelectedMetric}
                    />
                    <div className={classes.seriesLimitContainer}>
                      <Typography variant="body2" color="textSecondary">
                        Max Series Render Limit:
                      </Typography>
                      <FormControl className={classes.seriesLimitInput}>
                        <Input
                          value={seriesLimitInput}
                          onChange={(e) => setSeriesLimitInput(e.target.value)}
                          inputProps={{ min: 1, max: 1000 }}
                          placeholder={DEFAULT_SERIES_LIMIT.toString()}
                        />
                      </FormControl>
                    </div>
                  </>
                ) : (
                  <Box p={3} textAlign="center">
                    <Typography variant="h6" color="textSecondary" gutterBottom>
                      No Chart Data Available
                    </Typography>
                    <Typography variant="body2" color="textSecondary">
                      The query response does not contain valid timeseries data.
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
