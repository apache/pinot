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

/* eslint-disable no-nested-ternary */
import * as React from 'react';
import PropTypes from 'prop-types';
import {
  withStyles,
  createStyles,
  makeStyles,
  useTheme,
} from '@material-ui/core/styles';
import ComponentLoader from './ComponentLoader';
import Dialog from '@material-ui/core/Dialog';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import { TablePagination, Tooltip } from '@material-ui/core';
import {TableData, TableSortFunction} from 'Models';
import IconButton from '@material-ui/core/IconButton';
import FirstPageIcon from '@material-ui/icons/FirstPage';
import KeyboardArrowLeft from '@material-ui/icons/KeyboardArrowLeft';
import KeyboardArrowRight from '@material-ui/icons/KeyboardArrowRight';
import LastPageIcon from '@material-ui/icons/LastPage';
import ArrowDropDownIcon from '@material-ui/icons/ArrowDropDown';
import ArrowDropUpIcon from '@material-ui/icons/ArrowDropUp';
import { Link } from 'react-router-dom';
import Chip from '@material-ui/core/Chip';
import { get, has, orderBy } from 'lodash';
import app_state from '../app_state';
import { sortBytes, sortNumberOfSegments } from '../utils/SortFunctions'
import Utils from '../utils/Utils';
import TableToolbar from './TableToolbar';
import SimpleAccordion from './SimpleAccordion';

type Props = {
  title?: string,
  data: TableData,
  defaultRowsPerPage?: number,
  addLinks?: boolean,
  cellClickCallback?: Function,
  isCellClickable?: boolean,
  highlightBackground?: boolean,
  isSticky?: boolean,
  baseURL?: string,
  recordsCount?: number,
  showSearchBox: boolean,
  inAccordionFormat?: boolean,
  regexReplace?: boolean,
  accordionToggleObject?: {
    toggleChangeHandler: (event: React.ChangeEvent<HTMLInputElement>) => void;
    toggleName: string;
    toggleValue: boolean;
  },
  tooltipData?: string[],
  onRowsRendered?: (rows: any[]) => void
};

// These sort functions are applied to any columns with these names. Otherwise, we just
// sort on the raw data. Ideally users of this class would pass in custom sort functions
// for their columns, but this pattern already existed, so we're at least making the
// improvement to pull this out to a common variable.
let staticSortFunctions: Map<string, TableSortFunction> = new Map()
staticSortFunctions.set("Number of Segments", sortNumberOfSegments);
staticSortFunctions.set("Estimated Size", sortBytes);
staticSortFunctions.set("Reported Size", sortBytes);

const StyledTableRow = withStyles((theme) =>
  createStyles({
    root: {
      '&:nth-of-type(even)': {
        backgroundColor: theme.palette.action.hover,
      },
    },
  })
)(TableRow);

const StyledTableCell = withStyles((theme) =>
  createStyles({
    root: {
      padding: '10px 15px',
    }
  })
)(TableCell);

const StyledChip = withStyles((theme) =>
  createStyles({
    root: {
      height: '28px',
      '& span': {
        paddingLeft: '8px',
        paddingRight: '8px',
        fontWeight: '600'
      }
    }
  })
)(Chip);

const useStyles = makeStyles((theme) => ({
  root: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: '20px',
  },
  highlightBackground: {
    border: '1px #4285f4 solid',
    backgroundColor: 'rgba(66, 133, 244, 0.05)',
    borderRadius: 4,
    marginBottom: '20px',
  },
  table: {
    borderTop: '1px solid #BDCCD9',
    // 'td':{
    //   'tr:first-child':{
    //     wordBreak: 'break-all'
    //   }
    // }
  },
  isCellClickable: {
    color: theme.palette.primary.main,
    cursor: 'pointer',
    textDecoration: 'underline',
    borderTop: '1px #BDCCD9 solid'
  },
  isSticky: {
    whiteSpace: 'nowrap'
  },
  head: {
    fontWeight: 600,
    borderBottom: '2px solid #BDCCD9',
    lineHeight: '1rem',
    cursor: 'pointer',
    whiteSpace: 'nowrap'
  },
  body: {
    fontSize: 14,
    color: '#3B454E',
    padding: '0.5rem 0.6rem',
  },
  nodata: {
    textAlign: 'center',
  },
  link: {
    color: '#4285f4',
  },
  spacer: {
    flex: '0 1 auto',
  },
  cellStatusGood: {
    color: '#4CAF50',
    border: '1px solid #4CAF50',
  },
  cellStatusBad: {
    color: '#f44336',
    border: '1px solid #f44336',
  },
  cellStatusConsuming: {
    color: '#ff9800',
    border: '1px solid #ff9800',
  },
  cellStatusError: {
    color: '#a11',
    border: '1px solid #a11',
  },
  clickable: {
    cursor: 'pointer',
    color: '#4285f4',
    textDecoration: 'underline',
  }
}));

const usePaginationStyles = makeStyles({
  root: {
    flexShrink: 0,
    marginLeft: 'auto',
  },
});

function TablePaginationActions(props) {
  const classes = usePaginationStyles();
  const theme = useTheme();
  const { count, page, rowsPerPage, onChangePage } = props;

  const handleFirstPageButtonClick = (event) => {
    onChangePage(event, 0);
  };

  const handleBackButtonClick = (event) => {
    onChangePage(event, page - 1);
  };

  const handleNextButtonClick = (event) => {
    onChangePage(event, page + 1);
  };

  const handleLastPageButtonClick = (event) => {
    onChangePage(event, Math.max(0, Math.ceil(count / rowsPerPage) - 1));
  };

  return (
    <div className={classes.root}>
      <IconButton
        onClick={handleFirstPageButtonClick}
        disabled={page === 0}
        aria-label="first page"
      >
        {theme.direction === 'rtl' ? <LastPageIcon /> : <FirstPageIcon />}
      </IconButton>
      <IconButton
        onClick={handleBackButtonClick}
        disabled={page === 0}
        aria-label="previous page"
      >
        {theme.direction === 'rtl' ? (
          <KeyboardArrowRight />
        ) : (
          <KeyboardArrowLeft />
        )}
      </IconButton>
      <IconButton
        onClick={handleNextButtonClick}
        disabled={page >= Math.ceil(count / rowsPerPage) - 1}
        aria-label="next page"
      >
        {theme.direction === 'rtl' ? (
          <KeyboardArrowLeft />
        ) : (
          <KeyboardArrowRight />
        )}
      </IconButton>
      <IconButton
        onClick={handleLastPageButtonClick}
        disabled={page >= Math.ceil(count / rowsPerPage) - 1}
        aria-label="last page"
      >
        {theme.direction === 'rtl' ? <FirstPageIcon /> : <LastPageIcon />}
      </IconButton>
    </div>
  );
}

TablePaginationActions.propTypes = {
  count: PropTypes.number.isRequired,
  onChangePage: PropTypes.func.isRequired,
  page: PropTypes.number.isRequired,
  rowsPerPage: PropTypes.number.isRequired,
};

export default function CustomizedTables({
  title,
  data,
  defaultRowsPerPage,
  addLinks,
  cellClickCallback,
  isCellClickable,
  highlightBackground,
  isSticky,
  baseURL,
  recordsCount,
  showSearchBox,
  inAccordionFormat,
  regexReplace,
  accordionToggleObject,
  tooltipData,
  onRowsRendered
}: Props) {
  // Separate the initial and final data into two separated state variables.
  // This way we can filter and sort the data without affecting the original data.
  // If the component receives new data, we can simply set the new data to the initial data,
  // and the filters and sorts will be applied to the new data.
  const [initialData, setInitialData] = React.useState(data);
  const [finalData, setFinalData] = React.useState(Utils.tableFormat(data));
  React.useEffect( () => {
    setInitialData(data);
  }, [data]);
  // We do not use data.isLoading directly in the renderer because there's a gap between data
  // changing and finalData being set. Without this, there's a flicker where we go from
  // loading -> no records found -> not loading + data.
  const [isLoading, setIsLoading] = React.useState(false);
  React.useEffect( () => {
    setIsLoading(data.isLoading || false);
  }, [finalData]);

  const [order, setOrder] = React.useState(false);
  const [columnClicked, setColumnClicked] = React.useState('');

  const classes = useStyles();
  const [rowsPerPage, setRowsPerPage] = React.useState(defaultRowsPerPage || 10);
  const [page, setPage] = React.useState(0);
  const currentlyRenderedRows = React.useMemo(() => {
    return finalData.slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
  }, [finalData, page, rowsPerPage])
  React.useEffect(() => {
    if (typeof onRowsRendered === 'function' && page > 0) {
      onRowsRendered(currentlyRenderedRows)
    }
  }, [page, rowsPerPage])

  const handleChangeRowsPerPage = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const handleChangePage = (event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const [search, setSearch] = React.useState<string>('');

  const timeoutId = React.useRef<NodeJS.Timeout>();

  const filterSearchResults = React.useCallback((str: string) => {
    if (str === '') {
      setFinalData(Utils.tableFormat(data));
    } else {
      const filteredRescords = initialData.records.filter((record) => {
        const searchFound = record.find(
          (cell) => cell.toString().toLowerCase().indexOf(str) > -1
        );
        if (searchFound) {
          return true;
        }
        return false;
      });
      let filteredData = {...initialData, records: filteredRescords};
      setFinalData(Utils.tableFormat(filteredData));
    }
  }, [initialData, setFinalData]);

  React.useEffect(() => {
    clearTimeout(timeoutId.current);
    timeoutId.current = setTimeout(() => {
      filterSearchResults(search.toLowerCase());
      // Table.tsx currently doesn't support sorting after filtering. So for now, we just
      // remove the visual indicator of the sorted column until users sort again.
      setColumnClicked('')
    }, 200);

    return () => {
      clearTimeout(timeoutId.current);
    };
  }, [search, timeoutId, filterSearchResults]);

  const styleCell = (str: string) => {
    if (str.toLowerCase() === 'good' || str.toLowerCase() === 'online' || str.toLowerCase() === 'alive' || str.toLowerCase() === 'true') {
      return (
        <StyledChip
          label={str}
          className={classes.cellStatusGood}
          variant="outlined"
        />
      );
    }
    if (str.toLocaleLowerCase() === 'bad' || str.toLowerCase() === 'offline' || str.toLowerCase() === 'dead' || str.toLowerCase() === 'false') {
      return (
        <StyledChip
          label={str}
          className={classes.cellStatusBad}
          variant="outlined"
        />
      );
    }
    if (str.toLowerCase() === 'consuming' || str.toLocaleLowerCase() === "partial" || str.toLocaleLowerCase() === "updating" ) {
      return (
        <StyledChip
          label={str}
          className={classes.cellStatusConsuming}
          variant="outlined"
        />
      );
    }
    if (str.toLowerCase() === 'error') {
      return (
        <StyledChip
          label={str}
          className={classes.cellStatusError}
          variant="outlined"
        />
      );
    }
    if (str?.toLowerCase()?.search('partial-') !== -1) {
      return (
        <StyledChip
          label={str?.replace('Partial-','')}
          className={classes.cellStatusConsuming}
          variant="outlined"
        />
      );
    }
    if (str.search('\n') !== -1) {
      return (<pre>{str.toString()}</pre>);
    }
    return (<span>{str.toString()}</span>);
  };

  const [modalStatus, setModalOpen] = React.useState({});
  const handleModalOpen = (rowIndex) => () => setModalOpen({...modalStatus, [rowIndex]: true});
  const handleModalClose = (rowIndex) => () => setModalOpen({...modalStatus, [rowIndex]: false});

  const makeCell = (cellData, rowIndex) => {
    if (Object.prototype.toString.call(cellData) === '[object Object]') {
      // render custom table cell
      if (cellData && cellData.customRenderer) {
        return <>{cellData.customRenderer}</>;
      }

      if (has(cellData, 'component') && cellData.component) {


        let cell = (styleCell(cellData.value))
        let statusModal = (
            <Dialog
                onClose={handleModalClose(rowIndex)}
                open={get(modalStatus, rowIndex, false)}
                fullWidth={true}
                maxWidth={'xl'}
            >
              {cellData.component}
            </Dialog>
        )
        cell = (
            React.cloneElement(
                cell,
                {onClick: handleModalOpen(rowIndex)},
            )
        );
        if (has(cellData, 'tooltip') && cellData.tooltip) {
          cell = (
              <Tooltip
                  title={cellData.tooltip}
                  placement="top"
                  arrow
              >
                {cell}
              </Tooltip>
          )
        };
        return (
            <>
              {cell}
              {statusModal}
            </>
        );
      } else if (has(cellData, 'tooltip') && cellData.tooltip) {
        return (
            <Tooltip
                title={cellData.tooltip}
                placement="top"
                arrow
            >
              {styleCell(cellData.value)}
            </Tooltip>
        );
      } else {
        return styleCell(cellData.value);
      }
    }
    return styleCell(cellData.toString());
  }

  const renderTableComponent = () => {
    return (
      <>
        <TableContainer style={{ maxHeight: isSticky ? 400 : 500 }}>
          <Table className={classes.table} size="small" stickyHeader={isSticky}>
            <TableHead>
              <TableRow>
                {data.columns && data.columns.map((column, index) => (
                  <StyledTableCell
                    className={classes.head}
                    key={index}
                    onClick={() => {
                      if (staticSortFunctions.has(column)) {
                        finalData.sort((a, b) => staticSortFunctions.get(column)(a, b, column, index, order));
                        setFinalData(finalData);
                      } else {
                        setFinalData(orderBy(finalData, column+app_state.columnNameSeparator+index, order ? 'asc' : 'desc'));
                      }
                      setOrder(!order);
                      setColumnClicked(column);
                    }}
                  >
                    <>
                      {
                      tooltipData && tooltipData[index] ?
                        (<Tooltip title={tooltipData[index]} placement="top" arrow><span>{column}</span></Tooltip>)
                      :
                        column
                      }
                      {column === columnClicked ? order ? (
                        <ArrowDropDownIcon
                          color="primary"
                          style={{ verticalAlign: 'middle' }}
                        />
                      ) : (
                        <ArrowDropUpIcon
                          color="primary"
                          style={{ verticalAlign: 'middle' }}
                        />
                      ) : null}
                    </>
                  </StyledTableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody className={classes.body}>
              {isLoading ? <ComponentLoader /> : (
                finalData.length === 0 ? (
                <TableRow>
                  <StyledTableCell
                    className={classes.nodata}
                    colSpan={2}
                  >
                    No Record(s) found
                  </StyledTableCell>
                </TableRow>
              ) : (
                currentlyRenderedRows
                  .map((row, index) => (
                    <StyledTableRow key={index} hover>
                      {Object.values(row).map((cell: any, idx) =>{
                        let url = baseURL;
                        if(regexReplace){
                          const regex = /\:.*?:/;
                          const matches = baseURL.match(regex);
                          url = baseURL.replace(matches[0], row[matches[0].replace(/:/g, '')]);
                        }
                        return addLinks && !idx ? (
                          <StyledTableCell key={idx}>
                            <Link to={`${encodeURI(`${url}${encodeURIComponent(cell)}`)}`}>{cell}</Link>
                          </StyledTableCell>
                        ) : (
                          <StyledTableCell
                            key={idx}
                            className={isCellClickable ? classes.isCellClickable : (isSticky ? classes.isSticky : '')}
                            onClick={() => {cellClickCallback && cellClickCallback(cell);}}
                          >
                            {makeCell(cell ?? '--', index)}
                          </StyledTableCell>
                        );
                      })}
                    </StyledTableRow>
                  ))
              ))}
            </TableBody>
          </Table>
        </TableContainer>
        {finalData.length > 10 ? (
          <TablePagination
            rowsPerPageOptions={[5, 10, 25]}
            component="div"
            count={finalData.length}
            rowsPerPage={rowsPerPage}
            page={page}
            onChangePage={handleChangePage}
            onChangeRowsPerPage={handleChangeRowsPerPage}
            ActionsComponent={TablePaginationActions}
            classes={{ spacer: classes.spacer }}
          />
        ) : null}
      </>
    );
  };

  const renderTable = () => {
    return (
      <>
        <TableToolbar
          name={title}
          showSearchBox={showSearchBox}
          searchValue={search}
          handleSearch={(val: string) => setSearch(val)}
          recordCount={recordsCount}
        />
        {renderTableComponent()}
      </>
    );
  };

  const renderTableInAccordion = () => {
    return (
      <>
        <SimpleAccordion
          headerTitle={title}
          showSearchBox={showSearchBox}
          searchValue={search}
          handleSearch={(val: string) => setSearch(val)}
          recordCount={recordsCount}
          accordionToggleObject={accordionToggleObject}
        >
          {renderTableComponent()}
        </SimpleAccordion>
      </>
    );
  };

  return (
    <div className={highlightBackground ? classes.highlightBackground : classes.root}>
      {inAccordionFormat ?
        renderTableInAccordion()
        :
        renderTable()}
    </div>
  );
}
