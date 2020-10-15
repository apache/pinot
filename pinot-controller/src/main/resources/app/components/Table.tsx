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
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableContainer from '@material-ui/core/TableContainer';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import { TablePagination } from '@material-ui/core';
import { TableData } from 'Models';
import IconButton from '@material-ui/core/IconButton';
import FirstPageIcon from '@material-ui/icons/FirstPage';
import KeyboardArrowLeft from '@material-ui/icons/KeyboardArrowLeft';
import KeyboardArrowRight from '@material-ui/icons/KeyboardArrowRight';
import LastPageIcon from '@material-ui/icons/LastPage';
import ArrowDropDownIcon from '@material-ui/icons/ArrowDropDown';
import ArrowDropUpIcon from '@material-ui/icons/ArrowDropUp';
import { NavLink } from 'react-router-dom';
import Chip from '@material-ui/core/Chip';
import _ from 'lodash';
import Utils from '../utils/Utils';
import TableToolbar from './TableToolbar';
import SimpleAccordion from './SimpleAccordion';

type Props = {
  title?: string,
  data: TableData,
  noOfRows?: number,
  addLinks?: boolean,
  isPagination?: boolean,
  cellClickCallback?: Function,
  isCellClickable?: boolean,
  highlightBackground?: boolean,
  isSticky?: boolean,
  baseURL?: string,
  recordsCount?: number,
  showSearchBox: boolean,
  inAccordionFormat?: boolean,
  regexReplace?: boolean
};

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
  cellSatusGood: {
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
  noOfRows,
  addLinks,
  isPagination,
  cellClickCallback,
  isCellClickable,
  highlightBackground,
  isSticky,
  baseURL,
  recordsCount,
  showSearchBox,
  inAccordionFormat,
  regexReplace
}: Props) {
  const [finalData, setFinalData] = React.useState(Utils.tableFormat(data));

  const [order, setOrder] = React.useState(false);
  const [columnClicked, setColumnClicked] = React.useState('');

  const classes = useStyles();
  const [rowsPerPage, setRowsPerPage] = React.useState(noOfRows || 10);
  const [page, setPage] = React.useState(0);

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
      setFinalData(finalData);
    } else {
      const filteredRescords = data.records.filter((record) => {
        const searchFound = record.find(
          (cell) => cell.toString().toLowerCase().indexOf(str) > -1
        );
        if (searchFound) {
          return true;
        }
        return false;
      });
      setFinalData(filteredRescords);
    }
  }, [data, setFinalData]);

  React.useEffect(() => {
    clearTimeout(timeoutId.current);
    timeoutId.current = setTimeout(() => {
      filterSearchResults(search.toLowerCase());
    }, 200);

    return () => {
      clearTimeout(timeoutId.current);
    };
  }, [search, timeoutId, filterSearchResults]);

  const styleCell = (str: string) => {
    if (str === 'Good' || str.toLowerCase() === 'online' || str.toLowerCase() === 'alive') {
      return (
        <StyledChip
          label={str}
          className={classes.cellSatusGood}
          variant="outlined"
        />
      );
    }
    if (str === 'Bad' || str.toLowerCase() === 'offline' || str.toLowerCase() === 'dead') {
      return (
        <StyledChip
          label={str}
          className={classes.cellStatusBad}
          variant="outlined"
        />
      );
    }
    if (str.toLowerCase() === 'consuming') {
      return (
        <StyledChip
          label={str}
          className={classes.cellStatusConsuming}
          variant="outlined"
        />
      );
    }
    return str.toString();
  };

  const renderTableComponent = () => {
    return (
      <>
        <TableContainer style={{ maxHeight: isSticky ? 400 : 500 }}>
          <Table className={classes.table} size="small" stickyHeader={isSticky}>
            <TableHead>
              <TableRow>
                {data.columns.map((column, index) => (
                  <StyledTableCell
                    className={classes.head}
                    key={index}
                    onClick={() => {
                      setFinalData(_.orderBy(finalData, column, order ? 'asc' : 'desc'));
                      setOrder(!order);
                      setColumnClicked(column);
                    }}
                  >
                    {column}
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
                  </StyledTableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody className={classes.body}>
              {finalData.length === 0 ? (
                <TableRow>
                  <StyledTableCell
                    className={classes.nodata}
                    colSpan={2}
                  >
                    No Record(s) found
                  </StyledTableCell>
                </TableRow>
              ) : (
                finalData
                  .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                  .map((row, index) => (
                    <StyledTableRow key={index} hover>
                      {Object.values(row).map((cell, idx) =>{
                        let url = baseURL;
                        if(regexReplace){
                          const regex = /\:.*?:/;
                          const matches = baseURL.match(regex);
                          url = baseURL.replace(matches[0], row[matches[0].replace(/:/g, '')]);
                        }
                        return addLinks && !idx ? (
                          <StyledTableCell key={idx}>
                            <NavLink
                              className={classes.link}
                              to={`${url}${cell}`}
                            >
                              {cell}
                            </NavLink>
                          </StyledTableCell>
                        ) : (
                          <StyledTableCell
                            key={idx}
                            className={isCellClickable ? classes.isCellClickable : (isSticky ? classes.isSticky : '')}
                            onClick={() => {cellClickCallback && cellClickCallback(cell);}}
                          >
                            {styleCell(cell.toString())}
                          </StyledTableCell>
                        );
                      })}
                    </StyledTableRow>
                  ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
        {isPagination && finalData.length > 10 ? (
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
