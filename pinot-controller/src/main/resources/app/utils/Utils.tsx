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

import React from 'react';
import ReactDiffViewer, {DiffMethod} from 'react-diff-viewer';
import { map, isEqual, findIndex, findLast } from 'lodash';
import app_state from '../app_state';
import {DISPLAY_SEGMENT_STATUS, SEGMENT_STATUS, TableData} from 'Models';

const sortArray = function (sortingArr, keyName, ascendingFlag) {
  if (ascendingFlag) {
    return sortingArr.sort(function (a, b) {
      if (a[keyName] < b[keyName]) {
        return -1;
      }
      if (a[keyName] > b[keyName]) {
        return 1;
      }
      return 0;
    });
  }
  return sortingArr.sort(function (a, b) {
    if (b[keyName] < a[keyName]) {
      return -1;
    }
    if (b[keyName] > a[keyName]) {
      return 1;
    }
    return 0;
  });
};

const tableFormat = (data: TableData): Array<{ [key: string]: any }> => {
  const rows = data.records;
  const header = data.columns;

  const results: Array<{ [key: string]: any }> = [];
  rows.forEach((singleRow) => {
    const obj: { [key: string]: any } = {};
    singleRow.forEach((val: any, index: number) => {
      obj[header[index]+app_state.columnNameSeparator+index] = val;
    });
    results.push(obj);
  });
  return results;
};

const getSegmentStatus = (idealStateObj, externalViewObj) => {
  const tableStatus = getDisplayTableStatus(idealStateObj, externalViewObj);
  const statusMismatchDiffComponent = (
    <ReactDiffViewer
      oldValue={JSON.stringify(idealStateObj, null, 2)}
      newValue={JSON.stringify(externalViewObj, null, 2)}
      splitView={true}
      showDiffOnly={true}
      leftTitle={"Ideal State"}
      rightTitle={"External View"}
      compareMethod={DiffMethod.WORDS}
    />
  );

  if(tableStatus === DISPLAY_SEGMENT_STATUS.BAD) {
    return ({
      value: tableStatus,
      tooltip: "One or more segments in this table are in bad state. Click the status to view more details.",
      component: statusMismatchDiffComponent,
    })
  }

  if(tableStatus === DISPLAY_SEGMENT_STATUS.UPDATING) {
    return ({
      value: tableStatus,
      tooltip: "One or more segments in this table are in updating state. Click the status to view more details.",
      component: statusMismatchDiffComponent,
    })
  }

  return ({
    value: tableStatus,
    tooltip: "All segments in this table are in good state.",
  });
};

const findNestedObj = (entireObj, keyToFind, valToFind) => {
  let foundObj;
  JSON.stringify(entireObj, (a, nestedValue) => {
    if (nestedValue && nestedValue[keyToFind] === valToFind) {
      foundObj = nestedValue;
    }
    return nestedValue;
  });
  return foundObj;
};

const generateCodeMirrorOptions = (array, type, modeType?) => {
  const arr = [];
  // eslint-disable-next-line no-shadow
  const nestedFields = (arrayList, type, level, oldObj?) => {
    map(arrayList, (a) => {
      const obj = {
        text: a.displayName || a.name || a,
        displayText: a.displayName || a.name || a,
        filterText: oldObj
          ? `${oldObj.filterText}.${a.displayName || a.name || a}`
          : a.displayName || a.name || a,
        render: (el, cm, data) => {},
        className:
          type === 'FUNCTION'
            ? 'codemirror-func'
            : type === 'SQL'
              ? 'codemirror-sql'
              : type === 'BINARY-OPERATORS'
                ? 'codemirror-Operators'
                : type === 'TABLE'
                  ? 'codemirror-table'
                  : 'codemirror-column'
      };
      obj[type === 'FUNCTION' ? 'returnType' : 'type'] =
        type === 'FUNCTION'
          ? a.returnType
          : type === 'SQL'
            ? 'SQL'
            : type === 'BINARY-OPERATORS'
              ? 'Binary Operators'
              : a.type;
      obj.render = (el, cm, data) => {
        codeMirrorOptionsTemplate(el, data);
      };

      if (oldObj === undefined) {
        arr.push(obj);
      } else {
        const index = findIndex(
          arr,
          (n) => n.filterText === oldObj.filterText
        );
        if (index !== -1) {
          const name = obj.displayText;
          if (modeType === 'sql') {
            obj.displayText = `${oldObj.displayText}.${name}`;
            obj.text = `${oldObj.text}.${name}`;
          } else {
            obj.displayText = name;
            obj.text = name;
          }
          obj.filterText = `${oldObj.text}.${name}`;
          if (arr[index].fields) {
            arr[index].fields.push(obj);
          } else {
            arr[index].fields = [];
            arr[index].fields.push(obj);
          }
        } else {
          const indexPath = getNestedObjPathFromList(arr, oldObj);
          if (indexPath && indexPath.length) {
            pushNestedObjectInArray(indexPath, obj, arr, modeType);
          }
        }
      }
      if (a.fields) {
        nestedFields(a.fields, type, level + 1, obj);
      }
    });
    return arr;
  };
  return nestedFields(array, type, 0);
};

const pushNestedObjectInArray = (pathArr, obj, targetList, modeType) => {
  const rollOverFields = (target) => {
    map(target, (list) => {
      if (pathArr === list.filterText) {
        if (modeType === 'sql') {
          obj.displayText = `${pathArr}.${obj.displayText}`;
          obj.text = `${pathArr}.${obj.text}`;
        } else {
          obj.displayText = obj.displayText;
          obj.text = obj.text;
        }
        obj.filterText = `${pathArr}.${obj.text}`;
        if (list.fields) {
          list.fields.push(obj);
        } else {
          list.fields = [];
          list.fields.push(obj);
        }
      } else if (list.fields) {
        rollOverFields(list.fields);
      }
    });
  };
  rollOverFields(targetList);
};

const getNestedObjPathFromList = (list, obj) => {
  const str = [];
  const recursiveFunc = (arr, level) => {
    map(arr, (a) => {
      if (a.fields) {
        str.push(a.filterText);
        recursiveFunc(a.fields, level + 1);
      } else if (obj.filterText === a.filterText) {
        str.push(a.filterText);
      }
    });
    return findLast(str);
  };
  return recursiveFunc(list, 0);
};

const codeMirrorOptionsTemplate = (el, data) => {
  const text = document.createElement('div');
  const fNameSpan = document.createElement('span');
  fNameSpan.setAttribute('class', 'funcText');
  fNameSpan.innerHTML = data.displayText;

  text.appendChild(fNameSpan);
  el.appendChild(text);

  // data.returnType is for UDF Function ||  data.type is for Fields
  if (data.returnType || data.type) {
    const returnTypetxt = document.createElement('div');
    returnTypetxt.setAttribute('class', 'fieldText');
    const content = data.returnType
      ? `Return Type: ${data.returnType}`
      : `Type: ${data.type}`;
    returnTypetxt.innerHTML = content;
    el.appendChild(returnTypetxt);
  }
};

const serialize = (obj: any, prefix?: any) => {
  let str = [], p;
  for (p in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, p)) {
      var k = prefix ? prefix + "[" + p + "]" : p,
        v = obj[p];
      str.push((v !== null && typeof v === "object") ?
        serialize(v, k) :
        encodeURIComponent(k) + "=" + encodeURIComponent(v));
    }
  }
  return str.join("&");
};

const navigateToPreviousPage = (location, popTwice) => {
  const hasharr = location.pathname.split('/');
  hasharr.pop();
  if(popTwice){
    hasharr.pop();
  }
  return hasharr.join('/');
};

const syncTableSchemaData = (data, showFieldType) => {
  const dimensionFields = data.dimensionFieldSpecs || [];
  const metricFields = data.metricFieldSpecs || [];
  const dateTimeField = data.dateTimeFieldSpecs || [];

  dimensionFields.map((field) => {
    field.fieldType = 'Dimension';
  });

  metricFields.map((field) => {
    field.fieldType = 'Metric';
  });

  dateTimeField.map((field) => {
    field.fieldType = 'Date-Time';
  });
  const columnList = [...dimensionFields, ...metricFields, ...dateTimeField];
  if (showFieldType) {
    return {
      columns: ['Column', 'Type', 'Field Type', 'Multi Value'],
      records: columnList.map((field) => {
        return [field.name, field.dataType, field.fieldType, getMultiValueField(field)];
      }),
    };
  }
  return {
    columns: ['Column', 'Type'],
    records: columnList.map((field) => {
      return [field.name, field.dataType];
    }),
  };
};

const getMultiValueField = (field): boolean => {
  if(!field) {
    return false;
  }

  if("singleValueField" in field && field.singleValueField === false) {
    return true;
  }

  return false;
}

const encodeString = (str: string) => {
  if(str === unescape(str)){
    return escape(str);
  }
  return str;
}

const formatBytes = (bytes, decimals = 2) => {
  if (bytes === 0) return '0 Bytes';

  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

const splitStringByLastUnderscore = (str: string) => {
  if (!str.includes('_')) {
    return [str, ''];
  }
  let beforeUnderscore = str.substring(0, str.lastIndexOf("_"));
  let afterUnderscore = str.substring(str.lastIndexOf("_") + 1, str.length);
  return [beforeUnderscore, afterUnderscore];
}

export const getDisplayTableStatus = (idealStateObj, externalViewObj): DISPLAY_SEGMENT_STATUS => {
  const segmentStatusArr = [];
  Object.keys(idealStateObj).forEach((key) => {
    segmentStatusArr.push(getDisplaySegmentStatus(idealStateObj[key], externalViewObj[key]))
  })

  if(segmentStatusArr.includes(DISPLAY_SEGMENT_STATUS.BAD)) {
    return DISPLAY_SEGMENT_STATUS.BAD;
  }
  if(segmentStatusArr.includes(DISPLAY_SEGMENT_STATUS.UPDATING)) {
    return DISPLAY_SEGMENT_STATUS.UPDATING;
  }
  return DISPLAY_SEGMENT_STATUS.GOOD;
}

export const getDisplaySegmentStatus = (idealState, externalView): DISPLAY_SEGMENT_STATUS => {
  const externalViewStatesArray = Object.values(externalView || {});

  // if EV contains ERROR state then segment is in Bad state
  if(externalViewStatesArray.includes(SEGMENT_STATUS.ERROR)) {
    return DISPLAY_SEGMENT_STATUS.BAD;
  }

  // if EV status is CONSUMING or ONLINE then segment is in Good state
  if(externalViewStatesArray.every((status) => status === SEGMENT_STATUS.CONSUMING || status === SEGMENT_STATUS.ONLINE) && isEqual(idealState, externalView)) {
    return DISPLAY_SEGMENT_STATUS.GOOD;
  }

  // If EV state is OFFLINE and EV matches IS then segment is in Good state.
  if(externalViewStatesArray.includes(SEGMENT_STATUS.OFFLINE) && isEqual(idealState, externalView)) {
    return DISPLAY_SEGMENT_STATUS.GOOD;
  }

  // If EV is empty or EV state is OFFLINE and does not matches IS then segment is in Partial state.
  // PARTIAL state can also be interpreted as we're waiting for segments to converge
  if(externalViewStatesArray.length === 0 || externalViewStatesArray.includes(SEGMENT_STATUS.OFFLINE) && !isEqual(idealState, externalView)) {
    return DISPLAY_SEGMENT_STATUS.UPDATING;
  }

  // does not match any condition -> assume PARTIAL state as we are waiting for segments to converge 
  return DISPLAY_SEGMENT_STATUS.UPDATING;
}

export default {
  sortArray,
  tableFormat,
  getSegmentStatus,
  findNestedObj,
  generateCodeMirrorOptions,
  serialize,
  navigateToPreviousPage,
  syncTableSchemaData,
  encodeString,
  formatBytes,
  splitStringByLastUnderscore
};
