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

import _ from 'lodash';

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

const tableFormat = (data) => {
  const rows = data.records;
  const header = data.columns;

  const results = [];
  rows.forEach((singleRow) => {
    const obj = {};
    singleRow.forEach((val: any, index: number)=>{
      obj[header[index]] = val;
    });
    results.push(obj);
  });
  return results;
};

const getSegmentStatus = (idealStateObj, externalViewObj) => {
  const idealSegmentKeys = Object.keys(idealStateObj);
  const idealSegmentCount = idealSegmentKeys.length;

  const externalSegmentKeys = Object.keys(externalViewObj);
  const externalSegmentCount = externalSegmentKeys.length;

  if(idealSegmentCount !== externalSegmentCount){
    return 'Bad';
  }

  let segmentStatus = 'Good';
  idealSegmentKeys.map((segmentKey) => {
    if(segmentStatus === 'Good'){
      if( !_.isEqual( idealStateObj[segmentKey], externalViewObj[segmentKey] ) ){
        segmentStatus = 'Bad';
      }
    }
  });
  return segmentStatus;
};

const findNestedObj = (entireObj, keyToFind, valToFind) => {
  let foundObj;
  JSON.stringify(entireObj, (_, nestedValue) => {
    if (nestedValue && nestedValue[keyToFind] === valToFind) {
      foundObj = nestedValue;
    }
    return nestedValue;
  });
  return foundObj;
};

export default {
  sortArray,
  tableFormat,
  getSegmentStatus,
  findNestedObj
};
