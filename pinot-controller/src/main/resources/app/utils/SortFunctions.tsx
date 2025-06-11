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

import {TableSortFunction, DISPLAY_SEGMENT_STATUS} from "Models";
import app_state from "../app_state";


// table sorting requires a 1/-1 result. This helper function helps calculate this
// from any two results.
const valuesToResultNumber = (aRes: any, bRes: any, order: boolean): number => {
    const result = order ? aRes > bRes : aRes < bRes;
    return result ? 1 : -1;
}

export const sortNumberOfSegments: TableSortFunction = (a: any, b: any, column: string, index: number, order: boolean) => {
    const aSegmentInt = parseInt(a[column+app_state.columnNameSeparator+index]);
    const bSegmentInt = parseInt(b[column+app_state.columnNameSeparator+index]);
    return valuesToResultNumber(aSegmentInt, bSegmentInt, order);
}

export const sortBytes: TableSortFunction = (a: any, b: any, column: string, index: number, order: boolean) => {
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    const [aValue, aUnit] = a[column+app_state.columnNameSeparator+index].split(" ");
    const [bValue, bUnit] = b[column+app_state.columnNameSeparator+index].split(" ");
    const aUnitIndex = sizes.indexOf(aUnit);
    const bUnitIndex = sizes.indexOf(bUnit);

    if (sizes.indexOf(aUnit) === sizes.indexOf(bUnit)) {
        return valuesToResultNumber(parseFloat(aValue), parseFloat(bValue), order);
    } else {
        return valuesToResultNumber(aUnitIndex, bUnitIndex, order);
    }
}

export const sortSegmentStatus: TableSortFunction = (a: any, b: any, column: string, index: number, order: boolean) => {
    const statusOrder = {
        [DISPLAY_SEGMENT_STATUS.BAD]: 0,
        [DISPLAY_SEGMENT_STATUS.UPDATING]: 1,
        [DISPLAY_SEGMENT_STATUS.GOOD]: 2
    } as Record<string, number>;
    const getStatusValue = (record: any) => {
        const cell = record[column+app_state.columnNameSeparator+index];
        if (cell && typeof cell === 'object') {
            return cell.value ?? '';
        }
        return cell ?? '';
    };
    const aStatus = statusOrder[getStatusValue(a)] ?? 99;
    const bStatus = statusOrder[getStatusValue(b)] ?? 99;
    return valuesToResultNumber(aStatus, bStatus, order);
}
