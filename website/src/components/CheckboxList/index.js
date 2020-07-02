/*
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

import humanizeString from 'humanize-string';

import './styles.css';

function CheckboxList({humanize, icon, values, currentState, setState}) {
  if (values.size == 0)
    return null;

  let valuesArr = Array.from(values)

  return (
    <>
      {valuesArr.map((value, idx) => {
        let label = (typeof value === 'string' && humanize) ? humanizeString(value) : value;

        return (
          <label key={idx}>
            <input
              type="checkbox"
              onChange={(event) => {
                let newValues = new Set(currentState);

                if (event.currentTarget.checked)
                  newValues.add(value);
                else
                  newValues.delete(value);

                setState(newValues);
              }}
              checked={currentState.has(value)} />
            {label && <>{icon ? <i className={`feather icon-${icon}`}></i> : ''} {label}</>}
          </label>
        );
      })}
    </>
  );
}

export default CheckboxList;
