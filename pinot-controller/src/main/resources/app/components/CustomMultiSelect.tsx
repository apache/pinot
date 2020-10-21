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

import React, { forwardRef, useEffect, useState } from 'react';
import Chip from '@material-ui/core/Chip';
import Autocomplete, { AutocompleteRenderInputParams } from '@material-ui/lab/Autocomplete';
import TextField from '@material-ui/core/TextField';
import { withStyles } from '@material-ui/core';
import _ from 'lodash';

interface MyInputProps {
  onKeyDown: (event: object) => void;
}
interface MyParams extends AutocompleteRenderInputParams {
  inputProps: MyInputProps;
}

const StyledAutoComplete = withStyles(() => ({
  tag: {
    margin: '10px 5px 5px 0',
    border: '1px solid #4285F4',
    color: '#4285F4',
    '& .MuiChip-deleteIcon': {
      color: 'rgba(66, 133, 244, 0.6)',
      '&:hover':{
        color: 'rgb(66, 133, 244)'
      }
    }
  },
  option:{
    '&[data-focus="true"]': {
      backgroundColor: 'rgba(143, 182, 249, 0.3)',
      color: '#4285F4',
    },
    '&[aria-selected="true"]':{
      backgroundColor: '#4285F4',
      color: 'white'
    },
  },
  inputRoot: {
    paddingTop: '0 !important',
    backgroundColor: 'white !important',
    '&:hover':{
      backgroundColor: 'white !important'
    }
  },
}))(Autocomplete);

type Props = {
  options?: Array<string | Object>,
  value?: Array<string>,
  handleChange: (e: React.ChangeEvent<HTMLInputElement>, value: Array<string>|null) => void,
  error?: {isError: boolean, errorMessage: string}
};

const CustomMultiSelect = forwardRef(({
  options,
  value,
  handleChange,
  error
}: Props, ref) => {
  const [currentValue, setCurrentValue] = useState(value);

  useEffect(() => {
    setCurrentValue(value);
  }, [value]);

  const handleKeyDown = event => {
    switch (event.key) {
      case ",":
      case " ": {
        event.preventDefault();
        event.stopPropagation();
        if (event.target.value.length > 0) {
          handleChange(event, [...value, event.target.value]);
        }
        break;
      }
      default:
    }
  };

  return (
    <StyledAutoComplete
      multiple
      options={options || []}
      value={currentValue}
      freeSolo
      onChange={handleChange}
      renderTags={(values, getTagProps) =>
        values.map((option, index) => (
          <Chip key={index} variant="outlined" label={option} {...getTagProps({ index })} />
        ))}
      renderInput={(params: MyParams) => {{
        params.inputProps.onKeyDown = handleKeyDown;
        return (
        <TextField
          inputRef={ref}
          error={error && error.isError}
          helperText={error && error.errorMessage}
          {...params} variant="filled" placeholder="Enter Tags ..."
        />
      )}}}
    />
  );
});
export default CustomMultiSelect;