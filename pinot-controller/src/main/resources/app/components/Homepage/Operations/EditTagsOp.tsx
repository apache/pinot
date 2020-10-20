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
import { DialogContent} from '@material-ui/core';
import Dialog from '../../CustomDialog';
import CustomMultiSelect from '../../CustomMultiSelect';

type Props = {
  showModal: boolean,
  hideModal: (event: React.MouseEvent<HTMLElement, MouseEvent>) => void,
  saveTags: (event: React.MouseEvent<HTMLElement, MouseEvent>, tag: string) => void,
  tags: Array<string>,
  handleTagsChange: (e: React.ChangeEvent<HTMLInputElement>, value: Array<string>|null) => void,
  error: {isError: boolean, errorMessage: string}
};

export default function CustomModal({
  showModal,
  hideModal,
  saveTags,
  handleTagsChange,
  tags,
  error
}: Props) {

  const tagsRef = React.createRef<HTMLInputElement>();

  return (
    <Dialog
      open={showModal}
      handleClose={hideModal}
      title="Edit Tags"
      handleSave={(event)=>{
        saveTags(event, tagsRef.current.value);
      }}
    >
      <DialogContent>
        <CustomMultiSelect ref={tagsRef} handleChange={handleTagsChange} value={tags} error={error}/>
      </DialogContent>
    </Dialog>
  );
}