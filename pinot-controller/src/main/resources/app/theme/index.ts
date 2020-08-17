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

import { createMuiTheme } from '@material-ui/core/styles';

import primary from './color/primary';
import secondary from './color/secondary';
import typography from './typography';

const theme = createMuiTheme({
  palette: {
    common: {
      black: '#000',
      white: '#fff',
    },
    primary: {
      light: primary[50],
      main: primary[500],
      dark: primary[900],
      contrastText: '#fff',
    },
    secondary: {
      light: secondary[50],
      main: secondary[500],
      dark: secondary[900],
      contrastText: '#fff',
    },
    text: {
      primary: 'rgba(0, 0, 0, 0.87)',
      disabled: 'rgba(0, 0, 0, 0.38)',
      hint: 'white',
    },
    background: {
      default: '#ffffff',
      paper: '#fff',
    },
  },
  typography,
});
// TODO: remove all StyledButton usages
export default theme;
