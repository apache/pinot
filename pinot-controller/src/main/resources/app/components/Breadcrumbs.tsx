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
import { useLocation, Link as RouterLink, RouteComponentProps } from 'react-router-dom';

import Breadcrumbs from '@material-ui/core/Breadcrumbs';
import Typography from '@material-ui/core/Typography';
import Link, { LinkProps } from '@material-ui/core/Link';
import NavigateNextIcon from '@material-ui/icons/NavigateNext';
import Box from '@material-ui/core/Box';
import { createStyles, makeStyles, Theme } from '@material-ui/core/styles';
import _ from 'lodash';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    root: {
      color : '#fff'
    }
  })
);

interface LinkRouterProps extends LinkProps {
  to: string;
  replace?: boolean;
}

const LinkRouter = (props: LinkRouterProps) => (
  <Link {...props} component={RouterLink} />
);

const breadcrumbNameMap: { [key: string]: string } = {
  '/': 'Home',
  '/query': 'Query Console',
  '/cluster': 'Cluster Manager',
};

const BreadcrumbsComponent = ({ ...props }) => {
  const location = useLocation();
  const pathNames = location.pathname.split('/').filter((x) => x);
  const classes = useStyles();

  const getLabel = (name: string) => {
    return (
      <Typography variant="subtitle2" key={name} className={classes.root}>
        {name}
      </Typography>
    );
  };

  const getClickableLabel = (name: string, link: string) => {
    return (
      <LinkRouter
        underline="none"
        variant="subtitle2"
        to={link}
        key={name}
        className={classes.root}
      >
        {name}
      </LinkRouter>
    );
  };

  const generateBreadcrumb = () => {
    if(!pathNames.length){
      return getLabel(breadcrumbNameMap['/']);
    } else {
      const breadcrumbs = [getClickableLabel(breadcrumbNameMap['/'], '/')];
      const paramsKeys = _.keys(props.match.params);
      if(paramsKeys.length){
        const {tenantName, tableName, segmentName} = props.match.params;
        if(tenantName && tableName){
          breadcrumbs.push(getClickableLabel(tenantName, `/tenants/${tenantName}`));
        }
        if(tenantName && tableName && segmentName){
          breadcrumbs.push(getClickableLabel(tableName, `/tenants/${tenantName}/table/${tableName}`));
        }
        breadcrumbs.push(getLabel(segmentName || tableName || tenantName));
      } else {
        breadcrumbs.push(getLabel(breadcrumbNameMap[location.pathname]));
      }
      return breadcrumbs;
    }
  };

  return (
    <Box marginY="auto" padding="0.25rem 1.5rem" display="flex">
      <Breadcrumbs
        separator={<NavigateNextIcon fontSize="small" style={{ fill: '#fff' }} />}
        aria-label="breadcrumb"
      >
        {generateBreadcrumb()}
      </Breadcrumbs>
    </Box>
  );
}

export default BreadcrumbsComponent;
