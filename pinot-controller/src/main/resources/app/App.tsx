import * as React from 'react';
import * as ReactDOM from 'react-dom';
import { Typography } from '@material-ui/core';

const App = () => {
  return (
    <Typography variant="h1">Hello World</Typography>
  );
};

ReactDOM.render(<App />, document.getElementById('app'));
