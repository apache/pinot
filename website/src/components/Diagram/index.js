import React, { useState, useEffect } from 'react';
import classnames from 'classnames';
import Link from '@docusaurus/Link';

import './styles.css';

const sources = [
  {
    label: 'Kafka',
    fontSize: 22,
    yPosition: 44,
    url: '/docs/sources/kafka',
  }
]

const sinks = [
  {
    label: 'S3',
    fontSize: 48,
    yPosition: 55,
    xPosition: 12,
    url: '/docs/',
  }
]

function Diagram({className, height, width}) {
  // let shuffledSources = sources.sort(() => 0.5 - Math.random());
  // let selectedSources = shuffledSources.slice(0, 3);

  // let shuffledSinks = sinks.sort(() => 0.5 - Math.random());
  // let selectedSinks = shuffledSinks.slice(0, 3);

  let selectedSources = [sources[1], sources[3], sources[5]];
  let selectedSinks = [sinks[0], sinks[1], sinks[6]];

  const [_, updateState] = useState();
  const defaultXPosition = 7;
  const defaultTextLength = 60;

  useEffect(() => {
    const timeout = setTimeout(() => {
      updateState({});
    }, 100);
    return () => clearTimeout(timeout);
  }, []);

  if (!height) {
    height = "294px";
  }

  if (!width) {
    width = "900px";
  }

  return (
    <svg className={classnames(className, 'diagram')} viewBox={`0 0 683 294`} version="1.1" xmlns="http://www.w3.org/2000/svg">
      <title>Pinot Diagram</title>
      <desc>A lightweight and ultra-fast tool for building observability pipelines</desc>
      <defs>
        <linearGradient x1="50%" y1="0%" x2="50%" y2="100%" id="diagram-gradient-1">
          <stop stopColor="#10E7FF" offset="0%"></stop>
          <stop stopColor="#F44AF5" offset="100%"></stop>
        </linearGradient>
      </defs>
    </svg>
  );
}

export default Diagram;
