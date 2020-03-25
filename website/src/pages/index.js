// /**
//  * Copyright (c) 2017-present, Facebook, Inc.
//  *
//  * This source code is licensed under the MIT license found in the
//  * LICENSE file in the root directory of this source tree.
//  */

import React, { useState, useEffect } from 'react';

import CodeBlock from '@theme/CodeBlock';
import Diagram from '@site/src/components/Diagram';
import Heading from '@theme/Heading';
import Jump from '@site/src/components/Jump';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import SVG from 'react-inlinesvg';
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import classnames from 'classnames';
import {fetchNewPost} from '@site/src/exports/newPost';
import {fetchNewRelease} from '@site/src/exports/newRelease';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import repoUrl from '@site/src/exports/repoUrl';
import cloudify from '@site/src/exports/cloudify';

import styles from './index.module.css';
import './index.css';

const AnchoredH2 = Heading('h2');

const features = [
  {
    title: 'Blistering Fast',
    icon: 'zap',
    description: (
      <>
        Pinot is designed to answer OLAP queries with low latency on immutable data
      </>
    ),
  },
  {
    title: 'Pluggable indexing',
    icon: 'unlock',
    description: (
      <>
        Pluggable indexing technologies - Sorted Index, Bitmap Index, Inverted Index
      </>
    ),
  },
  {
    title: 'Near Real time ingestion',
    icon: 'rss',
    description: (
      <>
        Near Realtime ingestion with <Link to="https://kafka.apache.org/">Apache Kafka</Link> supports StringSerializer or <Link to="https://avro.apache.org/">Avro</Link> formats
      </>
    ),
  },
  {
    title: 'Horizontally scalable',
    icon: 'code',
    description: (
      <>
        Horizontally scalable and fault tolerant
      </>
    ),
  },
  {
    title: 'Joins using PrestoDB',
    icon: 'shuffle',
    description: (
      <>
        Joins are currently not supported, but this problem can be overcome by using <Link to="https://prestodb.io/">PrestoDB</Link> for querying
      </>
    ),
  },
  {
    title: 'SQL-like Query Interface (PQL)',
    icon: 'search',
    description: (
      <>
        SQL like language that supports selection, aggregation, filtering, group by, order by, distinct queries on data
      </>
    ),
  },
  {
    title: 'Hybrid tables',
    icon: 'list',
    description: (
      <>
        Consist of of <Link to="/img/dynamic-table.png">both offline and realtime table</Link>. Use realtime table only to cover segments for which offline data may not be available yet
      </>
    ),
  },
  {
    title: 'Anomaly Detection',
    icon: 'bar-chart',
    description: (
      <>
        Run ML Algorithms to detect Anomalies on the data stored in Pinot. Use <Link to="https://docs.pinot.apache.org/integrations/thirdeye">ThirdEye</Link> with Pinot for Anomaly Detection and Root Cause Analysis
      </>
    ),
  },
  {
    title: 'Smart Alerts in ThirdEye',
    icon: 'bell',
    description: (
      <>
        Detect the right anomalies by customizing anomaly detect flow and notification flow
      </>
    ),
  },
];

function Features({features}) {
  let rows = [];

  let i,j,temparray,chunk = 3;
  for (i=0,j=features.length; i<j; i+=chunk) {
    let featuresChunk = features.slice(i,i+chunk);
    
    rows.push(
      <div key={`features${i}`} className="row">
        {featuresChunk.map((props, idx) => (
          <Feature key={idx} {...props} />
        ))}
      </div>
    );
  }

  return (
    <section className={styles.features}>
      <div className="container">
        <AnchoredH2 id="features">Features</AnchoredH2>
        {rows}
      </div>
    </section>
  );
}

function Feature({icon, title, description}) {
  return (
    <div className={classnames('col col--4', styles.feature)}>
      <div className={styles.featureIcon}>
        <i className={classnames('feather', `icon-${icon}`)}></i>
      </div>
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  );
}


function WhoUses() {
  return (
    <section className="topologies">
      <div className="container">
        <AnchoredH2 id="installation">Who Uses?</AnchoredH2>
        <div className="sub-title">Pinot powers several big players, including LinkedIn, Uber, Microsoft, Factual, Weibo, Slack and more</div>

        <div className={styles.installationPlatforms}>
          <Link to="https://www.linkedin.com"><SVG src="/img/companies/linkedin.svg" /></Link>
          <Link to="https://www.ubereats.com"><SVG src="/img/companies/uber.svg" /></Link>
          <Link to="https://teams.microsoft.com"><SVG src="/img/companies/microsoft-teams.svg" /></Link>
          <Link to="https://slack.com"><SVG src="/img/companies/slack.svg" /></Link>
          <Link to="https://www.factual.com"><SVG src="/img/companies/factual.svg" /></Link>
          <Link to="https://www.weibo.com"><SVG src="/img/companies/weibo.svg" /></Link>
        </div>          

      </div>
    </section>

  );
}

function Usage() {
  return (
    <section className="topologies">
      <div className="container">
        <AnchoredH2 id="installation">Ingest and Query Options</AnchoredH2>
        
        <div className="sub-title">Ingest with Kafka, Spark, HDFS or Cloud Storages</div>
        <div className="sub-title">Query using PQL(Pinot Query Language ), SQL or Presto(supports Joins)</div>

        <SVG src="/img/ingest-query.svg" className="svg image-overview figure" />  
      </div>
    </section>
  );
}
// // <SVG src="/img/topologies-distributed.svg" className={styles.topologyDiagram} />


function Installation() {
  return (
    <section className={styles.installation}>
      <div className="container">
        <AnchoredH2 id="installation">Installs Everywhere</AnchoredH2>
        <div className="sub-title">Pinot can be installed using docker with presto</div>

        <div className={styles.installationPlatforms}>
          <Link to="/docs/setup/installation/containers/docker"><SVG src="/img/docker.svg" /></Link>
          <Link to="/docs/setup/installation/operating-systems"><SVG src="/img/linux.svg" /></Link>
          <Link to="/docs/setup/installation/operating-systems/raspbian"><SVG src="/img/raspbian.svg" /></Link>
          <Link to="/docs/setup/installation/operating-systems/windows"><SVG src="/img/windows.svg" /></Link>
          <Link to="/docs/setup/installation/operating-systems/macos"><SVG src="/img/apple.svg" /></Link>
        </div>

        <div className={styles.installationChecks}>
          <div>
            <i className="feather icon-package"></i> Helm or K8s crds
          </div>
          <div>
            <i className="feather icon-cpu"></i> On-Premesis
          </div>
          <div>
            <i className="feather icon-zap"></i> Public Cloud
          </div>
          <div>
            <i className="feather icon-feather"></i> Locally
          </div>
        </div>

        <h3 className={styles.installSubTitle}>Install:</h3>

        <Tabs
          className="mini"
          defaultValue="humans"
          values={[
            { label: <><i className="feather icon-user-check"></i> For Humans</>, value: 'humans', },
            { label: <><i className="feather icon-cpu"></i> For Machines</>, value: 'machines', },
          ]
        }>
          <TabItem value="humans">
            <CodeBlock className="language-bash">
              wget https://www.apache.org/dyn/closer.lua/incubator/pinot/apache-pinot-incubating-0.3.0/apache-pinot-incubating-0.3.0-bin.tar.gz
            </CodeBlock>
          </TabItem>
          <TabItem value="machines">
            <CodeBlock className="language-bash">
            wget https://www.apache.org/dyn/closer.lua/incubator/pinot/apache-pinot-incubating-0.3.0/apache-pinot-incubating-0.3.0-src.tar.gz -y
            </CodeBlock>
          </TabItem>
        </Tabs>

        <h3 className={styles.installSubTitle}>Or choose your preferred method:</h3>

        <div className="row">
          <div className="col">
            <Jump to="https://docs.pinot.apache.org/getting-started/running-pinot-in-docker">Containers</Jump>
          </div>
          <div className="col">
            <Jump to="https://docs.pinot.apache.org/getting-started/kubernetes-quickstart">Helm</Jump>
          </div>
          <div className="col">
            <Jump to="https://docs.pinot.apache.org/getting-started/quickstart">Cloud</Jump>
          </div>
          <div className="col">
            <Jump to="https://docs.pinot.apache.org/getting-started/running-pinot-locally">Manual/Local</Jump>
          </div>
        </div>
      </div>
    </section>
  );
}


function Home() {
  const context = useDocusaurusContext();
  const {siteConfig = {}} = context;


  return (
    <Layout
      title={`${siteConfig.title}: ${siteConfig.tagline}`}
      description={siteConfig.description}>

      <header className={classnames('hero', 'hero--full-height', styles.indexHeroBanner)}>
        <div className="container">
          <Link to="https://docs.pinot.apache.org/releases/0.3.0" className={styles.indexAnnouncement}>
              <span className="badge badge-primary">release</span>
              v0.3 has been released! Check the release notes
          </Link>
          <h1 className="hero__title">{siteConfig.title}</h1>
          <p className="hero__subtitle">{siteConfig.tagline}, designed to answer OLAP queries with low latency
          {/* <Diagram className={styles.indexHeroDiagram} width="100%" /> */}
          
          {/* <img className="image-overview figure" src="img/pinot-overview-light.png" alt="Components overview" /> */}
          <SVG src="/img/pinot-overview.svg" className="svg image-overview figure" />
          </p>
          <div className="hero--buttons">
            <Link to="docs/about/what_is_pinot" className="button button--primary button--highlight">Get Started</Link>
            <Link to="https://communityinviter.com/apps/apache-pinot/apache-pinot" className="button button--primary button--highlight">Join our Slack</Link>
          </div>
          <p className="hero--subsubtitle">
            Pinot is proven at <strong>scale in LinkedIn</strong> powers 50+ user-facing apps and serving <strong>100k+ queries</strong>
          </p>
        </div>
      </header>
      <main>
        {features && features.length && <Features features={features} />}
        <Usage />
        <WhoUses />
        <Installation />
      </main>
    </Layout>
  );
}

export default Home;
