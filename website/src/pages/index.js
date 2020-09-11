// /**
//  * Copyright (c) 2017-present, Facebook, Inc.
//  *
//  * This source code is licensed under the MIT license found in the
//  * LICENSE file in the root directory of this source tree.
//  */

import React, { useState, useEffect } from "react";

import CodeBlock from "@theme/CodeBlock";
import Heading from "@theme/Heading";
import Jump from "@site/src/components/Jump";
import Layout from "@theme/Layout";
import Link from "@docusaurus/Link";
import SVG from "react-inlinesvg";
import TabItem from "@theme/TabItem";
import Tabs from "@theme/Tabs";

import classnames from "classnames";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
// import PinotOverview from '@site/static/img/pinot-overview.svg';

import styles from "./index.module.css";
import "./index.css";

const AnchoredH2 = Heading("h2");

const features = [
    {
        title: "Blazing Fast",
        icon: "zap",
        description: (
            <>
                Pinot is designed to answer OLAP queries with low latency on
                immutable data
            </>
        ),
    },
    {
        title: "Pluggable indexing",
        icon: "unlock",
        description: (
            <>
                Pluggable indexing technologies - Sorted Index, Bitmap Index,
                Inverted Index
            </>
        ),
    },
    {
        title: "Near Real time ingestion",
        icon: "rss",
        description: (
            <>
                Near Realtime ingestion with{" "}
                <Link to="https://kafka.apache.org/">Apache Kafka</Link>{" "}
                supports StringSerializer or{" "}
                <Link to="https://avro.apache.org/">Avro</Link> formats
            </>
        ),
    },
    {
        title: "Horizontally scalable",
        icon: "code",
        description: <>Horizontally scalable and fault tolerant</>,
    },
    {
        title: "Joins using PrestoDB",
        icon: "shuffle",
        description: (
            <>
                Joins are currently not supported, but this problem can be
                overcome by using{" "}
                <Link to="https://prestodb.io/">PrestoDB</Link> for querying
            </>
        ),
    },
    {
        title: "SQL-like Query Interface (PQL)",
        icon: "search",
        description: (
            <>
                SQL like language that supports selection, aggregation,
                filtering, group by, order by, distinct queries on data
            </>
        ),
    },
    {
        title: "Hybrid tables",
        icon: "list",
        description: (
            <>
                Consist of of{" "}
                <Link to="http://pinot.apache.org/img/dynamic-table.png">
                    both offline and realtime table
                </Link>
                . Use realtime table only to cover segments for which offline
                data may not be available yet
            </>
        ),
    },
    {
        title: "Anomaly Detection",
        icon: "bar-chart",
        description: (
            <>
                Run ML Algorithms to detect Anomalies on the data stored in
                Pinot. Use{" "}
                <Link to="https://docs.pinot.apache.org/integrations/thirdeye">
                    ThirdEye
                </Link>{" "}
                with Pinot for Anomaly Detection and Root Cause Analysis
            </>
        ),
    },
    {
        title: "Smart Alerts in ThirdEye",
        icon: "bell",
        description: (
            <>
                Detect the right anomalies by customizing anomaly detect flow
                and notification flow
            </>
        ),
    },
];

function Features({ features }) {
    let rows = [];

    let i,
        j,
        temparray,
        chunk = 3;
    for (i = 0, j = features.length; i < j; i += chunk) {
        let featuresChunk = features.slice(i, i + chunk);

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

function Feature({ icon, title, description }) {
    return (
        <div className={classnames("col col--4", styles.feature)}>
            <div className={styles.featureIcon}>
                <i className={classnames("feather", `icon-${icon}`)}></i>
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
                <AnchoredH2 id="who-uses">Who Uses?</AnchoredH2>
                <div className="sub-title">
                    Pinot powers several big players, including LinkedIn, Uber,
                    Microsoft, Factual, Weibo, Slack and more
                </div>

                <div className={styles.installationPlatforms}>
                    <Link to="https://www.linkedin.com">
                        <SVG src="/img/companies/linkedin.svg" />
                    </Link>
                    <Link to="https://www.ubereats.com">
                        <SVG src="/img/companies/uber.svg" />
                    </Link>
                    <Link to="https://teams.microsoft.com">
                        <SVG src="/img/companies/microsoft-teams.svg" />
                    </Link>
                    <Link to="https://slack.com">
                        <SVG src="/img/companies/slack.svg" />
                    </Link>
                    <Link to="https://www.factual.com">
                        <SVG src="/img/companies/factual.svg" />
                    </Link>
                    <Link to="https://www.weibo.com">
                        <SVG src="/img/companies/weibo.svg" />
                    </Link>
                </div>
            </div>
        </section>
    );
}

function Usage() {
    return (
        <section className="topologies">
            <div className="container">
                <AnchoredH2 id="ingest-query">
                    Ingest and Query Options
                </AnchoredH2>

                <div className="sub-title">
                    Ingest with Kafka, Spark, HDFS or Cloud Storages
                </div>
                <div className="sub-title">
                    Query using PQL(Pinot Query Language ), SQL or
                    Presto(supports Joins)
                </div>

                <SVG
                    src="/img/ingest-query.svg"
                    className="svg image-overview figure"
                />
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
                <div className="sub-title">
                    Pinot can be installed using docker with presto
                </div>

                <div className={styles.installationChecks}>
                    <div>
                        <i className="feather icon-package"></i> Helm or K8s
                        crds
                    </div>
                    <div>
                        <i className="feather icon-cpu"></i> On-Premise
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
                    defaultValue="helm"
                    values={[
                        {
                            label: (
                                <>
                                    <i className="feather icon-download-cloud"></i>{" "}
                                    Using Helm
                                </>
                            ),
                            value: "helm",
                        },
                        {
                            label: (
                                <>
                                    <i className="feather icon-download"></i>{" "}
                                    Using Binary
                                </>
                            ),
                            value: "binary",
                        },
                        {
                            label: (
                                <>
                                    <i className="feather icon-github"></i>{" "}
                                    Build From Source
                                </>
                            ),
                            value: "github",
                        },
                    ]}
                >
                    <TabItem value="helm">
                        <CodeBlock className="language-bash">
                            {`helm repo add pinot https://raw.githubusercontent.com/apache/incubator-pinot/master/kubernetes/helm\nkubectl create ns pinot\nhelm install pinot pinot/pinot -n pinot --set cluster.name=pinot`}
                        </CodeBlock>
                    </TabItem>
                    <TabItem value="binary">
                        <CodeBlock className="language-bash">
                            {`VERSION=0.5.0\nwget https://downloads.apache.org/incubator/pinot/apache-pinot-incubating-$VERSION/apache-pinot-incubating-$VERSION-bin.tar.gz\ntar vxf apache-pinot-incubating-*-bin.tar.gz\ncd apache-pinot-incubating-*-bin\nbin/quick-start-batch.sh`}
                        </CodeBlock>
                    </TabItem>
                    <TabItem value="github">
                        <CodeBlock className="language-bash">
                            {`# Clone a repo\ngit clone https://github.com/apache/incubator-pinot.git\ncd incubator-pinot\n\n# Build Pinot\nmvn clean install -DskipTests -Pbin-dist\n\n# Run the Quick Demo\ncd pinot-distribution/target/apache-pinot-incubating-*-SNAPSHOT-bin/apache-pinot-incubating-*-SNAPSHOT-bin\nbin/quick-start-batch.sh`}
                        </CodeBlock>
                    </TabItem>
                </Tabs>

                <h3 className={styles.installSubTitle}>
                    Or choose your preferred method:
                </h3>

                <div className="row">
                    <div className="col">
                        <Jump to="https://docs.pinot.apache.org/getting-started/running-pinot-in-docker">
                            Containers
                        </Jump>
                    </div>
                    <div className="col">
                        <Jump to="https://docs.pinot.apache.org/getting-started/kubernetes-quickstart">
                            Helm
                        </Jump>
                    </div>
                    <div className="col">
                        <Jump to="https://docs.pinot.apache.org/basics/getting-started/public-cloud-examples">
                            Cloud
                        </Jump>
                    </div>
                    <div className="col">
                        <Jump to="https://docs.pinot.apache.org/getting-started/running-pinot-locally">
                            Manual/Local
                        </Jump>
                    </div>
                </div>
            </div>
        </section>
    );
}

function Home() {
    const context = useDocusaurusContext();
    const { siteConfig = {} } = context;

    return (
        <Layout
            title={`${siteConfig.title}: ${siteConfig.tagline}`}
            description={siteConfig.description}
        >
            <header
                className={classnames(
                    "hero",
                    "hero--full-height",
                    styles.indexHeroBanner
                )}
            >
                <div className="container">
                    <Link
                        to="https://docs.pinot.apache.org/releases/0.5.0"
                        className={styles.indexAnnouncement}
                    >
                        <span className="badge badge-primary">release</span>
                        v0.5.0 has been released! Check the release notes
                    </Link>
                    <h1 className="hero__title">{siteConfig.title}</h1>
                    <p className="hero__subtitle">
                        {siteConfig.tagline}, designed to answer OLAP queries
                        with low latency
                        {/* <Diagram className={styles.indexHeroDiagram} width="100%" /> */}
                        {/* <PinotOverview title="PinotOverview" className="svg image-overview figure" /> */}
                        <SVG
                            src="/img/pinot-overview.svg"
                            className="svg image-overview figure"
                        />
                    </p>
                    <div className="hero--buttons">
                        <Link
                            to="https://docs.pinot.apache.org/getting-started"
                            className="button button--primary button--highlight"
                        >
                            Getting Started
                        </Link>
                        <Link
                            to="https://communityinviter.com/apps/apache-pinot/apache-pinot"
                            className="button button--primary button--highlight"
                        >
                            Join our Slack
                        </Link>
                    </div>
                    <p className="hero--subsubtitle">
                        Pinot is proven at <strong>scale in LinkedIn</strong>{" "}
                        powers 50+ user-facing apps and serving{" "}
                        <strong>100k+ queries</strong>
                    </p>
                </div>
            </header>
            <main>
                {features && features.length && (
                    <Features features={features} />
                )}
                <Usage />
                <WhoUses />
                <Installation />
            </main>
        </Layout>
    );
}

export default Home;
