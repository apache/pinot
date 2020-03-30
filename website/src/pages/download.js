import React from 'react';

import Layout from '@theme/Layout';

import styles from './download.css';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

function Download() {
  const context = useDocusaurusContext();
  const {siteConfig = {}} = context;

  return (
    <Layout title="Download" description="Download Pinots versions">
      <header className="hero">
        <div className="container container--fluid">
          <h1>Download</h1>
          <div className="hero--subtitle">
          Be sure to verify your downloads by these <a href="https://www.apache.org/info/verification">procedures</a> using these <a href="https://downloads.apache.org/incubator/pinot/KEYS">KEYS</a> for any Apache release.
          </div>

        </div>
      </header>
      <main>
        <section>
          <div className="container">
            <div className="row">
              <div className="col">
                <a href="https://www.apache.org/dyn/closer.lua/incubator/pinot/apache-pinot-incubating-0.1.0/apache-pinot-incubating-0.1.0-bin.tar.gz" className="panel panel--link text--center">
                  <div className="panel--icon">
                    <i className="feather icon-download"></i>
                  </div>
                 
                  <div className="panel--title">0.1.0</div>
                  <a href="https://www.apache.org/dyn/closer.lua/incubator/pinot/apache-pinot-incubating-0.1.0/apache-pinot-incubating-0.1.0-bin.tar.gz" >
                  <div className="panel--title">Official source release</div>
                  </a>
                  <a href="https://downloads.apache.org/incubator/pinot/apache-pinot-incubating-0.1.0/apache-pinot-incubating-0.1.0-bin.tar.gz.sha512" >
                  <div className="panel--subtitle">SHA512</div>
                  </a>
                  <a href="https://downloads.apache.org/incubator/pinot/apache-pinot-incubating-0.1.0/apache-pinot-incubating-0.1.0-bin.tar.gz.asc" >
                  <div className="panel--subtitle">ASC</div>
                  </a>
                  
                </a>
              </div>

              <div className="col">
                <a href="https://www.apache.org/dyn/closer.lua/incubator/pinot/apache-pinot-incubating-0.2.0/apache-pinot-incubating-0.2.0-bin.tar.gz" className="panel panel--link text--center">
                  <div className="panel--icon">
                    <i className="feather icon-download"></i>
                  </div>
                 
                  <div className="panel--title">0.1.0</div>
                  <a href="https://www.apache.org/dyn/closer.lua/incubator/pinot/apache-pinot-incubating-0.2.0/apache-pinot-incubating-0.2.0-bin.tar.gz" >
                  <div className="panel--title">Official source release</div>
                  </a>
                  <a href="https://downloads.apache.org/incubator/pinot/apache-pinot-incubating-0.2.0/apache-pinot-incubating-0.2.0-bin.tar.gz.sha512" >
                  <div className="panel--subtitle">SHA512</div>
                  </a>
                  <a href="https://downloads.apache.org/incubator/pinot/apache-pinot-incubating-0.2.0/apache-pinot-incubating-0.2.0-bin.tar.gz.asc" >
                  <div className="panel--subtitle">ASC </div>
                  </a>
                  
                </a>
              </div>

              <div className="col">
                <a href="https://www.apache.org/dyn/closer.lua/incubator/pinot/apache-pinot-incubating-0.3.0/apache-pinot-incubating-0.3.0-bin.tar.gz" className="panel panel--link text--center">
                  <div className="panel--icon">
                    <i className="feather icon-download"></i>
                  </div>
                 
                  <div className="panel--title">0.1.0</div>
                  <a href="https://www.apache.org/dyn/closer.lua/incubator/pinot/apache-pinot-incubating-0.3.0/apache-pinot-incubating-0.3.0-bin.tar.gz" >
                  <div className="panel--title">Official source release</div>
                  </a>
                  <a href="https://downloads.apache.org/incubator/pinot/apache-pinot-incubating-0.3.0/apache-pinot-incubating-0.3.0-bin.tar.gz.sha512" >
                  <div className="panel--subtitle">SHA512</div>
                  </a>
                  <a href="https://downloads.apache.org/incubator/pinot/apache-pinot-incubating-0.3.0/apache-pinot-incubating-0.3.0-bin.tar.gz.asc" >
                  <div className="panel--subtitle">ASC </div>
                  </a>
                  
                </a>
              </div>
              
            </div>
          </div>

          <div className="container">
            <div className="row">
              <div className="col">
                <a href="https://www.apache.org/dyn/closer.lua/incubator/pinot/apache-pinot-incubating-0.1.0/apache-pinot-incubating-0.1.0-src.tar.gz" className="panel panel--link text--center">
                  <div className="panel--icon">
                    <i className="feather icon-code"></i>
                  </div>
                  <div className="panel--title">0.1.0</div>
                  <a href="https://www.apache.org/dyn/closer.lua/incubator/pinot/apache-pinot-incubating-0.1.0/apache-pinot-incubating-0.1.0-src.tar.gz" >
                  <div className="panel--title">Official source release</div>
                  </a>
                  <a href="https://downloads.apache.org/incubator/pinot/apache-pinot-incubating-0.1.0/apache-pinot-incubating-0.1.0-src.tar.gz.sha512" >
                  <div className="panel--subtitle">SHA512</div>
                  </a>
                  <a href="https://downloads.apache.org/incubator/pinot/apache-pinot-incubating-0.1.0/apache-pinot-incubating-0.1.0-src.tar.gz.asc" >
                  <div className="panel--subtitle">ASC</div>
                  </a>
                </a>
              </div>

              <div className="col">
                <a href="https://www.apache.org/dyn/closer.lua/incubator/pinot/apache-pinot-incubating-0.2.0/apache-pinot-incubating-0.2.0-src.tar.gz" className="panel panel--link text--center">
                  <div className="panel--icon">
                    <i className="feather icon-code"></i>
                  </div>
                  <div className="panel--title">0.2.0</div>
                  <a href="https://www.apache.org/dyn/closer.lua/incubator/pinot/apache-pinot-incubating-0.2.0/apache-pinot-incubating-0.2.0-src.tar.gz" >
                  <div className="panel--title">Official source release</div>
                  </a>
                  <a href="https://downloads.apache.org/incubator/pinot/apache-pinot-incubating-0.2.0/apache-pinot-incubating-0.2.0-src.tar.gz.sha512" >
                  <div className="panel--subtitle">SHA512</div>
                  </a>
                  <a href="https://downloads.apache.org/incubator/pinot/apache-pinot-incubating-0.2.0/apache-pinot-incubating-0.2.0-src.tar.gz.asc" >
                  <div className="panel--subtitle">ASC</div>
                  </a>
                </a>
              </div>

              <div className="col">
                <a href="https://www.apache.org/dyn/closer.lua/incubator/pinot/apache-pinot-incubating-0.3.0/apache-pinot-incubating-0.3.0-src.tar.gz" className="panel panel--link text--center">
                  <div className="panel--icon">
                    <i className="feather icon-code"></i>
                  </div>
                  <div className="panel--title">0.3.0</div>
                  <a href="https://www.apache.org/dyn/closer.lua/incubator/pinot/apache-pinot-incubating-0.3.0/apache-pinot-incubating-0.3.0-src.tar.gz" >
                  <div className="panel--title">Official source release</div>
                  </a>
                  <a href="https://downloads.apache.org/incubator/pinot/apache-pinot-incubating-0.3.0/apache-pinot-incubating-0.3.0-src.tar.gz.sha512" >
                  <div className="panel--subtitle">SHA512</div>
                  </a>
                  <a href="https://downloads.apache.org/incubator/pinot/apache-pinot-incubating-0.3.0/apache-pinot-incubating-0.3.0-src.tar.gz.asc" >
                  <div className="panel--subtitle">ASC</div>
                  </a>
                </a>
              </div>

              </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}

export default Download;
