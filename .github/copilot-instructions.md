# Project overview
Originally developed at LinkedIn, Apache PinotTM is a real-time distributed OLAP datastore, 
purpose-built to provide ultra low-latency analytics at extremely high throughput.

With its distributed architecture and columnar storage, Apache Pinot empowers businesses to gain valuable insights from 
real-time data, supporting data-driven decision-making and applications.

## Libraries and Frameworks

- Most source code is written in Java 11.
- Code in `pinot-clients` is written in Java 8.
- Pinot UI is a React.js frontend. It is stored in `pinot-controller/src/main/resources/`.
- The code is compiled with Maven and follows a multimodule structure. Use the `pom.xml` file on each module to 
  understand dependencies and configurations.

## Coding Standards

* Generate header files for all files that you support, including Java, XML, JSON and markdown
* Javadoc comments can either be written using the `/** ... */` syntax or the `///` syntax, 
  as specified in [JEP-467](https://openjdk.org/jeps/467)
* Method and parameters are not null by default, unless specified otherwise with the 
  `javax.annotation.Nullable` annotation