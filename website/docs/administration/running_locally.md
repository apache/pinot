---
id: running_locally
title: Running Pinot locally
sidebar_label: Running Pinot locally
---

## Running Pinot locally

Pinot Quickstart local

### Getting Pinot

First, let's get Pinot. You can either build it, or download it

> <b>Prerequisites:</b> <br/> <p>&nbsp; <a href="https://linuxize.com/post/install-java-on-ubuntu-18-04/#installing-openjdk-8" target="_blank">Install Java 8 or higher...</a> </p>

### Build

Follow these steps to checkout code from [Github](https://github.com/apache/incubator-pinot) and build Pinot locally
 
> <b>Prerequisites:</b> <br/> <p>&nbsp; Install <a href="https://maven.apache.org/install.html" target="_blank">Apache Maven</a>  3.6 or higher. </p>

```bash
# checkout pinot
git clone https://github.com/apache/incubator-pinot.git
cd incubator-pinot

# build pinot
mvn install package -DskipTests -Pbin-dist

# navigate to directory containing the setup scripts
cd pinot-distribution/target/apache-pinot-incubating-${pinot.version}-bin/apache-pinot-incubating-${pinot.version}-bin
```

### Download

Download the latest binary release from Apache Pinot, or use this command

```bash
wget https://downloads.apache.org/incubator/pinot/apache-pinot-incubating-${pinot.version}/apache-pinot-incubating-${pinot.version}-bin.tar.gz
```

Once you have the tar file,

```bash
# untar it
tar -zxvf apache-pinot-incubating-${pinot.version}-bin.tar.gz

# navigate to directory containing the launcher scripts
cd apache-pinot-incubating-${pinot.version}-bin
```

If you want to run Pinot using a Docker image instead, head over to Running Pinot in Docker

<Alert type="info">

Pro-tip: These field names can be controlled via the
[global `log_schema` options][docs.reference.global-options#log_schema].

</Alert>