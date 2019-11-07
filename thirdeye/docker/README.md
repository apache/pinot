## How to build a docker image

Usage:

```SHELL
docker build --no-cache -t [Docker Tag] --build-arg PINOT_BRANCH=[PINOT_BRANCH] --build-arg PINOT_GIT_URL=[PINOT_GIT_URL] --build-arg MAVEN_VERSION=[MAVEN_VERSION] -f Dockerfile .
```

Example

```SHELL
docker build --no-cache -t koat/thirdeye --build-arg PINOT_BRANCH=master --build-arg PINOT_GIT_URL=https://github.com/apache/incubator-pinot.git --build-arg MAVEN_VERSION=3.6.2 -f Dockerfile .
```

### How to Run it

#### Bring up dashboard server with default configuration 

```SHELL
docker run -it -p 1426:1426 -p 1427:1427 [Image name]:tag /bin/bash
java -cp "./bin/thirdeye-pinot-1.0-SNAPSHOT.jar" org.apache.pinot.thirdeye.dashboard.ThirdEyeDashboardApplication "./config"
```

#### Bring up backend server with default configuration

```SHELL
docker run -it -p 1426:1426 -p 1427:1427 [Image name]:tag /bin/bash
java -cp "./bin/thirdeye-pinot-1.0-SNAPSHOT.jar" org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyApplication "./config"
```

## Use docker compose 

Example


```SHELL
docker-compose -f docker-compose.yml up
```
