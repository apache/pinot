#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

######################################################################
# PY stage that simply does a pip install on our requirements
######################################################################
ARG PY_VER=3.6.9
FROM python:${PY_VER} AS superset-py

RUN mkdir /app \
        && apt-get update -y \
        && apt-get install -y --no-install-recommends \
            build-essential \
            default-libmysqlclient-dev \
            libpq-dev \
        && rm -rf /var/lib/apt/lists/*

# Superset version to build
ARG SUPERSET_VERSION=master
ENV SUPERSET_SRC=/app/superset-src

# Download source
WORKDIR ${SUPERSET_SRC}
RUN wget -qO /tmp/superset.tar.gz https://github.com/apache/incubator-superset/archive/${SUPERSET_VERSION}.tar.gz \
      && tar xzf /tmp/superset.tar.gz -C ${SUPERSET_SRC} --strip-components=1

# First, we just wanna install requirements, which will allow us to utilize the cache
# in order to only build if and only if requirements change
COPY requirements-db.txt /app/
RUN cp ${SUPERSET_SRC}/requirements.txt /app/ \
      && cd /app \
      && pip install --no-cache -r requirements.txt \
      && pip install --no-cache -r requirements-db.txt

######################################################################
# Node stage to deal with static asset construction
######################################################################
FROM node:10-jessie AS superset-node

ARG NPM_BUILD_CMD="build"
ENV BUILD_CMD=${NPM_BUILD_CMD}
ENV SUPERSET_SRC=/app/superset-src

# NPM ci first, as to NOT invalidate previous steps except for when package.json changes
RUN mkdir -p /app/superset-frontend
RUN mkdir -p /app/superset/assets

COPY --from=superset-py ${SUPERSET_SRC}/docker/frontend-mem-nag.sh /
COPY --from=superset-py ${SUPERSET_SRC}/superset-frontend/package* /app/superset-frontend/
RUN /frontend-mem-nag.sh \
        && cd /app/superset-frontend \
        && npm ci

# Next, copy in the rest and let webpack do its thing
COPY --from=superset-py ${SUPERSET_SRC}/superset-frontend /app/superset-frontend
# This is BY FAR the most expensive step (thanks Terser!)
RUN cd /app/superset-frontend \
        && npm run ${BUILD_CMD} \
        && rm -rf node_modules


######################################################################
# Final lean image...
######################################################################
ARG PY_VER=3.6.9
FROM python:${PY_VER} AS lean

ENV SUPERSET_SRC=/app/superset-src

ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    FLASK_ENV=production \
    FLASK_APP="superset.app:create_app()" \
    PYTHONPATH="/app/pythonpath" \
    SUPERSET_HOME="/app/superset_home" \
    SUPERSET_PORT=8080

# Configure environment
ENV GUNICORN_BIND=0.0.0.0:8088 \
    GUNICORN_LIMIT_REQUEST_FIELD_SIZE=0 \
    GUNICORN_LIMIT_REQUEST_LINE=0 \
    GUNICORN_TIMEOUT=60 \
    GUNICORN_WORKERS=3 \
    GUNICORN_THREADS=4
ENV GUNICORN_CMD_ARGS="--workers ${GUNICORN_WORKERS} --threads ${GUNICORN_THREADS} --timeout ${GUNICORN_TIMEOUT} --bind ${GUNICORN_BIND} --limit-request-line ${GUNICORN_LIMIT_REQUEST_LINE} --limit-request-field_size ${GUNICORN_LIMIT_REQUEST_FIELD_SIZE}"

RUN useradd --user-group --no-create-home --no-log-init --shell /bin/bash superset \
        && mkdir -p ${SUPERSET_HOME} ${PYTHONPATH} \
        && apt-get update -y \
        && apt-get install -y --no-install-recommends \
            build-essential \
            default-libmysqlclient-dev \
            libpq-dev \
        && rm -rf /var/lib/apt/lists/*

COPY --from=superset-py /usr/local/lib/python3.6/site-packages/ /usr/local/lib/python3.6/site-packages/
# Copying site-packages doesn't move the CLIs, so let's copy them one by one
COPY --from=superset-py /usr/local/bin/gunicorn /usr/local/bin/celery /usr/local/bin/flask /usr/bin/
COPY --from=superset-node /app/superset/static/assets /app/superset/static/assets
COPY --from=superset-node /app/superset-frontend /app/superset-frontend

## Lastly, let's install superset itself
COPY --from=superset-py ${SUPERSET_SRC}/superset /app/superset
COPY --from=superset-py ${SUPERSET_SRC}/setup.py ${SUPERSET_SRC}/MANIFEST.in ${SUPERSET_SRC}/README.md /app/
RUN cd /app \
        && chown -R superset:superset * \
        && pip install -e .

COPY --from=superset-py ${SUPERSET_SRC}/docker/docker-entrypoint.sh /usr/bin/

COPY bin /usr/local/bin

WORKDIR /app

USER superset

HEALTHCHECK CMD ["curl", "-f", "http://localhost:8088/health"]

EXPOSE ${SUPERSET_PORT}

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]
