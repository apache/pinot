..
.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.
..

.. _couchbase:

Couchbase as a Centralized Cache
=================================

Intro
------

`Couchbase <https://www.couchbase.com/>`__ is a distributed NoSQL data store. It’s a document store, which means that everything is stored as a “document”, or in JSON-format. An example document looks like this:

.. code-block:: none

	{
		“key”: “value”,
		“key2”: “value2”,
		“key3”: [ <arr val1>, <arr val2>, <arr val3>, …]
		“key4”: {
			“nested key1”: “value”,
			“nested key2”: “value2”,
			...
		}
		...
	}

Couchbase is “schema-less”, meaning that it doesn’t have a strict schema like traditional SQL. In essence, this means that you can put whatever you want in your document, 
and you can have multiple types of documents in the same “bucket” (this term will be explained later).

Terminology
------------

* **bucket** - Think of this as a “database” in a relational SQL database, except there’s no tables. All of your documents go in the bucket. It’s like a container where all your data lives.
* **host** - a Couchbase node
* **N1QL** - Couchbase’s SQL-like query language.
* **index** - a data-structure that provides quick and efficient means to query and access data, that would otherwise require scanning a lot more documents.

Setting up Couchbase as a Cache
--------------------------------

ThirdEye comes with the ability to use Couchbase as a centralized cache bundled with it, but there are a few steps to complete before we can get fully connected.

If you already have a Couchbase cluster or have already set up Couchbase Server locally, you can skip ahead to step

#. Download and setup Couchbase Server locally, or on your server/cluster machines: `<https://www.couchbase.com/downloads>`_

#. Go to the Couchbase admin console (for local installations, this will be default to http://localhost:8091).

	#. Create a bucket. The name can be whatever you want. This is where your data will live.
	#. Create a user (Security -> Add User). 
		* Note that if you plan to use certificate-based authentication (Couchbase Server Enterprise Edition only), the naming of your user may be important. Ask your local sysadmin if not sure.
	#. Give your user query access on your bucket. Click your user, then click Edit, then under "Roles" find your bucket. Check "Application Access" and all of the boxes under "Query and Index Services". 
	#. Go to the query console ("Query" on the left sidebar) and run the following commands, which will create indexes and make ThirdEye's queries fast.

		.. code-block:: sql
			
			CREATE INDEX `timestamp_idx` ON `<your bucket name>`(`timestamp`);

		.. code-block:: sql

			CREATE INDEX `metricId_idx` ON `<your bucket name>`(`metricId`);

		Optional:		

		.. code-block:: sql

			CREATE INDEX `timestampASC_idx` ON `<your bucket name>`(`timestamp` ASC);

		.. code-block:: sql

			CREATE INDEX `timestampASC2_idx` ON `<your bucket name>`(-`timestamp`);

		Theoretically, the last two queries should create the same index (timestamp ordered ascendingly), but results have varied in local testing.

#. Modify cache-config.yml (found in the data-sources folder) to fit whatever authentication scheme you're using. The exact meanings of each config setting is explained in the next section.

#. Enable use of Couchbase by setting the "useCentralizedCache" setting in cache-config.yml to be "true".

#. Start up ThirdEye and make sure that ThirdEye can connect correctly. Look for a log message "Caught exception while initializing centralized cache - reverting to default settings". If you don't find that message, everything is good, or you forgot to set "useCentralizedCache" to true.

Cache Config Settings Explained
--------------------------------

The config file for Couchbase is found in cache-config.yml. These are the settings that ThirdEye comes with by default, but you can add more if you feel like you need them. You may need to add these to the code in the CouchbaseCacheDAO 
For other settings in cache-config.yml, see :ref:`cache-config.yml`.

Couchbase specific settings:

* **useCertificateBasedAuthentication** - whether we should authenticate using certificates (Enterprise Edition only). True implies yes, false implies using username/password based authentication instead.
* **hosts** - list of hosts to connect to. For local setup. this will just be 'http://localhost:8091'.
* **bucketName** -  name of your bucket, created during setup.
* **enableDnsSrv** - toggle for whether to use DNS Srv - Couchbase client will just fallback to regular bootstrapping if this fails, so it doesn't really matter too much.

Username/Password authentication settings -- not relevant if using certificate-based authentication

* **authUsername** - username for username/password based auth, not relevant if using certificate based auth. Set this if you are using username/password based authentication.
* **authPassword** - same as above but for password

Certificate-based authentication settings -- only relevant if you have Couchbase Server Enterprise Edition and want to use certificate-based authentication

* **keyStoreFilePath** - path to keystore file (identity.p12)
* **keyStorePassword** - keystore file’s password, if it has one. If not, use  'work_around_jdk-6879539' which is how Java handles empty/no passwords for certificates.
* **trustStoreFilePath** - Path to trust store file - search for something like 'cacerts' or talk to your sysadmin.
* **trustStorePassword** - Password for trust store file, if there is one. If not, empty string or null is fine.

You only need to use one authentication method, either username/password or certificates. Either will work.
