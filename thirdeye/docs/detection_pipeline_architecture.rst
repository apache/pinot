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

.. _detection-pipeline-architecture:

######################################
Detection Pipeline Architecture
######################################

This document summarizes the motivation and rationale behind the 2018 refactoring of ThirdEye's anomaly framework library. We describe critical user, dev, and ops pain points, define a list of requirements and discuss our approach to building a scalable and robust detection framework for ThirdEye. We also discuss conscious design trade-offs made in order to facilitate future modification and maintenance of the system.

Motivation
########################
ThirdEye has been adopted by numerous teams for production monitoring of business and system metrics, and ThirdEye's user base grows consistently. With this come challenges of scale and a demand for excellence in development and operation. ThirdEye has outgrown the assumptions and limits of its existing detection framework in multiple dimensions and we need to address technical debt in order to enable the on-boarding of new use-cases and continue to satisfy our existing customers. We experience several pain points with the current detection framework:

Lack of support for business rules and custom work flows
***********************************************************
Anomaly detection workflows have many common components such as the monitoring of different metrics and the drill-down into multiple dimensions of time series. However, each individual detection use-case usually comes with an additional set of business rules to integrate smoothly with established processes at LinkedIn. These typically include cut-off thresholds and fixed lists of sub-dimensions to monitor or ignore, but may extend to custom alerting workflows and the grouping of detected anomalies for different sets of users. ThirdEye's existing detection infrastructure is monolithic and prevents us from encoding business logic in a plug-able way.

Difficult debugging and modification
**************************************
The existing framework has grown over multiple generations of developers with different preferences, designs, and goals. This lead to an inconsistent and undocumented approach to architecture and code design. Worse, existing design documentation is outdated and misleading. This is exacerbated by a lack of testing infrastructure, integration tests, and unit tests. Many assumptions about the behavior of the framework are implicit and only exist in the heads of past maintainers of platform code and algorithms. This makes modification of code extremely difficult as there aren't any ways to estimate the impact of a proposed change to the detection framework before running the whole integrated system on production data.

Tight coupling prevents testing
************************************
Another property of the existing detection code base is tight coupling of individual components and the leaking of state in unexpected places, such as utility functions. It is not currently possible to unit test an individual piece, such as the implementation of an anomaly merger strategy, without setting up the entire system from caching and data layer, over scheduler and worker, to a populated database with pre-existing configuration and anomalies. The untangling of these dependencies is hampered by unnecessarily complex interfaces for anomaly detection functions that blend together aspects of database configuration, detection date ranges, multiple detection modes, merging of detected anomalies, and even UI visualization.

Low performance and limitations to scaling
********************************************
The current implementation of detection, on-boarding, and tuning executes slowly. This is surprising given the modest amount of time series ingested and processed by ThirdEye, especially when considering that Pinot and Autometrics do the heavy lifting of data aggregation and filtering. This can be attributed to the serial retrieval of data points and redundant data base access, part of which is a consequence of the many stacked generations of designs and code. The execution of on-boarding and detection tasks takes several minutes rather than seconds. This leads to regular queue buildup in the system, and also gave rise to unnecessary complexity in the UI to inform users about task progress and its various delays and failure modes. For ThirdEye to scale to support large sets of system metrics or automatically monitor thousands of UMP metrics in parallel these bottlenecks must be eliminated.

Half-baked self-service and configuration capabilities
********************************************************
Configuration currently is specific to the chosen algorithm and no official front-end exists to manually modify detection settings. As a workaround users access and modify configuration directly via a database admin tool, which comes with its own set of dangers and pitfalls. While the database supports JSON serialization, the detection algorithms currently use their own custom string serialization format that is inaccessible even to advanced users. Additionally, the feedback cycle for users from re-configuring an alert (or creating a new one) to ultimately evaluating its detection performance is long and requires manual cleanup and re-triggering of detection by algorithm maintainers.

No ensemble detection or multi-metric dependencies
******************************************************
Some users prefer execution of rules and algorithms in parallel, using algorithms for regular detection but relying on additional rules as fail-safe against false negatives. Also, detection cannot easily incorporate information from multiple metrics without encoding this functionality directly into the algorithm. For example, site-wide impact filters for business metrics are currently part of each separate algorithm implementation rather than modular, re-usable components.

No sandbox support
**********************
On-boarding, performance tuning, and re-configuration of alerts are processes that involve iterative back-testing and to some degree rely on human judgement. In order for users to experiment with detection settings, the execution of algorithms and evaluation of rules must be sandboxed from affecting the state of the production system. Similarly, integration testing of new components may require parallel execution of production and staging environments to build trust after invasive changes. The current tight coupling of detection components in ThirdEye makes this impossible.

Requirements
################
An architecture is only as concise as its requirements. After running ThirdEye for metric monitoring in production for over a year, many original assumptions changed and new requirements came in. In the following we summarize the requirements we deem critical for moving ThirdEye's detection capabilities onto a robust and scalable new foundation.

De-coupling of components
**************************************
Components of the detection framework must be separated to a degree that allows testing of individual units and sandboxed execution of detection workflows. Furthermore, contracts (interfaces) between components should be minimal and should not pre-impose a structure that is modeled after specific use-cases.

Full testability
**************************************
Every single part of the detection pipeline must be testable as a unit as well as in integration with others. This allows us to isolate problems a in individual components and avoid regressions via dedicated tests. We must alos provide test infrastructure to mock required components with simple implementations of existing interfaces. This testability requirement also serves as verification step of our efforts to decouple components. 

Gradual migration via emulation of existing anomaly interface
****************************************************************************
ThirdEye has an existing use-base that has built trust in existing detection methods and tweaked them to their needs, and hence, support for legacy algorithms via an emulation layer is a must-have. It is near impossible to ensure perfect consistency of legacy and emulated execution due to numerous undocumented behavioral quirks. Therefore, the emulation layer will be held to a minimum. Additionally, as we migrate users' workflows to newer implementations this support will be phased out.

Simple algorithms should be simple to build, test, and configure
*************************************************************************
Simple algorithms and rules must be easy to implement, test, and configure. As a platform ThirdEye hosts different types of algorithms and continuously adds more. In order to scale development to both a larger team of developers and collaborators, development of custom workflows and algorithms must be kept as as friction-less as possible.

Support multiple metrics and data sources in single algorithm
*******************************************************************
Several use-cases require information from several metrics and metric-dimensions at once to reliably detect and classify anomalies. Our framework needs native support for this integration of data from multiple sources. This includes multiple metrics, as well as other sources such as external events, past anomalies, etc.

Use-case specific workflows and logic
**************************************
Most detection use-cases bring their own domain-specific business logic. These processes must be encoded into ThirdEye's detection workflows in order to integrate with existing processes at LinkedIn and enable the on-boarding of additional users and teams. This integration of business logic should be possible via configuration options in the majority of cases, but will eventually require additional plug-able code to execute during detection and alerting workflows.

Don't repeat yourself (code and component re-use)
****************************************************
With often similar but not perfectly equal workflows there is a temptation to copy code sequences for existing algorithms and re-use them for new implementations. This redundancy leads to code bloat and the duplication of mistakes and should be avoided to the maximum degree possible. Code re-use via building blocks and utilities correctly designed to be stateless in nature must be a priority.

Consistent configuration of algorithms (top-level and nested)
******************************************************************
The mechanism for algorithm configuration should be uniform across different implementations. This should be true also for nested algorithms. As ThirdEye already uses JSON as serialization format for data base storage, configuration should be stored in a compatible way. While we note JSON is not the best choice for human read-able configuration it is the straight-forward choice given the existing metadata infrastructure.

Stateless, semi-stateful, stateful execution
**********************************************
Algorithms can exist in multiple environments. A stateless sandbox environment, a semi-stateful sandbox environment that has been prepared with data such as pre-existing anomalies, and the production environment in which the database keeps track of results of previous executions. The algorithm implementation should be oblivious to the executing harness to the maximum extent possible.

Interactive detection preview and performance tuning for users
*********************************************************************
As part of the on-boarding workflow and tuning procedure, ThirdEye allows users to tweak settings - either manually or via automated parameter search. This functionality should support interactive replay and preview of detection results in order to help our users build trust in and improve on the detection algorithm or detection rules. This is primarily a performance requirement as is demands execution of newly-generated detection rules at user-interactive speeds. Furthermore, this interactive preview can serve as a test bed for algorithm maintainers and developers to build new algorithms and debug existing ones.

Flow parallelism
***********************
Multiple independent detection flows must execute in parallel and without affecting each other's results. Sub-task level parallelism is out of scope.

Architecture
#################
We split the architecture of ThirdEye in three layers: the execution layer, the framework layer, and the logic layer. The execution layer is responsible for tying in the anomaly detection framework with ThirdEye's existing task execution framework and provide configuration facilities. The framework layer provides an abstraction for algorithm development by providing an unified interface for data and configuration retrieval as well as utilities for common aspects involved in algorithm development. It also provides the contract for the low-level detection pipeline that serves as foundation for any specialized algorithms and pipelines built on top. The logic layer implements business logic and detection algorithms. It also contains the implementation of wrapper pipelines for common tasks such as dimension exploration and anomaly merging that can be used in conjunction with different types of detection detection algorithms.


.. image:: https://user-images.githubusercontent.com/4448437/88264885-670d6500-cc81-11ea-92da-b69073a69e03.png
  :width: 500

Execution layer
**********************
The execution layer ties in the detection framework with ThirdEye's existing task execution framework and data layer. ThirdEye triggers the execution of detection algorithms either time-based (cron job) or on-demand for testing or per on-boarding request from a user. The scheduled execution executes per cron schedule in a stateful manner such that the result of previous executions is available to the algorithm on every run. This component is especially important as it serves most production workloads of ThirdEye. The sandbox executor triggers on demand an can choose to either execute from a clean slate or use a copy past exhaust data generated by the scheduled executor. Results can either be discarded or retained, thus enabling state-less, semi-stateful, and stateful execution. Finally, the test executor runs on-demand with a user-specified input set and allows unit- and integration-testing of higher-level implementations.

Framework layer
*********************
The framework provides an abstraction over various data sources and configuration facilities in ThirdEye and presents a uniform layer to pipeline and algorithm developers. A major aspect of this is the Data Provider, which encapsulates time-series, anomaly, and meta-data access. Furthermore, there are helpers for configuration injection and utilities for common aspects of algorithm development, such as time-series transformations and the data frame API. The framework layer also manages the life cycle of detection algorithms from instantiation, configuration, and execution to result collection and tear down. It's primary contract is the Detection Pipeline, which serves as a foundation of all higher-level abstractions.

Logic layer
*********************
The business logic layer builds on the framework's pipeline contract to implement detection algorithms and specialized pipelines that share functionality across groups of similar algorithms. A special aspect of the business logic layer are wrapper pipelines which enable implementation and configuration of custom detection workflows, such as the exploration of individual dimensions or the domain-specific merging of anomalies with common dimensions. The framework pipelines supports this functionality by supporting nested execution of pipelines with configuration injected from wrapping pipelines and user-configuration.

Design decisions and trade-offs
#####################################

"Simple algorithms should be simple to build" vs "arbitrary workflows should be possible"
***********************************************************************************************
Our detection framework provides a layered pipeline API to balance simplicity and flexibility in algorithm and workflow development. We chose to provide two layers: the raw "DetectionPipeline" and the safer "StaticDetectionPipeline". The raw pipeline layer allows dynamic loading of data and iterative execution of nested code, which enables us to implement arbitrary workflows but comes at the cost higher complexity and placing the burden of performance optimization on the developer. The static pipeline serves as a safe alternative that implements a single round-trip of specifying required data through the algorithm and retrieving the data in one pass from the framework. The static pipeline covers the vast majority of use cases and simplifies algorithm development. We tie both layers together by enabling raw pipelines to seamlessly execute nested algorithms, such as those implemented via the static pipeline interface, thus enabling safe development of algorithms as the nodes of a workflow with arbitrary edges in between them.

"De-coupling" vs "simple infrastructure"
*******************************************
Simplicity and testability stand at the core of the refactoring of the anomaly detection framework. De-coupling of components is strictly necessary to enable unit testing, however, a separation of the framework into dozens of individual components makes the writing of algorithms and test-cases confusing and difficult, especially as it introduces various partial-failure modes. The data provider shows this trade-off between loose coupling and one-stop simplicity: rather than registering individual data connectors and referencing the loosely by name, the data provider uses a statically defined interface to contains accessors to any available type of input data. This clearly specifies the contract, but also requires that new data sources be added via code-change rather than configuration. Additionally, the test infrastructure (the mock provider) must immediately implement this new data source. Should the number of available data sources grow significantly, this design decision should be revisited.

"batch detection" vs "point-wise walk forward"
*************************************************
The detection pipeline contract was designed to handle time ranges rather than single timestamps. This enables batch operations and multi-period detection scenarios but offloads some complexity of implementing walk-forward analysis onto the maintainers of algorithms that perform point-wise anomaly detection. At the current state this is mainly an issue with legacy detection algorithms and we address it by providing a specialized wrapper pipeline that contains a generic implementation of the walk-forward procedure. In case there are several new algorithms that require walk-forward detection, this legacy wrapper should generalized further.

"complex algorithms" vs "performance and scalability"
**********************************************************
Our architecture currently does not enforce any structure on the algorithm implementation besides the specification of inputs and outputs. Specifically, there are no limits on the amount of data that can be requested from the provider. This enables algorithm maintainers to implement algorithms in non-scalable ways, such as re-training the detection model on long time ranges before each evaluation of the detection model. It also doesn't prevent the system (and its data sources) from mistakes of algorthm developers or configuration errors.

Another limitation here is the restriction of parallelism on a per-flow basis. Pipelines and algorithms can contain internal state during execution which is not stored in any external meta data store. This enables algorithm developers to create arbitrary logic, but restricts parallelism to a single serial thread of execution per flow in order to avoid the complexity of synchronization and distributed processing.

"nesting and non-nesting configuration" vs "implicit coupling via property key injection"
**********************************************************************************************
There is a fundamental trade-off between separately configuring individual metric- or dimension-level alerts and setting up a single detector with overrides specific to a single sub-task of detection. Furthermore, this configuration may be injected from a wrapper pipeline down into a nested pipeline. We explicitly chose to use a single, all-encompassing configuration per detection use-case to allow consistent handling of related anomalies in a single flow, for example for merging or clustering. This introduces the need to support configuration overrides in wrapper-to-nested property injection.

"generalized configuration object" vs "static type safety of config"
****************************************************************************
Configuration of pipelines could be served as statically defined config classes or semi-structured (and dynamically typed) key-value maps. Static objects provide type-safety and would allow static checking of configuration correctness. They add overhead for pipeline development and code however. The alternative delays error checking to runtime, i.e. only when the configured pipeline is instantiated and executed. This approach is more lightweight and flexible in terms of development. When the configuration structure changes, static typing introduces increasing overhead for maintaining support for multiple versions of valid configuration objects while semi-structured configuration pushes this issue to the developer of the pipeline. We specifically chose the semi-structured approach.

"atomic execution" vs "redundant computation"
*************************************************
Anomaly detection isn't a purely online process, i.e. detection sometimes changes its decisions about the past state of the world after detection already on this past time range. For example, a new but short short outlier may be ignored by detection initially, but may be re-classified as an anomaly when the following data points are generated and show similar anomalous behavior. ThirdEye's legacy detection pipeline chose to store both "candidates" and "confirmed" anomalies in the data base. This initially removed the need to re-compute candidate anomalies for merging and re-classification since they could all be retrieved from the database. However, depending on the part of the application only part of all these anomalies were surface which lead to inconsistent handling in back- and front-end and confused developers and users alike. Furthermore, storing all potential anomalies lead to extreme database bloat (400%+) and the intended compute savings did not materialize as back-filled data and run time errors in detection tasks made cleanup and re-computation of anomalies necessary anyways. The new detection framework explicitly chooses an atomic execution model where anomalies either materialize or not. This comes at the cost of having to re-compute a longer time period (several days) in addition to any newly added time range to find all potential candidates for merging. Given that all current algorithms already retrieve several weeks worth of training data for each detection run this overhead is negligible.

"serial execution, custom and re-usable wrappers" vs "parallel execution pipeline parts"
*********************************************************************************************
Parallelism in ThirdEye performs on job level, but not per task. This allows users to specify arbitrary flows of exploration, detection, merging, and grouping tasks as all the state is available in a single place during execution (see atomic execution above). The trade-off here comes from a limit to scaling of extremely large singular detection flows that cannot execute serially. This can be mitigated by splitting the job into multiple independent ones, effectively allowing the user to choose between execution speed and flow complexity.


