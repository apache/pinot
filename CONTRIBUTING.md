<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# [Contribution Guidelines](https://docs.pinot.apache.org/developers/developers-and-contributors/contribution-guidelines)

Before you begin to contribute, make sure you have reviewed [Dev Environment Setup](https://docs.pinot.apache.org/developers/developers-and-contributors/code-setup) and [Code Modules and Organization](https://docs.pinot.apache.org/developers/developers-and-contributors/code-modules-and-organization) sections and that you have created your own fork of the pinot source code.

> If you wish to contribute to the UI codebase, follow [this guide](/pinot-controller/src/main/resources/Readme.md) to setup UI locally for development.

### Create a design document

If your change is relatively minor, you can skip this step. If you are adding new major feature, we suggest that you add a design document and solicit comments from the community before submitting any code.

[Here](https://cwiki.apache.org/confluence/display/PINOT/Design+Documents) is a list of current design documents.

### Create an issue for the change

Create a Pinot issue [here](https://github.com/apache/pinot/issues) for the change you would like to make. Provide information on why the change is needed and how you plan to address it. Use the conversations on the issue as a way to validate assumptions and the right way to proceed. Be sure to review sections on [Backward and Forward compatibility changes](https://docs.pinot.apache.org/developers/developers-and-contributors/contribution-guidelines#backward-and-forward-compatibility-changes) and [External libraries](https://docs.pinot.apache.org/developers/developers-and-contributors/contribution-guidelines#external-libraries).

If you have a design document, please refer to the design documents in your Issue. You may even want to create multiple issues depending on the extent of your change.

Once you are clear about what you want to do, proceed with the next steps listed below.

### Create a branch for your change

```text
$ cd pinot
#
# ensure you are starting from the latest code base
# the following steps, ensure your fork's (origin's) master is up-to-date
#
$ git fetch upstream
$ git checkout master
$ git merge upstream/master
# create a branch for your issue
$ git checkout -b <your issue branch>
```

Make the necessary changes. If the changes you plan to make are too big, make sure you break it down into smaller tasks.

### Making the changes

Follow the recommendations/best-practices noted here when you are making changes.

#### Code documentation

Please ensure your code is adequately documented. Some things to consider for documentation:

* Always include class level java docs. At the top class level, we are looking for information about what functionality is provided by the class, what state is maintained by the class, whether there are concurrency/thread-safety concerns and any exceptional behavior that the class might exhibit.
* Document public methods and their parameters.

#### Logging

* Ensure there is adequate logging for positive paths as well as exceptional paths. As a corollary to this, ensure logs are not noisy.
* Do not use System.out.println to log messages. Use the `slf4j` loggers.
* Use logging levels correctly: set level to `debug` for verbose logs useful for only for debugging.
* Do not log stack traces via `printStackTrace` method of the exception.

#### Code Formatting
* Ensure that the code you add is properly formatted as per the Pinot style-sheets.
* Pinot's style-sheets can be found in ```pinot/config```.
* For information on how to import the style-sheet into your IDE, refer [here](https://docs.pinot.apache.org/developers/developers-and-contributors/code-setup#setup-ide).

#### Exceptions and Exception-Handling

* Where possible, throw specific exceptions, preferably checked exceptions, so the callers can easily determine what the erroneous conditions that need to be handled are.
* Avoid catching broad exceptions (i.e., `catch (Exception e)` blocks), except for when this is in the `run()` method of a thread/runnable.

Current Pinot code does not strictly adhere to this, but we would like to change this over time and adopt best practices around exception handling.

#### Backward and Forward compatibility changes

If you are making any changes to state stored, either in Zookeeper or in segments, make sure you consider both backward and forward compatibility issues.

* For backward compatibility, consider cases where one component is using the new version and another is still on the old version. E.g., when the request format between broker and server is updated, consider resulting behaviors when a new broker is talking to an older server. Will it break?
* For forward compatibility, consider rollback cases. E.g., consider what happens when state persisted by new code is handled by old code. Does the old code skip over new fields?

#### External libraries

Be cautious about pulling in external dependencies. You will need to consider multiple things when faced with a need to pull in a new library.

* What capability is the addition of the library providing you with? Can existing libraries provide this functionality (may be with a little bit of effort)?
* Is the external library maintained by an active community of contributors?
* What are the licensing terms for the library. For more information about handling licenses, see [License Headers for newly added files](https://docs.pinot.apache.org/developers/developers-and-contributors/contribution-guidelines#license-headers-for-newly-added-files).
* Are you adding the library to [Foundational modules](https://docs.pinot.apache.org/developers/developers-and-contributors/code-modules-and-organization#foundational-modules)? This will affect the rest of the Pinot code base. If the new library pulls in a lot of transitive dependencies, then we might encounter unexpected issues with multiple classes in the classpath. These issues are hard to catch with tests as the order of loading the libraries at runtime matters. If you absolutely need the support, consider adding it via extension modules, see [Extension modules](https://docs.pinot.apache.org/developers/developers-and-contributors/code-modules-and-organization#extension-modules).

#### Testing your changes

Automated tests are always recommended for contributions. Make sure you write tests so that:

1. You verify the correctness of your contribution. This serves as proof to you as well as the reviewers.
2. You future proof your contributions against code refactors or other changes. While this may not always be possible (see [Testing Guidelines](https://docs.pinot.apache.org/developers/developers-and-contributors/contribution-guidelines#testing-guidelines), it's a good goal to aim for.

Identify a list of tests for the changes you have made. Depending on the scope of changes, you may need one or more of the following tests:

* Unit Tests

  Make sure your code has the necessary class or method level unit tests. It is important to write both positive case as well as negative case tests. Document your tests well and add meaningful assertions in the tests; when the assertions fail, ensure that the right messages are logged with information that allows other to debug.

* Integration Tests

  Add integration tests to cover End-to-End paths without relying on _mocking_ (see note below). You `MUST` add integration tests for REST APIs, and must include tests that cover different error codes; i.e., 200 OK, 4xx or 5xx errors that are explicit contracts of the API.

#### Testing Guidelines

* **Mocking**

  Use [Mockito](https://site.mockito.org/) to mock classes to control specific behaviors - e.g., simulate various error conditions.

  **DO NOT** use advanced mock libraries such as [PowerMock](https://github.com/powermock/powermock). They make bytecode level changes to allow tests for static/private members but this typically results in other tools like jacoco to fail. They also promote incorrect implementation choices that make it harder to test additional changes. When faced with a choice to use PowerMock or advanced mocking options, you might either need to refactor the code to work better with mocking or you actually need to write an integration test instead of a unit test.

* **Validate assumptions in tests**

  Make sure that adequate asserts are added in the tests to verify that the tests are passing for the right reasons.

* **Write reliable tests**

  Make sure you are writing tests that are reliable. If the tests depend on asynchronous events to be fired, do not add `sleep` to your tests. Where possible, use appropriate mocking or condition based triggers.

#### License Headers for newly added files

All source code files should have license headers. To automatically add the header for any new file you plan to checkin, run in `pinot` top-level folder:

```text
mvn license:format
```

Note

If you checkin third-party code or files, please make sure you review Apache guidelines:

* [Licences that can be included](https://www.apache.org/legal/resolved.html#what-can-we-include-in-an-asf-project-category-a)
* [Licences that may be included](https://www.apache.org/legal/resolved.html#what-can-we-maybe-include-in-an-asf-project-category-b)
* [Licenses that should not be included](https://www.apache.org/legal/resolved.html#what-can-we-not-include-in-an-asf-project-category-x)

Once you determine the code you are pulling in adhere to the guidelines above, go ahead pull the changes in. Do not add license headers for them. Follow these instructions to ensure we are compliant with Apache Licensing process:

* Under `pinot/licenses` add a LICENSE-&lt;newlib&gt; file that has the license terms of the included library.
* Update the `pinot/LICENSE` file to indicate the newly added library file paths under the corresponding supported Licenses.
* Update the exclusion rules for `license` and `rat` maven plugins in the parent pom: `pinot/pom.xml`.

If attention to the licensing terms in not paid early on, they will be caught much later in the process, when we prepare to make a new release. Updating code at that time to work with the right libraries at that time might require bigger refactoring changes and delay the release process.

### Creating a Pull Request (PR)

* **Verifying code-style**

  Run the following command to verify the code-style before posting a PR

```text
mvn checkstyle:check
```

* **Run tests**

  Before you create a review request for the changes, make sure you have run the corresponding unit tests for your changes. You can run individual tests via the IDE or via maven command-line. Finally run all tests locally by running `mvn clean install -Pbin-dist`.

  For changes that are related to performance issues or race conditions, it is hard to write reliable tests, so we recommend running manual stress tests to validate the changes. You `MUST` note the manual tests done in the PR description.

* **Push changes and create a PR for review**

  Commit your changes with a meaningful commit message.

```text
$ git add <files required for the change>
$ git commit -m "Meaningful oneliner for the change"
$ git push origin <your issue branch>

After this, create a PullRequest in `github <https://github.com/apache/pinot/pulls>`_. Include the following information in the description:

  * The changes that are included in the PR.

  * Design document, if any.

  * Information on any implementation choices that were made.

  * Evidence of sufficient testing. You ``MUST`` indicate the tests done, either manually or automated.

Once the PR is created, the code base is compiled and all tests are run via ``travis``. Make sure you followup on any issues flagged by travis and address them.
If you see test failures that are intermittent, ``please`` create an issue to track them.

Once the ``travis`` run is clear, request reviews from atleast 2 committers on the project and be sure to gently to followup on the issue with the reviewers.
```

* Once you receive comments on github on your changes, be sure to respond to them on github and address the concerns. If any discussions happen offline for the changes in question, make sure to capture the outcome of the discussion, so others can follow along as well.

  It is possible that while your change is being reviewed, other changes were made to the master branch. Be sure to pull rebase your change on the new changes thus:

```text
# commit your changes
$ git add <updated files>
$ git commit -m "Meaningful message for the udpate"
# pull new changes
$ git checkout master
$ git merge upstream/master
$ git checkout <your issue branch>
$ git rebase master

At this time, if rebase flags any conflicts, resolve the conflicts and follow the instructions provided by the rebase command.

Run additional tests/validations for the new changes and update the PR by pushing your changes:
```

```text
$ git push origin <your issue branch>
```

* When you have addressed all comments and have an approved PR, one of the committers can merge your PR.
* After your change is merged, check to see if any documentation needs to be updated. If so, create a PR for documentation.

### Update Documentation

Usually for new features, functionalities, API changes, documentation update is required to keep users up to date and keep track of our development.

Please follow this link to [Update Document](https://docs.pinot.apache.org/developers/developers-and-contributors/update-document) accordingly.
