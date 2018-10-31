# Testing Basics

There are 2 types of testing, manual and automated testing. While some manual testing is required in the end, it is much more productive to rely on automated testing during development and deploy. Automated testing is best at catching programming errors in JavaScript. It is less useful for catching errors in CSS and HTML. It is particularly useful for ensuring a particular regression does not recur or a feature is not inadvertently removed.

It is required that automated tests be written and updated with every review board. In our experience, it is difficult to identify and backfill tests at any later date. While it is recommended that data be mocked and tests written prior to actually writing the code, this is not required. However, in our experience if data is mocked and tests are written first, the code is much more reliable and concise in the end, and code is written in a way that is easier to test. This practice is called Test Driven Development. Sometimes, it is actually faster to throw away code, write tests, and fill the code back in then it is to make tests to fit old code in the first place!

Automated tests can make it possible to easily refactor large amounts of code quickly while being sure that it all still works and fewer bugs introduced. Often, without tests, badly needed refactors are not done simply because we are afraid large amounts of bugs will be introduced. Tests are also a way to document how features should work, rather than how they happened to work at any given time.

There are going to be 4 types of automated tests relevant to UI:

1. [Unit Tests](0300-unit-testing.md)
    - This is useful for testing individual methods and computed properties within a class. These are fast and should represent the majority of tests. Any nontrivial method or computed property should be unit tested.
2. [Integration Tests](0400-integration-testing.md)
    - This is useful for testing a component or helper with DOM context. These are still pretty fast and should be written for every component. These are essential for testing code within a template.
3. [Acceptance Tests](0500-acceptance-testing.md)
    - This is useful for testing an entire workflow against mock data. Most major "happy path" flows should be acceptance tested.
4. [Live Tests](0600-live-testing.md)
    - This is useful for testing an entire workflow against the real api / backend / database. Because these are very slow, they should only be used for basic "smoke tests".

When writing a test, one should focus on what is being tested as much as possible and isolate any other code or factors. This is especially true for unit tests. A test must be:

1. repeatable
    - You should be able to run individual tests in any order or run them twice.
2. reliable
    - They should pass every time or fail every time.
3. readable
    - It should be clear what steps are being followed.
4. maintainable
    - Don't test implementation, only the public facing criteria for the code.
5. specific
    - Don't do logic or calculations or lookups, 4, not 2 + 2
6. fast
    - 'nuff said

Tests have some different characteristics than code. Tests should not follow DRY or have heavy refactoring to minimize code size and maximize code reuse. Particularly, it is better to clearly show the steps in the test than to hide them in other places.

Generally, all tests should run in 5 minutes or less. If this is not the case, developers will run them less often to the detriment of reliable code. To speed up tests, we are using ember exam to run tests in parallel. You can run `ember exam --split <n> --parallel` where n is the number of processes you would like to run the tests much faster on your development machine.

If any of the tests are taking too long to run (more than 15s for acceptance, 1.5 minutes for live):

1. Create a bug in JIRA
2. Set the test to be skipped
3. Add a comment with the JIRA ticket on the test.

Tests should be VERY reliable, or else we will experience a large number of reverts when the tests fail to pass. If a test is known to be flakey:

1. Create a bug in JIRA.
2. Comment out the assertion(s) that are flakey and/or skip the test. Include a comment with the JIRA ticket.

Do not simply erase a flakey test.

Some ways to make tests more reliable:

1. Mock all data. Do not rely on any real data.
2. Use specific mocked data. The default scenario / factories create good fake data for development, but is random.
3. Do not rely on specific ids. Look up the actual id.
4. Do not assume data is created and/or saved in a specific order.
5. Mock dates with sinon.useFakeTimers You can even mock the timezone.
6. Override any function or method with unpredictable results to return something predictable.

Also, don't forget that tests can't cover everything, so some manual testing is required.
