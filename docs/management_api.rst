Managing Pinot via REST API on the Controller
=============================================

*TODO* : Remove this section altogether and find a place somewhere for a pointer to the management API. Maybe in the 'Running pinot in production' section?

There is a REST API which allows management of tables, tenants, segments and schemas. It can be accessed by going to ``http://[controller host]/help`` which offers a web UI to do these tasks, as well as document the REST API.

It can be used instead of the ``pinot-admin.sh`` commands to automate the creation of tables and tenants.
