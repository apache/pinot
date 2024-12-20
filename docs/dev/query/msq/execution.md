# Multi stage query execution

Once a query string is transformed into a `MultiStageOperator` (see [Query tree lifecycle](tree-lifecycle.md)) it
is ready to be executed.
This document provides an overview of how the execution of a multi-stage query works and the classes involved in it. 

## Execution

The execution of each stage is done in parallel.
Right now we use an unbouded thread pool to execute the stages.
This can starve other parts of Pinot and allocate too much memory on high QPS scenarios, but it is a simple solution
for possible blocks produced by inter-stage dependencies.
We are planning to change this to better (but more complex) solutions in the future.

During the execution each worker executes their view of the stage in a single thread fashion.
This means that the implementation of `MultiStageOperator` can assume that it is executed by a single thread.
The most important method in these operators is `nextBlock`.
The execution is done in a pull based fashion, where the worker calls `nextBlock` on the operator in the root of
the stage and then the operator calls `nextBlock` on its children as needed.

Each call to `nextBlock` is guaranteed to return a non-null block, which may contain data or be a block that signals
the end of the stream, which contains metadata like errors or stats.
See `TransferableBlock` to learn more about these blocks.

Some special operators are `MailboxReceiveOperator`, which fetches data from other workers and `MailboxSendOperator`,
which does the opposite.
Every stage has a single `MailboxSendOperator` which is also the root of the stage.
These send operators are a bit special: They do not return the data when calling `nextBlock` but instead the side effect
of this method is that the data is sent to the mailbox.

Data can be sent to the mailbox in two ways:
1. Serialized as bytes (see `mailbox.proto` and specially implementations of `DataBlockSerde`). This is done when data
   needs to be sent to a worker in another server.
2. In memory format. This is done when the worker is in the same server. In this case they just share the Java objects
   through the concurrent queue associated to the mailbox.

The stage in the root and its single children are special.
The root stage is the only stage that:
- Is only be scheduled in the broker (in fact is the only one scheduled in a broker)
- Its root is always a `MailboxReceiveOperator`

The broker treats this stage in a special way, directly reading the results from the mailbox and sending them to the
client.

The other special stage is the single child of the root stage.
This stage is always scheduled in a server and it always has parallelism 1 and applies the final reduce operation.
Therefore this server is the one that actually calculates the result.
The side effect of this is that multi-stage queries are usually less memory intensive in the brokers.
