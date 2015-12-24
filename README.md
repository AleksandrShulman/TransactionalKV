# TransactionalKV
A simple Key-Value store for arbitrary objects that can handle parallel clients and perform transactions around read, write, and increment, as well as arbitrary user-defined transactions.

This project sets out to create a Transactional Key-Value store, through various approaches. The final approach, TransactionKV, works by enforcing a simple invariant.

# Cool aspects
- The parallel client testing really proves out the correctness of the KV store
- The ability to send arbitrary user-defined transactions to the server, to have it handle the retry logic, is pretty nifty. Just send your business logic over the wire, and it'll get executed in a transactional manner.

# Next steps
- Add more tests for multi-key transactions
- Add more tests for server-side arbitrary code execution
- Adjust the packaging to provide a basic client library w/configs to point at a remove instance of the server


