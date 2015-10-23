import java.util.*;

/**
 * The purpose of this class is to prove out a KV Store that uses, as its invariant, the fact that
 * for any transaction that is started at a given point in time, regardless of how long it actually takes,
 * we want to preserve the notion that it is a point transaction and takes no time at all.
 * <p/>
 * The obvious challenge here is to do it, without needing to specify, apriori, the keys to be used.
 * <p/>
 * So what happens if a transaction is transpiring, and then a second transaction, which uses a subset of
 * the keys in the first transaction, begins executing as well?
 * <p/>
 * It's clear that if the earlier transaction dirties any value that is *READ* by the later transaction,
 * that the later transaction must be rolled back. If the earlier transaction dirties a value that
 * the later transaction writes (but does not read), that is not a problem.
 * <p/>
 * Obvious next question - what are the rules around transactions that have been rolled back?
 * So, suppose that transaction n0 starts at time t0. Before it can be committed, transaction n1 starts
 * at time t1. n0 commits, and when n1 tries to commit, it needs to be rolled back and restarted.
 * Meanwhile, while n1 was executing fruitlessly, transactions n2 and n3 were enqueued. Now, if n2 and n3
 * shared no keys with n1, then it doesn't really matter. The interesting case is if n2 or n3 shared any
 * keys with n1. That becomes a challenge because from the perspective of a global observer, n1 is issued before
 * n2. So n2 must then wait behind n1. --- Could we begin to form a dependency graph? n2 can't execute until n1 is done.
 * However, we won't know that, because we don't know, ahead of time, which keys n2 is going to modify.
 * <p/>
 * So, as n2 is going on, if it hits a key is part of a rolled-back transaction, we can then rollback n2.
 * So if you're trying to re-run an earlier transaction, you can re-run it if for all the keys in the
 * transaction that are also in the data structure, each of the transaction keys' transaction stamps are
 * less than what's there.
 * <p/>
 * <p/>
 * Key optimizations - if there is a set of transactions, that span keys k{i..m}, and there is another
 * pending transaction, that does not involve and keys i..m, then the time at which it can be executed
 * is arbitrary, with respect to the transaction in the queue. This will allow the KV store to re-order
 * transactions without consequence.
 */
public class TimeConsistentTransactionalKVStore<K, V> {

    /*
    Data structures in play:
    1. Rollback keys: A map of keys to List of transaction ids where that key was rolled back from
    2. List of all states for a given transaction: Meaning - the state of the world when the transaction was started.
    If a transaction is rolled back, its state of the world is reset to the state of the world when the transaction before it
    has its state of the world merged with the master state of the world.
     */

    //The full known good payload. Only updated in the commit function
    HashMap<K, MetadataValue<V>> masterMap = new HashMap<K, MetadataValue<V>>();

    // Helper data structure for keeping track of transactions, based on their id.
    // Updated at the begin and the commit methods
    Map<Integer, Transaction> transactionIdToObjectMapping = new HashMap<Integer, Transaction>();

    // Data structure of the ongoing transactions and the units of work they are performing
    // Updated in the read/write section
    Map<Transaction, List<TransactionalKVStore.TransactionalUnit>> transactionsInFlight =
            new HashMap<Transaction, List<TransactionalKVStore.TransactionalUnit>>();

    /*
     *  This data structure keeps a mapping of each transaction and its understanding of the world
     *  It will contain a deep copy of all the values, so that if the master is changed, it will remain the same
     *
     *  Updated in the read/write section.
     */
    Map<Transaction, Map<K, MetadataValue<V>>> transactionsAndState = new HashMap<Transaction, Map<K, MetadataValue<V>>>();

    /*
     * This data structure keeps track of all transactions that need to be replayed once it is possible
     */
    Map<K, List<Integer>> rolledBackTransactions = new HashMap<K, List<Integer>>();

    public void begin(final int transactionId) {

        if (transactionId < 0) {
            throw new RuntimeException("Transaction id " + transactionId + " cannot be started because it is invalid");
        }

        if (transactionIdToObjectMapping.containsKey(transactionId)) {
            throw new RuntimeException("Transaction id " + transactionId + " cannot be started because it already exists");
        }

        Transaction newTransaction = new Transaction(transactionId);

        //Make a deep copy of all objects inside this array
        transactionIdToObjectMapping.put(transactionId, newTransaction);
        transactionsAndState.put(newTransaction, (HashMap<K, MetadataValue<V>>) masterMap.clone());
        transactionsInFlight.put(newTransaction, new ArrayList<TransactionalKVStore.TransactionalUnit>());
    }


    public V read(K key, int transactionId) {

        if (transactionId < 0) {
            throw new RuntimeException("Transaction id " + transactionId + " cannot be found because it is not valid");
        }

        Transaction transaction = transactionIdToObjectMapping.get(transactionId);
        if (transaction == null) {
            throw new RuntimeException("Transaction with transactionId " + transactionId + " not found.");
        }


        final TransactionalKVStore.IsolatedRead<K, V> read = new TransactionalKVStore.IsolatedRead(key);
        transactionsInFlight.get(transaction).add(read);
        Map<K, MetadataValue<V>> localTransactionState = transactionsAndState.get(transaction);
        MetadataValue<V> metadataValue = localTransactionState.get(key);
        return metadataValue.getValue();
    }

    public void write(K key, V value, int transactionId) {

        Transaction transaction = transactionIdToObjectMapping.get(transactionId);
        if (transactionId < 0 || !transactionsInFlight.containsKey(transaction)) {
            throw new RuntimeException("Transaction id " + transactionId + " cannot be found because it either is not running" +
                    " or is invalid");
        }

        final TransactionalKVStore.ValueChange<K, V> write = new TransactionalKVStore.ValueChange<K, V>(key, value);

        transactionsInFlight.get(transaction).add(write);
        transactionsAndState.get(transaction).put(key, new MetadataValue<V>(value));
    }

    public void commit(final int transactionId) throws RetryLaterException {

        /* for through each key used in the transaction (we should store this info somehow)
        *  Rules:
        *  If a key has been read in the transaction
        */

        Transaction transaction = transactionIdToObjectMapping.get(transactionId);
        if (transaction == null) {
            throw new IllegalStateException("About to commit, but there" +
                    " is no transaction available with transaction id " + transactionId);
        }


        int numConflictingKeys = needToRollBack(transaction);
        if (numConflictingKeys > 0) {
            transactionsAndState.remove(transaction);
            transactionsInFlight.remove(transactionId);
            throw new RetryLaterException(numConflictingKeys);
        }

        // Now that we know that nothing needs to be rolled back from this transaction
        for (TransactionalKVStore.TransactionalUnit transactionalUnit : transactionsInFlight.get(transaction)) {

            final K KEY = (K) transactionalUnit.getKey();
            MetadataValue<V> currentV = masterMap.get(KEY);
            if (transactionalUnit instanceof TransactionalKVStore.ValueChange) {

                //do an upsert on v
                if (!masterMap.containsKey(KEY)) {

                    MetadataValue<V> vForInsert = new MetadataValue<V>((V) transactionalUnit.getValue());
                    vForInsert.setLastWritten(((TransactionalKVStore.ValueChange) transactionalUnit).getTimeStamp());
                    vForInsert.setLastRead(null);
                    masterMap.put(KEY, vForInsert);
                } else {

                    //update
                    currentV.setValue((V) transactionalUnit.getValue());
                    currentV.setLastWritten(((TransactionalKVStore.ValueChange) transactionalUnit).getTimeStamp());

                }
            } else if (transactionalUnit instanceof TransactionalKVStore.IsolatedRead) {

                if (!masterMap.containsKey(KEY)) {

                    throw new IllegalStateException("We tried" +
                            " to read from a key, " + KEY + " that didn't exist in the master version");
                } else {

                    //update
                    currentV.setLastRead(((TransactionalKVStore.IsolatedRead<K, V>) transactionalUnit).getTimeStamp());
                }
            }
        }

        // At this point, all members of the transaction have been committed in order
        // all LR/LR updated. Now it's time for housekeeping

        // Transaction is over. Release locks and remove all references to it.
        transactionsInFlight.remove(transaction); // this transaction no longer running
        transactionsAndState.remove(transaction); // if it's not running, we don't need its copy of the data
        transactionIdToObjectMapping.remove(transactionId); //we will no longer need to do lookups
    }

    /* Check that since the transaction started, that no conflicting things have happened in the
     * master store
     */
    private int needToRollBack(Transaction t) {

        int numKeysThatPreventCommit = 0;
        for (TransactionalKVStore.TransactionalUnit unit : transactionsInFlight.get(t)) {


        }

        return numKeysThatPreventCommit;
    }

    public class TransactionValue<V> {

        private V value;
        private Date lastRead;
        private Date lastWritten;

        public V getValue() {
            return value;
        }

        public Date getLastRead() {
            return lastRead;
        }

        public Date getLastWritten() {
            return lastWritten;
        }
    }
}
