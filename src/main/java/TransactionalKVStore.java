import java.util.*;

/**
 * The purpose of this class is to store key-values and accept
 * transactions over those stored values.
 * <p/>
 * In the case of concurrent modification, to enforce ACID properties,
 * isolation in particular, it is required to carefully monitor reads
 * and writes.
 * <p/>
 * The way to think about it is that every transaction starts with its
 * own copy of the database, that it modifies in isolation. When the
 * transaction is finished, it must incorporate its modifications to
 * the master key-value store.
 * 1. If your transaction, t1 is attempting to commit, and it used as an input
 * any key that had a write commit occur after t1 started, then that invalidates
 * the input. The transaction has to be restarted.
 * <p/>
 * The canonical example is a bank account where there are two concurrent
 * appends. The later transaction will need to be retried because the data
 * it used (initial amount) has become stale.
 * <p/>
 * A secondary benefit to this KV store is the ability to re-run sets of
 * anonymous instructions, of arbitrarily complex logic. This way a user
 * will just need to send a request once and then poll (no need to resubmit).
 */
public class TransactionalKVStore<K, V> {

    final public static int DEFAULT_MAX_HANDLED_ATTEMPTS = 100;
    private final int SLEEP_CONST_MS = 5;
    //The master copy of the data. Considered the source of truth. Only updated in the commit function.
    volatile HashMap<K, MetadataValue<V>> masterMap = new HashMap<K, MetadataValue<V>>();
    // Helper data structure for keeping track of transactions, based on their id.
    // Updated at the begin and the commit methods
    volatile Map<Integer, Transaction> transactionIdToObjectMapping = new HashMap<Integer, Transaction>();
    // Data structure of the ongoing transactions and the units of work they are performing
    // Updated in the read/write section
    volatile Map<Transaction, List<StaticTransactionalKVStore.TransactionalUnit>> transactionsInFlight =
            new HashMap<Transaction, List<StaticTransactionalKVStore.TransactionalUnit>>();


    /*
     *  This data structure keeps a mapping of each transaction and its understanding of the world
     *  It will contain a deep copy of all the values, so that if the master is changed, it will remain the same
     *
     *  Updated in the read/write section.
     */
    volatile Map<Transaction, Map<K, MetadataValue<V>>> transactionsAndState = new HashMap<Transaction, Map<K, MetadataValue<V>>>();

    /**
     * This method will evaluate whether a transaction, t, in the given context of the transactionalUnits,
     * and the current state of the KV, can proceed or whether it will need to be rolled back and retried
     * <p/>
     * Rules:
     * Rule 1: If you are about to commit a read, there should not be any write commits on that key after
     * your transaction was started
     * <p/>
     * Info needed: 1. The commit timestamp of the write to that value.
     * 2. The transactions start time of the transaction you're trying to commit
     *
     * @param t
     * @param transactionalUnits
     * @param masterMap
     * @return
     */
    static boolean needToRollBack(final Transaction t, final List<StaticTransactionalKVStore.TransactionalUnit> transactionalUnits, final HashMap masterMap) {

        if (transactionalUnits.size() == 0) {
            System.out.println("WARNING: transaction " + t.getId() + " had no associated transactionalUnits!!");
        }

        for (StaticTransactionalKVStore.TransactionalUnit unit : transactionalUnits) {

            final Date T_START_TIME = t.getStartTime();
            final Object KEY = unit.getKey();

            MetadataValue returnedValue = (MetadataValue) masterMap.get(KEY);
            if (returnedValue != null) {

                // common case. there was already an entry there
                final Date LAST_MASTER_WRITTEN = returnedValue.getLastWritten();
                if (LAST_MASTER_WRITTEN != null) {

                    // There has been a write

                    //if (LAST_MASTER_WRITTEN.after(T_START_TIME) || LAST_MASTER_WRITTEN.equals(T_START_TIME)) {
                    if (LAST_MASTER_WRITTEN.getTime() >= T_START_TIME.getTime()) {

                        // If a write was committed after the transaction started, the state of the world (and hence the read)
                        // is invalidated

                        System.out.println("Transaction " + t.getId() + " key " + KEY + ", for which there was a read transaction at "
                                + unit.getTimeStamp().getTime() + ", was had a write commit at " +
                                LAST_MASTER_WRITTEN.getTime() + " which is after the transaction started");
                        return true;
                    }
                }
            }
        }

        return false;
    }

    synchronized public void begin(final int transactionId) throws InterruptedException {

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
        transactionsInFlight.put(newTransaction, new ArrayList<StaticTransactionalKVStore.TransactionalUnit>());
    }

    /**
     * Read the current value for a given key. If it is not present, return null
     * <p/>
     * Side effect is that it updates the timestamp for last read
     *
     * @param key
     * @param transactionId
     * @return
     */
    public V read(K key, int transactionId) throws InterruptedException {

        Transaction transaction = validateTransactionId(transactionId);

        final StaticTransactionalKVStore.IsolatedRead<K, V> read = new StaticTransactionalKVStore.IsolatedRead(key);

        List<StaticTransactionalKVStore.TransactionalUnit> returnedTransactionList = transactionsInFlight.get(transaction);
        if (returnedTransactionList == null) {
            throw new IllegalStateException("Expected transaction with id " + transaction.getId() + " to not be null");
        }

        returnedTransactionList.add(read);
        Map<K, MetadataValue<V>> localTransactionState = transactionsAndState.get(transaction);
        if (localTransactionState == null) {
            throw new IllegalStateException("Local transition state for transaction " + transaction.getId() + " cannot be null");
        }

        MetadataValue<V> metadataValue = localTransactionState.get(key);

        if (metadataValue == null) {

            //if there have been no requests on this key
            MetadataValue<V> v = new MetadataValue<V>((V) null);
            v.setLastWritten(null);
            v.setLastRead(new Date());

            // save the fact that someone attempted to read this value before it was written
            localTransactionState.put(key, v);
            return null;
        }

        return metadataValue.getValue();
    }

    public void write(K key, V value, int transactionId) throws InterruptedException {

        Transaction transaction = validateTransactionId(transactionId);
        final StaticTransactionalKVStore.ValueChange<K, V> write = new StaticTransactionalKVStore.ValueChange<K, V>(key, value);

        transactionsInFlight.get(transaction).add(write);
        transactionsAndState.get(transaction).put(key, new MetadataValue<V>(value));
    }

    public void remove(K key) {

        // Sort of like a write to make the value null.
        // Do we clear metadata as a result?
        throw new RuntimeException("Remove not yet implemented");
    }

    /**
     * The logic here is that in this single-threaded server, if there were transactions that
     * dirtied values used, then a simple server-side replay should fix everything.
     *
     * @param w
     * @throws InterruptedException
     */
    public static void submitReplayableTransaction(ReplayableTransaction w, Object[]
            arguments, TransactionalKVStore store, Integer maxAttempts) throws
            InterruptedException {

        if (maxAttempts == null) {
            maxAttempts = DEFAULT_MAX_HANDLED_ATTEMPTS;
        }

        int attempts = 0;
        while (true) {

            try {

                //Expectation is that commit should occur here
                w.transaction(arguments, store);
                // if no exception thrown
                break;
            } catch (RetryLaterException rte) {

                System.out.println(rte.getLocalizedMessage());
                Thread.sleep(rte.getWaitTimeMs());
                attempts++;
                if (attempts > maxAttempts) {
                    throw new RuntimeException("Could not commit transaction, even after " +
                            attempts + " attempts");
                }
            }
        }
    }

    synchronized public void commit(final int transactionId) throws RetryLaterException, InterruptedException {

        // Add some padding to make sure that events that are not supposed to occur in
        // the same milliseconds aren't treated as though they do

        Thread.sleep(SLEEP_CONST_MS);
        final Date COMMIT_START_TIME = new Date();

        System.out.println(COMMIT_START_TIME.getTime() + "--Will attempt to commit on transactionId " + transactionId);
        Transaction transaction = validateTransactionId(transactionId);
        if (transaction == null) {
            String message = "About to commit, but there" +
                    " is no transaction available with transaction id " + transactionId;
            System.out.println(message);
            throw new IllegalStateException(message);
        }

        if (needToRollBack(transaction)) {
            transactionsAndState.remove(transaction);
            transactionsInFlight.remove(transactionId);
            String message = "need to roll back transaction " + transactionId;
            System.out.println(message);
            throw new RetryLaterException(message);
        }

        // Now that we know that nothing needs to be rolled back from this transaction
        for (StaticTransactionalKVStore.TransactionalUnit transactionalUnit : transactionsInFlight.get(transaction)) {

            final K KEY = (K) transactionalUnit.getKey();
            MetadataValue<V> currentV = masterMap.get(KEY);
            if (transactionalUnit instanceof StaticTransactionalKVStore.ValueChange) {

                //do an upsert on v
                if (!masterMap.containsKey(KEY)) {

                    // First write
                    MetadataValue<V> vForInsert = new MetadataValue<V>((V) transactionalUnit.getValue());
                    vForInsert.setLastWritten(COMMIT_START_TIME);
                    vForInsert.setLastRead(null);
                    System.out.println("-----First write " + transactionId);
                    masterMap.put(KEY, vForInsert);
                } else {

                    // Update
                    currentV.setValue((V) transactionalUnit.getValue());
                    currentV.setLastWritten(COMMIT_START_TIME);
                }
            } else if (transactionalUnit instanceof StaticTransactionalKVStore.IsolatedRead) {

                //update, unless this is a read of a value that does not exist
                if (currentV != null) {
                    currentV.setLastRead(((StaticTransactionalKVStore.IsolatedRead<K, V>) transactionalUnit).getTimeStamp());
                } else {
                    // if there is no entry for this in the master map, but there was a read
                    // we need to inform the system that someone read null, which I guess is a read.

                    Map<K, MetadataValue<V>> localTransactionState = transactionsAndState.get(transaction);
                    localTransactionState.get(KEY).setLastRead(COMMIT_START_TIME);
                    masterMap.put(KEY, localTransactionState.get(KEY));
                }
            } else {
                Thread.sleep(SLEEP_CONST_MS);
                throw new IllegalStateException("Unrecognized form of Transaction");
            }
        }

        // At this point, all members of the transaction have been committed in order
        // all LR/LR updated. Now it's time for housekeeping

        // Transaction is over. Release locks and remove all references to it.
        transactionsInFlight.remove(transaction); // this transaction no longer running
        transactionsAndState.remove(transaction); // if it's not running, we don't need its copy of the data
        transactionIdToObjectMapping.remove(transactionId); //we will no longer need to do lookups

        System.out.println(new Date().getTime() + "--Just finished commit on transactionId " + transactionId);
        Thread.sleep(SLEEP_CONST_MS);
    }

    private Transaction validateTransactionId(int transactionId) {
        if (transactionId < 0) {
            throw new NoSuchTransactionException(transactionId);
        }

        Transaction transaction = transactionIdToObjectMapping.get(transactionId);
        if (transaction == null) {
            throw new NoSuchTransactionException(transactionId);
        }

        if (!transactionsInFlight.containsKey(transaction)) {

            throw new NoSuchTransactionException(transactionId);
        }

        return transaction;
    }

    /* Check that since the transaction started, that no conflicting things have happened in the
     * master store
     */
    boolean needToRollBack(Transaction t) {
        return TransactionalKVStore.needToRollBack(t, transactionsInFlight.get(t), masterMap);
    }

    boolean needToRollBack(final int transactionId) {
        return needToRollBack(validateTransactionId(transactionId));
    }

    /**
     * This class defined a transaction that is kicked off using a static method.
     * The method will then attempt to replay it.
     */
    public abstract static class ReplayableTransaction {

        /**
         * This function is a general main method that will accept a list of arguments,
         * and manipulate them using the primitives available to produce a transaction.
         *
         * @param arguments
         * @throws RetryLaterException
         */
        public abstract void transaction(Object[] arguments, TransactionalKVStore
                store) throws
                RetryLaterException, InterruptedException;
    }
}
