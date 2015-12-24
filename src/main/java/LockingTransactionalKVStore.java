import java.util.*;

/**
 * What:
 * This KV store is designed to create transactions whose logic and values written can be influenced by the current state.
 * For example, we can have complex logic such as if the KV store had key A->5 we can increment it by 2, but if the store
 * had key A->8, we can increment it by 10 and write it back. This interactivity is the reason for the name of the class.
 * <p/>
 * Why:
 * This is a strict improvement over StaticTransactionalTransactionStore because we now have the capability to change the transaction
 * based on what a read operation returns mid-transaction (i.e. read is not void here).
 * <p/>
 * How:
 * This is done by getting a lock on all the keys that are to be involved in the transaction. These must be known apriori.
 * The keys will be locked, and a client back-off mechanism will be necessary if and when a client lock cannot be
 * immediately obtained.
 * <p/>
 * Limitations:
 * The need to specify which keys will be used apriori makes this not particularly elegant. Also, the retry mechanism
 * also produces the risk of starvation, and makes client code a little awkward.
 *
 * @param <K>
 * @param <V>
 */
public class LockingTransactionalKVStore<K, V> {

    Set<K> keysEnqueuedInTransactions;
    // A mapping of the transaction to the keys it will use. Starts with a copy of the keys
    // and maintains a state. We can get away with this because we know apriori which keys will
    // be used/modified
    Map<Integer, Map<K, V>> transactionsAndKeys;
    private Map<K, V> store;

    {
        store = new HashMap<K, V>();
        keysEnqueuedInTransactions = new HashSet<K>();
        transactionsAndKeys = new HashMap<Integer, Map<K, V>>();
    }

    //TODO: Find out if it's common practice to lock sets of keys in this way
    public void begin(int transactionId, List<K> keysToLock) throws RetryLaterException {

        // The advice would be to wait 150 ms + 50 ms per locked key
        int keyEnqueuedCount = 0;
        for (K key : keysToLock) {

            if (keysEnqueuedInTransactions.contains(key)) {
                keyEnqueuedCount++;
            }
        }

        if (keyEnqueuedCount > 0) {
            throw new RetryLaterException(keyEnqueuedCount);
        } else {
            //Get a lock on it by placing all keys into keysEnqueued.
            //Place a copy of the data to be modified transactionValues
            keysEnqueuedInTransactions.addAll(keysToLock);
            Map<K, V> transactionValues = new HashMap<K, V>();
            for (K key : keysToLock) {
                transactionValues.put(key, store.get(key));
            }
            transactionsAndKeys.put(transactionId, transactionValues);
        }
    }

    // This is different than what is in the StaticTransactionalKVStore because this actually returns a value
    // Note though that you should read a value that you've already written
    public V read(K key, int transactionId) {

        //The logic is that we already have checked that nothing else will be operating on the returned value
        if (transactionsAndKeys.get(transactionId) == null) {
            throw new RuntimeException("Invalid transaction id " + transactionId + " specified");
        }
        return transactionsAndKeys.get(transactionId).get(key);
    }

    // This is different than what is in the StaticTransactionalKVStore because this actually returns a value
    public void write(K key, V value, int transactionId) {

        //The logic is that we already have checked that nothing else will be operating on the returned value
        if (transactionsAndKeys.get(transactionId) == null) {
            throw new RuntimeException("Invalid transaction id " + transactionId + "specified");
        }

        transactionsAndKeys.get(transactionId).put(key, value);
    }

    public void commit(int transactionId) {

        if (!transactionsAndKeys.containsKey(transactionId)) {
            throw new RuntimeException("Transaction " + transactionId + " not valid. Failing.");
        }

        copyFromTempStore(transactionsAndKeys.get(transactionId));
        keysEnqueuedInTransactions.removeAll(transactionsAndKeys.get(transactionId).keySet());
        transactionsAndKeys.remove(transactionId);
    }

    private void copyFromTempStore(Map<K, V> temporaryMap) {

        for (K key : temporaryMap.keySet()) {
            store.put(key, temporaryMap.get(key));
        }
    }
}
