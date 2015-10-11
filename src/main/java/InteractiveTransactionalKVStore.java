import java.util.*;

/**
 * What:
 * This KV store is designed to create transactions whose logic and values written can be influenced by the current state.
 * For example, we can have complex logic such as if the KV store had key A->5 we can increment it by 2, but if the store
 * had key A->8, we can increment it by 10 and write it back. This interactivity is the reason for the name of the class.
 *
 * Why:
 * This is a strict improvement over TestTransactionStore because we now have the capability to change the transaction
 * based on what a read operation returns mid-transaction (i.e. read is not void here).
 *
 * How:
 * This is done by getting a lock on all the keys that are to be involved in the transaction. These must be known apriori.
 * The keys will be locked, and a client backoff mechanism will be necessary if and when a client lock cannot be
 * immediately obtained.
 *
 * Limitations:
 * The need to specify which keys will be used apriori makes this not particularly elegant. Also, the retry mechanism
 * also produces the risk of starvation, and makes client code a little awkward.
 *
 * @param <K>
 * @param <V>
 */
public class InteractiveTransactionalKVStore<K,V>  {

    private Map<K, V> store;
    private Map<K, V> secondaryStore;

    Set<K> keysEnqueuedInTransactions;
    Map<Integer, List<K>> transactionsAndKeys;

    {
        store = new HashMap<K, V>();
        keysEnqueuedInTransactions = new HashSet<K>();
        transactionsAndKeys = new HashMap<Integer, List<K>>();
    }

    //TODO: Find out if it's common practice to lock sets of keys in this way
    public void begin(int transactionId, List<K> keysToLock) throws RetryLaterException {

        //I'm wondering if this is a deep copy or a shallow one
        secondaryStore = new HashMap<K, V>(store);

        // The advice would be to wait 150 ms + 50 ms per locked key
        int keyEnqueuedCount = 0;
        for(K key : keysToLock) {

            if(keysEnqueuedInTransactions.contains(key)) {
                keyEnqueuedCount++;
            }
        }

        if(keyEnqueuedCount>0) {
            throw new RetryLaterException(keyEnqueuedCount);
        } else {
            //Get a lock on it by placing all keys into keysEnqueued
            keysEnqueuedInTransactions.addAll(keysToLock);
            transactionsAndKeys.put(transactionId, keysToLock);
        }
    }

    // This is different than what is in the TransactionalKVStore because this actually returns a value
    // Note though that you should read a value that you've already written
    public V read(K key, int transactionId) {

        //The logic is that we already have checked that nothing else will be operating on the returned value
        return secondaryStore.get(key);
    }

    // This is different than what is in the TransactionalKVStore because this actually returns a value
    public void write(K key, V value, int transactionId) {

        //The logic is that we already have checked that nothing else will be operating on the returned value
        secondaryStore.put(key, value);
    }
    public void commit(int transactionId) {

        store = secondaryStore;
        secondaryStore = null;
        //Need a good way to remove the key Ids we've processed from that list we have.
        // Probably accumulate them here and then remove at once.

        keysEnqueuedInTransactions.removeAll(transactionsAndKeys.get(transactionId));
        transactionsAndKeys.remove(transactionId);
    }
}
