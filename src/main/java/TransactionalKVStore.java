import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class designed to group operations into transactions and execute those transactions.
 * It should give the classic ACID properties:
 * 1. Atomic - All mutations and reads in a transaction are carried out to completion, or not at all
 * 2. Consistent - The values that the transaction depends on will not change
 * 3. Isolated - Every action is led to believe it is the only one executing. Nothing can change the state of the system underneath it.
 * 4. Durable - Nothing is written to disk. We'll call memory durable. But we could easily just write to a WAL.
 *
 * @param <K>
 * @param <V>
 */

/**
 * Some common cases:
 * <p/>
 * 1. Q: What if a transaction is started that includes a key that is already being processed?
 * A: It won't be enqueued until that other transaction is completed, so the values it read/see
 * will change before it starts, but at least they will not change over the duration
 */
public class TransactionalKVStore<K, V> {


    //Store is source of truth. It's the backing store.
    private final Map<K, V> store;

    //This is the list of transactions that need to be processed
    Map<Integer, List<TransactionalUnit<K, V>>> transactionQueue;

    {
        store = new HashMap<K, V>();
        transactionQueue = new HashMap<Integer, List<TransactionalUnit<K, V>>>();
    }


    public void begin(final int transactionId) {

        //Open a transaction with this id. Throw an exception if this ID already exists
        if (transactionQueue.get(transactionId) != null) {
            throw new RuntimeException("Request to begin transaction with ID " + transactionId + " received more than once");
        }

        transactionQueue.put(transactionId, new ArrayList<TransactionalUnit<K, V>>());
    }

    public void write(K key, V value, final int transactionId) {

        List<TransactionalUnit<K, V>> transactionList = transactionQueue.get(transactionId);
        if (transactionList == null) {
            throw new RuntimeException("Attempting to add a write " +
                    "operation to a transaction for which there is no entry " + transactionId);
        }

        TransactionalUnit<K, V> writeRequest = new ValueChange<K, V>(key, value);
        transactionList.add(writeRequest);
    }

    public void read(K key, int transactionId) {

        List<TransactionalUnit<K, V>> transactionList = transactionQueue.get(transactionId);
        if (transactionList == null) {
            throw new RuntimeException("Attempting to add a write " +
                    "operation to a transaction for which there is no entry " + transactionId);
        }

        TransactionalUnit<K, V> writeRequest = new IsolatedRead<K, V>(key);
        transactionList.add(writeRequest);
    }

    public List<TransactionalUnit<K,V>> commit(final int transactionId) {

        /** TODO: Think of an atomic implementation. Likely this will just involve operating on
         * a copy of the data until it succeeds or fails. Then the pointer will point to the new
         * data structure and the old one will be discarded. In the case of failure, however, the
         * pointer will not be switched and a failure will be returned.
         */

        List<TransactionalUnit<K, V>> returnedResults = new ArrayList<TransactionalUnit<K, V>>();
        for (TransactionalUnit<K, V> individualCommit : transactionQueue.get(transactionId)) {

            K key = individualCommit.getKey();
            if (individualCommit instanceof IsolatedRead) {

                //a read. populate the read object
                ((IsolatedRead) individualCommit).setValue(store.get(key));
                returnedResults.add(individualCommit);
            } else if (individualCommit instanceof ValueChange) {
                //a write. Write it out.
                store.put(key, individualCommit.getValue());
                returnedResults.add(individualCommit);
            }
        }
        return returnedResults;
    }

    public abstract static class TransactionalUnit<K, V> {

        abstract K getKey();
        abstract V getValue();
    }

    public static class IsolatedRead<K, V> extends TransactionalUnit {

        private K key;
        private V value;

        public IsolatedRead(K key) {

            this.key = key;
        }

        public void setValue(V value) {
            this.value = value;
        }

        public K getKey() { return this.key; }

        Object getValue() {
            return this.value;
        }
    }

    public static class ValueChange<K, V> extends TransactionalUnit {

        private K key;
        private V value;

        public ValueChange(K key, V newValue) {

            this.key = key;
            this.value = newValue;
        }

        public K getKey() {
            return key;
        }

        public void setKey(K key) {
            this.key = key;
        }

        @Override
        public V getValue() {
            return value;
        }

    }
}
