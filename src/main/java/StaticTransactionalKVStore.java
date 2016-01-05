import java.util.*;

/**
 * A class designed to group operations into transactions and execute those transactions.
 * It should give the classic ACID properties:
 * 1. Atomic - All mutations and reads in a transaction are carried out to completion, or not at all
 * 2. Consistent - The values that the transaction depends on will not change
 * 3. Isolated - Every action is led to believe it is the only one executing. Nothing can change the state of the system underneath it.
 * 4. Durable - Nothing is written to disk. We'll call memory durable. But we could easily just write to a WAL.
 * <p/>
 * <p/>
 * The major limitation with this implementation is that while reads occur, and results are returned when
 * the transaction is committed, there are no intermediate reads. Meaning, the logic cannot depend on the
 * current state.
 * <p/>
 * As a result, so while this is a good first attempt, it is not very practical.
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
public class StaticTransactionalKVStore<K, V> {


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

        TransactionalUnit<K, V> readRequest = new IsolatedRead<K, V>(key);
        transactionList.add(readRequest);
    }

    public List<TransactionalUnit<K, V>> commit(final int transactionId) {

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

        abstract Date getTimeStamp();

        abstract V getValue();
    }

    public static class IsolatedRead<K, V> extends TransactionalUnit {

        final private Date timestamp;
        private K key;
        private V value;

        public IsolatedRead(K key) {

            this.key = key;
            this.timestamp = new Date();
        }

        public K getKey() {
            return this.key;
        }

        public Date getTimeStamp() {
            return this.timestamp;
        }

        Object getValue() {
            return this.value;
        }

        public void setValue(V value) {
            this.value = value;
        }
    }

    public static class ValueChange<K, V> extends TransactionalUnit {

        final private K key;
        final private V value;
        final private Date timestamp;

        public ValueChange(K key, V newValue) {

            this.key = key;
            this.value = newValue;
            this.timestamp = new Date();
        }

        public Date getTimeStamp() {
            return this.timestamp;
        }

        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }
    }

    /**
     * A remove is a ValueChange, except the value is set null.
     *
     * @param <K>
     * @param <V>
     */
    public static class Remove<K, V> extends ValueChange<K, V> {

        public Remove(K key) {
            super(key, null);
        }
    }
}
