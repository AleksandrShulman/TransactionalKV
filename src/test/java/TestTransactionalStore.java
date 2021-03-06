import junit.framework.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by aleks on 10/22/15.
 */
public class TestTransactionalStore {

    final static String KEY_1 = "key1";
    public static AtomicInteger sharedTransactionCounter = new AtomicInteger(0);
    public static TransactionalKVStore.ReplayableTransaction CONTEXT_FREE_INCREMENT_ACTION;
    public static TransactionalKVStore.ReplayableTransaction FIBONACCI_ACTION;

    static {

        /*
        This action will calculate the Fibonacci sequence.
        The key "size" is the size of the list.
        The algorithm will use the value of size to set the value(size)'th key
         */
        FIBONACCI_ACTION =
                new TransactionalKVStore.ReplayableTransaction() {

                    @Override
                    public void transaction(Object[] arguments, TransactionalKVStore
                            store) throws RetryLaterException, InterruptedException {

                        // This is the number of elements in the Array
                        final String SIZE_KEY = "size";

                        final int REPLAYABLE_T_ID = sharedTransactionCounter.incrementAndGet();
                        store.begin(REPLAYABLE_T_ID);

                        // read in the size
                        Object currentSize = store.read(SIZE_KEY, REPLAYABLE_T_ID);
                        if (currentSize == null) {

                            //initialize the array
                            store.write("0", 0L, REPLAYABLE_T_ID);
                            store.write("1", 1L, REPLAYABLE_T_ID);
                            store.write(SIZE_KEY, 2L, REPLAYABLE_T_ID);
                            store.commit(REPLAYABLE_T_ID);

                        } else {

                            // We want to write to the index that is the current value of size
                            // Then we want to increment size


                            Long previousValue = (Long) store.read(String.valueOf((Long) currentSize
                                            - 2),
                                    REPLAYABLE_T_ID);
                            Long currentValue = (Long) store.read(String.valueOf((Long) currentSize
                                            - 1),
                                    REPLAYABLE_T_ID);

                            if (currentValue == null) {
                                throw new IllegalStateException("A value was null for key " +
                                        ((Long) currentSize - 1));
                            }

                            if (previousValue == null) {
                                throw new IllegalStateException("A value was null for key " +
                                        ((Long) currentSize - 2L));
                            }

                            Long newValue = previousValue + currentValue;
                            store.write(String.valueOf(currentSize), newValue, REPLAYABLE_T_ID);
                            store.write(SIZE_KEY, (Long) currentSize + 1, REPLAYABLE_T_ID);
                            store.commit(REPLAYABLE_T_ID);
                        }
                    }
                };

        CONTEXT_FREE_INCREMENT_ACTION =
                new TransactionalKVStore.ReplayableTransaction() {

                    @Override
                    public void transaction(Object[] arguments, TransactionalKVStore
                            store) throws RetryLaterException, InterruptedException {

                        // Very important that this all relies on the arguments being parsed
                        // correctly

                        // Args: KEY, INCREMENT_AMOUNT
                        String KEY = (String) arguments[0];
                        int INCR_VALUE = (Integer) arguments[1];

                        final int REPLAYABLE_T_ID = sharedTransactionCounter.incrementAndGet();
                        System.out.println("Implementing an append");
                        store.begin(REPLAYABLE_T_ID);
                        Integer result = (Integer) store.read(KEY, REPLAYABLE_T_ID);
                        if (result == null)
                            result = 0; //if it does not exist, we're incrementing off of 0
                        int newValue = result + INCR_VALUE;
                        store.write(KEY, newValue, REPLAYABLE_T_ID);
                        store.commit(REPLAYABLE_T_ID);
                    }
                };
    }

    final int DEFAULT_MAX_FAILED_COMMIT_ATTEMPTS = 5;
    final Integer VALUE_1 = 42;

    /**
     * Because sometimes we're not able to succeed right away, a user will need to have retry logic. In this case,
     * I implemented the business logic to client_side_increment. A user will need to write something similar to this for
     * everything they're working on
     * <p/>
     * TODO: Find a way to re-use the retry logic here around the functional shell. We can use anonymous functions to
     * do this, I imagine
     *
     * @param store
     * @param KEY
     * @param AMOUNT
     * @throws InterruptedException
     */

    public static void client_side_increment(TransactionalKVStore<String, Integer> store, final
    String KEY, final int AMOUNT, final int MAX_FAILED_ATTEMPTS) throws InterruptedException {

        int attempts = 0;
        while (true) {

            int transactionId = sharedTransactionCounter.incrementAndGet();
            if (attempts > 0)
                System.out.println("Got new transactionId " + transactionId + ". This is attempt " + attempts);
            try {

                //do some logic
                store.begin(transactionId);
                Integer someValue = store.read(KEY, transactionId);
                Integer newValue = someValue + AMOUNT;
                store.write(KEY, newValue, transactionId);

                store.commit(transactionId);
                break;
            } catch (RetryLaterException rte) {

                System.out.println("Commit for transactionId " + transactionId + " did throw an exception");

                Thread.sleep(rte.getWaitTimeMs());
                System.out.println("For transactionId " + transactionId + " attempt " + attempts + " failed.");
                attempts++;
                if (attempts > MAX_FAILED_ATTEMPTS) {
                    throw new RuntimeException("Could not commit transaction, even after " + MAX_FAILED_ATTEMPTS + " attempts");
                }
            }
        }
    }

    public static void server_side_increment(TransactionalKVStore<String, Integer> store, final
    String KEY, final int AMOUNT, final int MAX_FAILED_ATTEMPTS) throws InterruptedException {

        // Transaction is already defined. Just submit it for processing
        TransactionalKVStore.submitReplayableTransaction(
                CONTEXT_FREE_INCREMENT_ACTION, new Object[]{KEY, AMOUNT, MAX_FAILED_ATTEMPTS},
                store, MAX_FAILED_ATTEMPTS);
    }

    @Test
    /**
     * Given: A simple Transactional Key-Value Store
     * Assert that: A value can be written and then retrieved.
     */
    public void testBasicWriteAndRetrieve() throws InterruptedException {

        final int T_ID = 1;

        //Write transaction
        TransactionalKVStore<String, Integer> store = new TransactionalKVStore<String, Integer>();
        store.begin(T_ID);
        store.write(KEY_1, VALUE_1, T_ID);

        commitWithException(store, T_ID);

        final int T_ID_2 = T_ID + 1;
        store.begin(T_ID_2);

        Integer result = store.read(KEY_1, T_ID_2);

        Assert.assertEquals("Results did not match on basic read!", VALUE_1, result);
        commitWithException(store, T_ID_2);
    }

    //TODO: Write tests to verify last read and write time in master
    @Test
    /**
     * Given: A set of read and writes on a given value
     * Assert that: Read and write times on the values in the master KV store are updated
     */
    public void testMasterReadAndWriteTimes() throws InterruptedException {

        final Date START_TIME = new Date();

        final int T_ID = 1;
        final int T_ID_2 = T_ID + 1;

        TransactionalKVStore<String, Integer> tctStore = new TransactionalKVStore<String, Integer>();
        tctStore.begin(T_ID);
        tctStore.write(KEY_1, VALUE_1, T_ID);
        commitWithException(tctStore, T_ID);

        //This will work because it's package-protected. The other option is to add
        //a getter that's only available in the testing context
        Date lastWritten = tctStore.masterMap.get(KEY_1).getLastWritten();
        Assert.assertTrue(lastWritten.after(START_TIME));

        Date lastRead = tctStore.masterMap.get(KEY_1).getLastRead();
        Assert.assertNull(lastRead);

        tctStore.begin(T_ID_2);
        tctStore.read(KEY_1, T_ID_2);
        commitWithException(tctStore, T_ID_2);

        lastRead = tctStore.masterMap.get(KEY_1).getLastRead();
        Assert.assertTrue(lastRead.after(START_TIME));
        Assert.assertTrue(lastRead.after(lastWritten));
    }

    @Test
    /**
     * Given: A value is stored in a transaction, then overwritten by the next transaction
     * Assert that: The new value is the one that is retrieved.
     */
    public void testBasicOverwriteAndRetrieve() throws InterruptedException {

        final int T_ID = 1;
        final int T_ID_2 = T_ID + 1;
        final int T_ID_3 = T_ID_2 + 1;

        final Integer NEW_VALUE = VALUE_1 + 1;

        //Write transaction
        TransactionalKVStore<String, Integer> tctStore = new TransactionalKVStore<String, Integer>();
        tctStore.begin(T_ID);
        tctStore.write(KEY_1, VALUE_1, T_ID);
        commitWithException(tctStore, T_ID);

        tctStore.begin(T_ID_2);
        tctStore.write(KEY_1, NEW_VALUE, T_ID_2);
        commitWithException(tctStore, T_ID_2);

        tctStore.begin(T_ID_3);
        Integer result = tctStore.read(KEY_1, T_ID_3);

        Assert.assertEquals("Results did not match on basic read!", NEW_VALUE, result);
        commitWithException(tctStore, T_ID_3);
    }

    @Test
    /**
     * Given: Multiple overlapping writes on non-common keys
     * Assert that: All writes get committed and all transactions complete
     */
    public void testOverlappingWritesOnDifferentKeys() throws InterruptedException {

        final int T_ID = 1;
        final int T_ID_2 = T_ID + 1;
        final int T_ID_3 = T_ID_2 + 1;

        final String KEY_1 = "key1";
        final String KEY_2 = "key2";
        final String KEY_3 = "key3";

        final int VALUE_1 = 1;
        final int VALUE_2 = 2;
        final int VALUE_3 = 99;

        final int VERIFY_TRANSACTION = T_ID_3 + 1;

        TransactionalKVStore<String, Integer> tctStore = new TransactionalKVStore<String, Integer>();

        tctStore.begin(T_ID);
        tctStore.write(KEY_1, VALUE_1, T_ID);

        tctStore.begin(T_ID_2);
        tctStore.write(KEY_2, VALUE_2, T_ID_2);

        tctStore.begin(T_ID_3);
        tctStore.write(KEY_3, VALUE_3, T_ID_3);

        try {
            tctStore.commit(T_ID);
            tctStore.commit(T_ID_2);
            tctStore.commit(T_ID_3);

        } catch (RetryLaterException rte) {
            System.out.print(rte.getLocalizedMessage());
            throw new IllegalStateException("Test transaction hit a situation where it was asked to retry, but for no apparent reason");
        }

        tctStore.begin(VERIFY_TRANSACTION);
        final int RESULT_1 = tctStore.read(KEY_1, VERIFY_TRANSACTION);
        Assert.assertEquals(VALUE_1, RESULT_1);

        final int RESULT_2 = tctStore.read(KEY_2, VERIFY_TRANSACTION);
        Assert.assertEquals(VALUE_2, RESULT_2);

        final int RESULT_3 = tctStore.read(KEY_3, VERIFY_TRANSACTION);
        Assert.assertEquals(VALUE_3, RESULT_3);

        commitWithException(tctStore, VERIFY_TRANSACTION);
    }

    @Test
    /**
     * Given:   Overlapping commits that client_side_increment a value
     * Assert:  Both increments are considered
     *
     * WARNING: This test fails when run as part of all tests, but passes when run in isolation...
     *          This is due to a race condition, but this is fixed by adding minor sleeps between
     *          key events.
     */
    public void testConcurrentIncrements() throws InterruptedException {

        final TransactionalKVStore<String, Integer> tctStore = new TransactionalKVStore<String, Integer>();

        // Initialize a value
        final int INITIAL_WRITE_TRANSACTION = sharedTransactionCounter.incrementAndGet();
        final int FIRST_INCREMENT_TRANSACTION = sharedTransactionCounter.incrementAndGet();
        final int SECOND_INCREMENT_TRANSACTION = sharedTransactionCounter.incrementAndGet();
        final int VERIFY_TRANSACTION = sharedTransactionCounter.incrementAndGet();

        final int INCREMENT_AMOUNT_1 = 8;
        final int INCREMENT_AMOUNT_2 = 13;
        final int EXPECTED_FINAL_VALUE = VALUE_1 + INCREMENT_AMOUNT_1 + INCREMENT_AMOUNT_2;

        // Start one transaction on the value for client_side_increment
        tctStore.begin(INITIAL_WRITE_TRANSACTION);
        tctStore.write(KEY_1, VALUE_1, INITIAL_WRITE_TRANSACTION);
        commitWithException(tctStore, INITIAL_WRITE_TRANSACTION);

        // Start a second transaction before the first one is committed
        tctStore.begin(FIRST_INCREMENT_TRANSACTION);
        tctStore.begin(SECOND_INCREMENT_TRANSACTION);

        //Client 1 doing a basic client_side_increment
        int preFirstIncrementValue = tctStore.read(KEY_1, FIRST_INCREMENT_TRANSACTION);
        int postFirstIncrementValue = preFirstIncrementValue + INCREMENT_AMOUNT_1;
        tctStore.write(KEY_1, postFirstIncrementValue, FIRST_INCREMENT_TRANSACTION);

        //Client 2 doing a basic client_side_increment
        int preSecondIncrementValue = tctStore.read(KEY_1, SECOND_INCREMENT_TRANSACTION);

        int postSecondIncrementValue = preSecondIncrementValue + INCREMENT_AMOUNT_2;
        tctStore.write(KEY_1, postSecondIncrementValue, SECOND_INCREMENT_TRANSACTION);

        // Commit the transactions in the order that they were issued
        commitWithException(tctStore, FIRST_INCREMENT_TRANSACTION);
        try {
            tctStore.commit(SECOND_INCREMENT_TRANSACTION);
            Assert.fail("The server did not instruct the client to retry");
        } catch (RetryLaterException rte) {

            client_side_increment(tctStore, KEY_1, INCREMENT_AMOUNT_2, DEFAULT_MAX_FAILED_COMMIT_ATTEMPTS);
        }

        // Verify the final value is correct
        tctStore.begin(VERIFY_TRANSACTION);
        final int RETURNED_VALUE = tctStore.read(KEY_1, VERIFY_TRANSACTION);

        Assert.assertEquals("Increments overwrote one another!", EXPECTED_FINAL_VALUE, RETURNED_VALUE);
        commitWithException(tctStore, VERIFY_TRANSACTION);
    }

    private void commitWithException(final TransactionalKVStore<String, Integer> tctStore, final int TRANSACTION_ID) {

        try {
            tctStore.commit(TRANSACTION_ID);
        } catch (RetryLaterException rte) {
            System.out.print(rte.getLocalizedMessage());
            throw new IllegalStateException("Failed while committing transaction " + TRANSACTION_ID);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    /**
     * Given:  A basic set of instructions that compose a transactions
     * Assert: It is possible to use server-side processing run (and if necessary, retry) them
     *
     * Notes:  Being able to do this is a real game-changer. Then, we can have retry logic on the server-side
     *         So the current APIs, read and write, just make calls to the internal data structures for transactions
     *         in flight. So maybe we can just add some calls here
     */
    public void testCustomCodePushdown() throws InterruptedException {

        final TransactionalKVStore<String, Integer> store = new TransactionalKVStore<String, Integer>();
        final AtomicInteger transactionId = new AtomicInteger();


        //Note: This test is testing an odd condition where there is a transaction conflict to append on a value
        // that does not yet exist. This is something of an edge case. For now, I'll add in a call to create this
        // initial value and see how that affects things.
        final int INITIAL_WRITE_TRANSACTION = transactionId.incrementAndGet();
        final String KEY_1 = "key1";
        final int INITIAL_VALUE = 5;
        final int INCR_VALUE = 204;

        // Start one transaction on the value for client_side_increment
        store.begin(INITIAL_WRITE_TRANSACTION);
        store.write(KEY_1, INITIAL_VALUE, INITIAL_WRITE_TRANSACTION);
        commitWithException(store, INITIAL_WRITE_TRANSACTION);

        final int T_ID = transactionId.incrementAndGet();
        final int T_ID_2 = transactionId.incrementAndGet();
        final int T_ID_3 = transactionId.incrementAndGet();

        store.begin(T_ID);
        int currentValue = store.read(KEY_1, T_ID);
        store.write(KEY_1, currentValue + INCR_VALUE, T_ID);

        final TransactionalKVStore.ReplayableTransaction INCREMENT_ACTION =
                new TransactionalKVStore.ReplayableTransaction() {
                    @Override
                    public void transaction(Object[] arguments, TransactionalKVStore
                            store) throws
                            RetryLaterException, InterruptedException {

                        final int REPLAYABLE_T_ID = transactionId.incrementAndGet();
                        System.out.println("Implementing an append");
                        store.begin(REPLAYABLE_T_ID);
                        Integer result = (Integer) store.read(KEY_1, REPLAYABLE_T_ID);
                        if (result == null)
                            result = 0; //if it does not exist, we're incrementing off of 0
                        int newValue = result + INCR_VALUE;
                        store.write(KEY_1, newValue, REPLAYABLE_T_ID);
                        store.commit(REPLAYABLE_T_ID);
                    }
                };

        store.submitReplayableTransaction(INCREMENT_ACTION, null, store, TransactionalKVStore.DEFAULT_MAX_HANDLED_ATTEMPTS);
        try {
            store.commit(T_ID);
            Assert.fail("Expected the initial transaction to fail after the replayable action was called.");
        } catch (RetryLaterException rte) {

            //The previous attempt failed, so that will invalidate the transaction. It needs to be re-run
            store.begin(T_ID_2);
            currentValue = store.read(KEY_1, T_ID_2);
            store.write(KEY_1, INCR_VALUE + currentValue, T_ID_2);
            commitWithException(store, T_ID_2);
        }
        // Get a read on the value
        store.begin(T_ID_3);
        int RESULT_AFTER_ONE_INCREMENT = store.read(KEY_1, T_ID_3);
        Assert.assertEquals("Increment on initial value did not take", INITIAL_VALUE + 2 * INCR_VALUE, RESULT_AFTER_ONE_INCREMENT);
        commitWithException(store, T_ID_3);
    }

    //TODO: Write tests to handle edge cases of two threads trying to initialize a value simultaneously

    @Test
    /**
     * Given:  A set of reads and writes to a particular key
     * Assert: The last-read and last-written metadata values are set properly
     */
    public void testMetadataUpdated() throws InterruptedException {

        final Date TRANSACTION_START_TIME = new Date();
        final TransactionalKVStore<String, Integer> store = new TransactionalKVStore<String, Integer>();

        final int INITIAL_READ_TRANSACTION = 0;
        final String KEY_1 = "key1";
        final int INITIAL_VALUE = 5;

        // Start one transaction on the value for client_side_increment
        store.begin(INITIAL_READ_TRANSACTION);
        store.read(KEY_1, INITIAL_READ_TRANSACTION);
        commitWithException(store, INITIAL_READ_TRANSACTION);

        MetadataValue<Integer> returnedValue = store.masterMap.get(KEY_1);
        Assert.assertNotNull("After initial read, the returned last read value was null.", returnedValue);

        final Date INITIAL_READ_TIME = returnedValue.getLastRead();
        System.out.println("initial read time: " + INITIAL_READ_TIME.toString());
        Assert.assertTrue("After a read, the last read time was not after the test start time.",
                TRANSACTION_START_TIME.before(INITIAL_READ_TIME));

        Assert.assertNull("No writes occurred, yet the write value is somehow not null", returnedValue.getLastWritten());

        final Date WRITE_TRANSACTION_START_TIME = new Date();

        //verify that last read value unchanged after write
        final int WRITE_TRANSACTION = INITIAL_READ_TRANSACTION + 1;
        store.begin(WRITE_TRANSACTION);
        store.write(KEY_1, INITIAL_VALUE, WRITE_TRANSACTION);
        commitWithException(store, WRITE_TRANSACTION);

        Date newLastReadTime = store.masterMap.get(KEY_1).getLastRead();
        System.out.println("new last read time: " + newLastReadTime.toString());
        Date newLastWrittenTime = store.masterMap.get(KEY_1).getLastWritten();
        Assert.assertEquals("Last read time updated upon write, which should not happen", INITIAL_READ_TIME, newLastReadTime);

        // Now verify the write time actually set
        Assert.assertTrue("After a write, the last write time was not after the last read time.",
                newLastWrittenTime.after(WRITE_TRANSACTION_START_TIME));
    }

    @Test
    /**
     * Given:  A transaction that writes a piece of data that invalidates a read
     * Assert: The transaction to be committed understands that its state has been invalidated
     *         and understands it needs to be rolled back.
     */
    public void testRollBackOnWriteThatInvalidatesState() throws InterruptedException, RetryLaterException {

        final TransactionalKVStore<String, Integer> tctStore = new TransactionalKVStore<String, Integer>();

        final String KEY_1 = "key1";
        final Integer INITIAL_VALUE = 55;
        final Integer UPDATED_VALUE = INITIAL_VALUE + 1;
        final Integer TRANSACTION_SET_INITIAL_VALUE = 1;
        final Integer TRANSACTION_FAILED_EXPECTED_READ = 2;
        final Integer TRANSACTION_SUCCESSFUL_WRITE = 3;

        // Set the initial value
        tctStore.begin(TRANSACTION_SET_INITIAL_VALUE);
        tctStore.write(KEY_1, INITIAL_VALUE, TRANSACTION_SET_INITIAL_VALUE);
        commitWithException(tctStore, TRANSACTION_SET_INITIAL_VALUE);

        //Start a transaction that will do a read
        tctStore.begin(TRANSACTION_FAILED_EXPECTED_READ);
        tctStore.read(KEY_1, TRANSACTION_FAILED_EXPECTED_READ);

        //Start a transaction that will commit a write
        tctStore.begin(TRANSACTION_SUCCESSFUL_WRITE);
        tctStore.write(KEY_1, UPDATED_VALUE, TRANSACTION_SUCCESSFUL_WRITE);
        commitWithException(tctStore, TRANSACTION_SUCCESSFUL_WRITE);

        Assert.assertTrue("Read transaction needed to have been rolled back", tctStore.needToRollBack(TRANSACTION_FAILED_EXPECTED_READ));

        try {
            tctStore.commit(TRANSACTION_FAILED_EXPECTED_READ);
            Assert.fail("Transaction should thrown an exception upon attempting to commit a transaction that needed to be rolled back ");
        } catch (RetryLaterException rte) {

            rte.getLocalizedMessage().contains(KEY_1);
        }
    }

    @Test
    /**
     * Given:  Two transaction that attempt to append the same value that has not yet been set.
     * Assert: This is handled elegantly with the transaction with the later commit time rejected
     * Notes:  This is a corner case, in the sense that we are appending on values that have
     *         not yet been set.
     */
    public void testInitialAppend() throws InterruptedException {

        final TransactionalKVStore<String, Integer> store = new TransactionalKVStore<String, Integer>();


        final Integer T_ID_1 = 1;
        final Integer T_ID_2 = 2;
        final Integer T_ID_3 = 3;
        final Integer T_ID_4 = 4;

        store.begin(T_ID_1);
        store.begin(T_ID_2);
        store.begin(T_ID_3);
        store.begin(T_ID_4);

        /*
        *  Note that reading a null (uninitialized value) is perfectly acceptable. Why?
        *  This is acceptable because the application logic will know how to handle an
        *  uninitialized value. That also means that this read will be considered when
        *  determining if this transaction should be rolled back.
        */
        store.read(KEY_1, T_ID_2);

        // A write occurred on this. Should this be committed, it invalidates any reads on this
        // key performed before the commit was made.
        store.write(KEY_1, VALUE_1, T_ID_1);

        store.read(KEY_1, T_ID_3);

        // Commit write - the effect here is that you invalidate the key for all other transactions,
        // namely T_ID_1 and T_ID_3
        commitWithException(store, T_ID_2);

        // Check that write started before commit transaction (T2) was not invalidated
        Assert.assertFalse("The write transaction, " + T_ID_1 + " did not need to have been rolled back", (store.needToRollBack(T_ID_1)));

        // Check that read started before commit transaction (T2) was not invalidated
        Assert.assertFalse("The read transaction, " + T_ID_3 + " did not need to have been rolled back", (store.needToRollBack(T_ID_3)));

        // Commit the write
        commitWithException(store, T_ID_1);

        // Now verify that the read transaction would need to be rolled back
        Assert.assertTrue("The read transaction, " + T_ID_3 + " needed to have been rolled back but was not", (store.needToRollBack(T_ID_3)));
    }

    /**
     * Given:  A transaction that only has a begin and a commit
     * Assert: The server treats this as an invalid transaction
     */
    @Test
    public void testBeginAndCommitWithoutTransactions() throws InterruptedException {

        final TransactionalKVStore<String, Integer> store = new TransactionalKVStore<String, Integer>();
        final Integer T_ID_1 = 1;
        store.begin(T_ID_1);
        commitWithException(store, T_ID_1);
    }

    @Test
    public void testBeginningSameTransactionTwice() throws InterruptedException {

        final TransactionalKVStore<String, Integer> store = new TransactionalKVStore<String, Integer>();
        final Integer T_ID_1 = 1;
        store.begin(T_ID_1);
        try {
            store.begin(T_ID_1);
            Assert.fail("Transaction was started more than once yet no exception was thrown");
        } catch (RuntimeException rte) {

            Assert.assertTrue(rte.getLocalizedMessage().contains("because it already exists"));
        }
    }

    @Test
    public void testCommittingSameTransactionTwice() throws InterruptedException {

        final TransactionalKVStore<String, Integer> store = new TransactionalKVStore<String, Integer>();
        final Integer T_ID_1 = 1;
        store.begin(T_ID_1);
        store.write(KEY_1, VALUE_1, T_ID_1);
        commitWithException(store, T_ID_1);

        try {

            store.commit(T_ID_1);
            Assert.fail("Should not have been able to commit twice on same transactionId, " +
                    T_ID_1);
        } catch (NoSuchTransactionException nst) {
            Assert.assertTrue(nst.getLocalizedMessage().contains(String.valueOf(T_ID_1)));
        } catch (RetryLaterException rte) {
            Assert.fail("Incorrect exception type thrown");
        }
    }
}