import com.sun.org.apache.bcel.internal.generic.RET;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by aleks on 10/22/15.
 */
public class TestTCTStore {

    AtomicInteger sharedTransactionCounter = new AtomicInteger(0);
    final int DEFAULT_MAX_FAILED_COMMIT_ATTEMPTS = 5;

    @Test
    /**
     * Given: A simple TCTStore
     * Assert that: A value can be written and then retrieved.
     */
    public void testBasicWriteAndRetrieve() {

        final int T_ID = 1;
        final String KEY = "MeaningOfLife";
        final Integer VALUE = 42;

        //Write transaction
        TimeConsistentTransactionalKVStore<String, Integer> tctStore = new TimeConsistentTransactionalKVStore<String, Integer>();
        tctStore.begin(T_ID);
        tctStore.write(KEY, VALUE, T_ID);

        commitWithException(tctStore, T_ID);

        final int T_ID_2 = T_ID + 1;
        tctStore.begin(T_ID_2);
        Integer result = tctStore.read(KEY, T_ID_2);

        Assert.assertEquals("Results did not match on basic read!", VALUE, result);
        commitWithException(tctStore, T_ID_2);
    }

    //TODO: Write tests to verify last read and write time in master
    @Test
    /**
     * Given: A set of read and writes on a given value
     * Assert that: Read and write times on the values in the master KV store are updated
     */
    public void testMasterReadAndWriteTimes() throws InterruptedException {

        final Date START_TIME = new Date();

        //Sleep to make sure that time increases
        Thread.sleep(1000);

        String KEY_1 = "key1";
        Integer VALUE_1 = 99;

        final int T_ID = 1;
        final int T_ID_2 = T_ID + 1;

        TimeConsistentTransactionalKVStore<String, Integer> tctStore = new TimeConsistentTransactionalKVStore<String, Integer>();
        tctStore.begin(T_ID);
        tctStore.write(KEY_1, VALUE_1, T_ID);
        commitWithException(tctStore, T_ID);

        //This will work because it's package-protected. The other option is to add
        //a getter that's only available in the testing context
        Date lastWritten = tctStore.masterMap.get(KEY_1).getLastWritten();
        Assert.assertTrue(lastWritten.after(START_TIME));

        Thread.sleep(1000);
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
    public void testBasicOverwriteAndRetrieve() {

        final int T_ID = 1;
        final int T_ID_2 = T_ID + 1;
        final int T_ID_3 = T_ID_2 + 1;

        final String KEY = "MeaningOfLife";
        final Integer VALUE = 42;
        final Integer NEW_VALUE = 43;

        //Write transaction
        TimeConsistentTransactionalKVStore<String, Integer> tctStore = new TimeConsistentTransactionalKVStore<String, Integer>();
        tctStore.begin(T_ID);
        commitWithException(tctStore, T_ID);

        tctStore.begin(T_ID_2);
        tctStore.write(KEY, NEW_VALUE, T_ID_2);
        commitWithException(tctStore, T_ID_2);

        tctStore.begin(T_ID_3);
        Integer result = tctStore.read(KEY, T_ID_3);

        Assert.assertEquals("Results did not match on basic read!", NEW_VALUE, result);
        commitWithException(tctStore, T_ID_3);
    }

    @Test
    /**
     * Given: Multiple overlapping writes on non-common keys
     * Assert that: All writes get committed and all transactions complete
     */
    public void testOverlappingWritesOnDifferentKeys() {

        final int T_ID = 1;
        final int T_ID_2 = T_ID + 1;
        final int T_ID_3 = T_ID_2 + 1;

        final String KEY_1 = "key1";
        final String KEY_2 = "key2";
        final String KEY_3 = "key3";

        final int VALUE_1 = 1;
        final int VALUE_2 = 2;
        final int VALUE_3 = 1;

        final int VERIFY_TRANSACTION = T_ID_3 + 1;

        TimeConsistentTransactionalKVStore<String, Integer> tctStore = new TimeConsistentTransactionalKVStore<String, Integer>();

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
     * Given: Overlapping commits that increment a value
     * Assert that: Both increments are considered
     *
     * WARNING: This test fails when run as part of all tests, but passes when run in isolation...
     */
    public void testConcurrentIncrements() throws InterruptedException {

        final TimeConsistentTransactionalKVStore<String, Integer> tctStore = new TimeConsistentTransactionalKVStore<String, Integer>();

        // Because this test runs single-threaded, on a faster machine, there will not be enough time change
        // to cause the transaction manager to detect stale data. So, adding a 1ms sleep is useful here.
        final int TIME_SPACER_MS = 1;

        // Initialize a value
        final int INITIAL_WRITE_TRANSACTION = sharedTransactionCounter.incrementAndGet();
        final int FIRST_INCREMENT_TRANSACTION = sharedTransactionCounter.incrementAndGet();
        final int SECOND_INCREMENT_TRANSACTION = sharedTransactionCounter.incrementAndGet();
        final int VERIFY_TRANSACTION = sharedTransactionCounter.incrementAndGet();

        final String KEY_1 = "key1";
        final int INITIAL_VALUE = 5;
        final int INCREMENT_AMOUNT_1 = 8;
        final int INCREMENT_AMOUNT_2 = 13;
        final int EXPECTED_FINAL_VALUE = INITIAL_VALUE + INCREMENT_AMOUNT_1 + INCREMENT_AMOUNT_2;

        // Start one transaction on the value for increment
        tctStore.begin(INITIAL_WRITE_TRANSACTION);
        tctStore.write(KEY_1, INITIAL_VALUE, INITIAL_WRITE_TRANSACTION);
        commitWithException(tctStore, INITIAL_WRITE_TRANSACTION);

        // Start a second transaction before the first one is committed
        tctStore.begin(FIRST_INCREMENT_TRANSACTION);
        tctStore.begin(SECOND_INCREMENT_TRANSACTION);
        Thread.sleep(TIME_SPACER_MS);

        //Client 1 doing a basic increment
        int preFirstIncrementValue = tctStore.read(KEY_1, FIRST_INCREMENT_TRANSACTION);
        int postFirstIncrementValue = preFirstIncrementValue + INCREMENT_AMOUNT_1;
        tctStore.write(KEY_1, postFirstIncrementValue, FIRST_INCREMENT_TRANSACTION);

        //Client 2 doing a basic increment
        int preSecondIncrementValue = tctStore.read(KEY_1, SECOND_INCREMENT_TRANSACTION);

        int postSecondIncrementValue = preSecondIncrementValue + INCREMENT_AMOUNT_2;
        tctStore.write(KEY_1, postSecondIncrementValue, SECOND_INCREMENT_TRANSACTION);
        Thread.sleep(TIME_SPACER_MS);

        // Commit the transactions in the order that they were issued
        commitWithException(tctStore, FIRST_INCREMENT_TRANSACTION);
        try {
            tctStore.commit(SECOND_INCREMENT_TRANSACTION);
            Assert.fail("The server did not instruct the client to retry");
        } catch (RetryLaterException rte) {

            increment(tctStore, KEY_1, INCREMENT_AMOUNT_2, DEFAULT_MAX_FAILED_COMMIT_ATTEMPTS);
        }

        // Verify the final value is correct
        tctStore.begin(VERIFY_TRANSACTION);
        final int RETURNED_VALUE = tctStore.read(KEY_1, VERIFY_TRANSACTION);

        Assert.assertEquals("Increments overwrote one another!", EXPECTED_FINAL_VALUE, RETURNED_VALUE);
        commitWithException(tctStore, VERIFY_TRANSACTION);
    }

    private void commitWithException(final TimeConsistentTransactionalKVStore tctStore, final int TRANSACTION_ID) {

        try {
            tctStore.commit(TRANSACTION_ID);
        } catch (RetryLaterException rte) {
            System.out.print(rte.getLocalizedMessage());
            throw new IllegalStateException("Failed while committing transaction " + TRANSACTION_ID);
        }
    }

    /**
     * Because sometimes we're not able to succeed right away, a user will need to have retry logic. In this case,
     * I implemented the business logic to increment. A user will need to write something similar to this for
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
    private void increment(TimeConsistentTransactionalKVStore<String, Integer> store, final String KEY, final int AMOUNT, final int MAX_FAILED_ATTEMPTS) throws InterruptedException {

        int attempts = 0;
        while (true) {

            try {

                //do some logic
                int transactionId = sharedTransactionCounter.incrementAndGet();
                store.begin(transactionId);
                Integer someValue = store.read(KEY, transactionId);
                Integer newValue = someValue + AMOUNT;
                store.write(KEY, newValue, transactionId);

                //commit
                store.commit(transactionId);

                // if no exception thrown
                break;
            } catch (RetryLaterException rte) {

                Thread.sleep(rte.getWaitTimeMs());
                attempts++;
                if (attempts > MAX_FAILED_ATTEMPTS) {
                    throw new RuntimeException("Could not commit transaction, even after " + MAX_FAILED_ATTEMPTS + " attempts");
                }
            }
        }
    }
}
