import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by aleks on 10/6/15.
 */


public class TestInteractiveTransactionStore {

    private static final Logger logger =
            Logger.getLogger(TestTransactionStore.class.getName());

    static {
        logger.setLevel(Level.FINE);
    }

    final int MAX_FAILED_ATTEMPTS_TO_LOCK = 5;

    @Test
    /**
     * Given: A known value that is incremented by a transaction
     * Assert: The returned value is correct
     */
    public void testIncrement() throws InterruptedException {

        //Initialize with initial value
        InteractiveTransactionalKVStore<String, Integer> ikv = new InteractiveTransactionalKVStore<String, Integer>();
        final int FIRST_COMMIT = 0;
        final String KEY = "key1";
        final int VALUE1 = (int) Math.random() * 50;
        final int INCREMENT_AMOUNT = (int) (Math.random() * 100);
        final int EXPECTED_FINAL_VALUE = VALUE1 + INCREMENT_AMOUNT;
        final List<String> INITIAL_KEY_LIST = new ArrayList<String>();
        INITIAL_KEY_LIST.add(KEY);

        //Insert the initial value
        beginAndWait(ikv, FIRST_COMMIT, INITIAL_KEY_LIST, MAX_FAILED_ATTEMPTS_TO_LOCK);
        ikv.write(KEY, VALUE1, FIRST_COMMIT);
        ikv.commit(FIRST_COMMIT);

        //Now perform the increment
        int SECOND_COMMIT = FIRST_COMMIT + 1;
        beginAndWait(ikv, SECOND_COMMIT, INITIAL_KEY_LIST, MAX_FAILED_ATTEMPTS_TO_LOCK);
        int returnedInitialValue = ikv.read(KEY, SECOND_COMMIT);
        int newValue = returnedInitialValue + INCREMENT_AMOUNT;
        ikv.write(KEY, newValue, SECOND_COMMIT);
        int returnedIncrementedValue = ikv.read(KEY, SECOND_COMMIT);
        ikv.commit(SECOND_COMMIT);

        Assert.assertEquals("Upon re-read in the same transaction, " +
                "the returned value was incorrect", (int) EXPECTED_FINAL_VALUE, (int) returnedIncrementedValue);

        // Final transaction to read the value
        int THIRD_COMMIT = SECOND_COMMIT + 1;
        beginAndWait(ikv, THIRD_COMMIT, INITIAL_KEY_LIST, MAX_FAILED_ATTEMPTS_TO_LOCK);
        int returnedFinalValue = ikv.read(KEY, THIRD_COMMIT);

        Assert.assertEquals("Upon re-query, the KV store " +
                "returned an incorrect value.", (int) EXPECTED_FINAL_VALUE, (int) returnedFinalValue);
    }


    public void beginAndWait(InteractiveTransactionalKVStore kv, int transactionId,
                             List<String> keysToLock, int MAX_ATTEMPTS) throws InterruptedException {

        int attempts = 0;
        while (attempts < MAX_ATTEMPTS)
            try {
                attempts++;
                kv.begin(transactionId, keysToLock);
                break;
            } catch (RetryLaterException rtl) {
                int waitTimeMs = rtl.getWaitTimeMs();

                // if we get interrupted, then probably we should bubble up the exception
                Thread.sleep(waitTimeMs);
            }
        if (attempts == MAX_ATTEMPTS)
            throw new RuntimeException("Could not obtain a lock on the keys for the transaction");
    }

    @Test
    /**
     * Given: A transaction that attempts to lock keys that are part of a transaction already begun, but not yet committed
     * Assert: The second transaction cannot obtain a lock on those keys and proper error handling is done
     */
    public void testLockingCausesException() throws InterruptedException {

        InteractiveTransactionalKVStore<String, Integer> ikv = new InteractiveTransactionalKVStore<String, Integer>();
        final int FIRST_COMMIT = 0;

        final String KEY = "key1";
        final int VALUE1 = (int) (Math.random() * 50);

        final List<String> INITIAL_KEY_LIST = new ArrayList<String>();
        INITIAL_KEY_LIST.add(KEY);

        //Insert the initial value
        beginAndWait(ikv, FIRST_COMMIT, INITIAL_KEY_LIST, MAX_FAILED_ATTEMPTS_TO_LOCK);
        ikv.write(KEY, VALUE1, FIRST_COMMIT);

        final int SECOND_COMMIT = FIRST_COMMIT + 1;
        try {
            ikv.begin(SECOND_COMMIT, INITIAL_KEY_LIST);
            Assert.fail("Exception not thrown when attempting to request lock");
        } catch (RetryLaterException rtl) {

            Assert.assertTrue("Exception message not useful. ", rtl.getLocalizedMessage().contains("wait"));
            Assert.assertTrue("Wait time less than zero or not initialized. ", rtl.getWaitTimeMs() > 0);
        }
    }

    @Test
    /**
     * Given: Initial KV values for key{1..3}, perform an update key{2..3} and then an insert on key4.
     * Assert that: The final values are correct
     *
     * Explanation: The code that handles the double-buffered data structure does so without losing data.
     * This is a white-box test that tries to expose potential weaknesses in how we handle temporary data
     * structures in the KV store.
     */
    public void testCommitSwapDataIntegrity() throws InterruptedException {

        //Create a few transactions that place data in K1, K2, and K3
        final String KEY_1 = "key1";
        final String KEY_2 = "key2";
        final String KEY_3 = "key3";
        final String KEY_4 = "key4";

        final Integer VALUE_1 = (int) (Math.random() * 50);
        final Integer VALUE_2 = (int) (Math.random() * 50);
        final Integer VALUE_3 = (int) (Math.random() * 50);
        final Integer VALUE_4 = (int) (Math.random() * 50);

        //Data map will store what we imagine the values to be in the KV
        final Map<String, Integer> expectedKVs = new HashMap<String, Integer>();
        expectedKVs.put(KEY_1, VALUE_1);
        expectedKVs.put(KEY_2, VALUE_2);
        expectedKVs.put(KEY_3, VALUE_3);

        InteractiveTransactionalKVStore<String, Integer> ikv = new InteractiveTransactionalKVStore<String, Integer>();
        final int FIRST_COMMIT = 0;
        final int SECOND_COMMIT = FIRST_COMMIT + 1;
        final int THIRD_COMMIT = SECOND_COMMIT + 1;
        final int FOURTH_COMMIT = THIRD_COMMIT + 1;
        final int FIFTH_COMMIT = FOURTH_COMMIT + 1;

        final List<String> INITIAL_KEY_LIST = new ArrayList<String>();
        INITIAL_KEY_LIST.add(KEY_1);
        INITIAL_KEY_LIST.add(KEY_2);
        INITIAL_KEY_LIST.add(KEY_3);

        beginAndWait(ikv, FIRST_COMMIT, INITIAL_KEY_LIST, MAX_FAILED_ATTEMPTS_TO_LOCK);
        for (String key : expectedKVs.keySet()) {

            ikv.write(key, expectedKVs.get(key), FIRST_COMMIT);
        }

        ikv.commit(FIRST_COMMIT);

        //Perform a transaction on K4
        List<String> justK4 = new ArrayList<String>();
        justK4.add(KEY_4);

        beginAndWait(ikv, SECOND_COMMIT, justK4, MAX_FAILED_ATTEMPTS_TO_LOCK);
        ikv.write(KEY_4, VALUE_4, SECOND_COMMIT);
        ikv.commit(SECOND_COMMIT);

        //Verify data from K1, K2, and K3 is present.
        beginAndWait(ikv, THIRD_COMMIT, INITIAL_KEY_LIST, MAX_FAILED_ATTEMPTS_TO_LOCK);
        for (String key : expectedKVs.keySet()) {

            Assert.assertEquals(expectedKVs.get(key), ikv.read(key, THIRD_COMMIT));
        }

        ikv.commit(THIRD_COMMIT);

        List<String> k2k3 = new ArrayList<String>();
        k2k3.add(KEY_2);
        k2k3.add(KEY_3);

        //Perform a transaction on K2 and K3.
        beginAndWait(ikv, FOURTH_COMMIT, k2k3, MAX_FAILED_ATTEMPTS_TO_LOCK);
        int NEW_VALUE_2 = VALUE_2 + 1;
        int NEW_VALUE_3 = VALUE_3 + 1;
        ikv.write(KEY_2, NEW_VALUE_2, FOURTH_COMMIT);
        ikv.write(KEY_3, NEW_VALUE_3, FOURTH_COMMIT);

        ikv.commit(FOURTH_COMMIT);

        //Verify that data from K1, K2, K3, and K4 is present.
        expectedKVs.put(KEY_2, NEW_VALUE_2);
        expectedKVs.put(KEY_3, NEW_VALUE_3);
        expectedKVs.put(KEY_4, VALUE_4);

        INITIAL_KEY_LIST.add(KEY_4);

        beginAndWait(ikv, FIFTH_COMMIT, INITIAL_KEY_LIST, MAX_FAILED_ATTEMPTS_TO_LOCK);
        for (String key : expectedKVs.keySet()) {

            Assert.assertEquals(expectedKVs.get(key), ikv.read(key, FIFTH_COMMIT));
        }

        ikv.commit(FIFTH_COMMIT);
    }

    @Test
    /**
     * Given: A set of simultaneous commits on key{2,3} and key4
     * Asser that: Outcome is correct once all values are committed
     */
    public void testInterleavedCommit() throws InterruptedException {

        //add some data
        final String KEY_1 = "key1";
        final String KEY_2 = "key2";
        final String KEY_3 = "key3";
        final String KEY_4 = "key4";

        final Integer VALUE_1 = (int) (Math.random() * 50);
        final Integer VALUE_2 = (int) (Math.random() * 50);
        final Integer VALUE_3 = (int) (Math.random() * 50);
        final Integer VALUE_4 = (int) (Math.random() * 50);

        //Initialize with the first set of values. Data structure reflects the expected source of truth for the system
        final Map<String, Integer> expectedKVs = new HashMap<String, Integer>();
        expectedKVs.put(KEY_1, VALUE_1);
        expectedKVs.put(KEY_2, VALUE_2);
        expectedKVs.put(KEY_3, VALUE_3);

        InteractiveTransactionalKVStore<String, Integer> ikv = new InteractiveTransactionalKVStore<String, Integer>();
        final int FIRST_COMMIT = 0;
        final int SECOND_COMMIT = FIRST_COMMIT + 1;
        final int THIRD_COMMIT = SECOND_COMMIT + 1;
        final int FOURTH_COMMIT = THIRD_COMMIT + 1;

        final List<String> INITIAL_KEY_LIST = new ArrayList<String>();
        INITIAL_KEY_LIST.add(KEY_1);
        INITIAL_KEY_LIST.add(KEY_2);
        INITIAL_KEY_LIST.add(KEY_3);

        beginAndWait(ikv, FIRST_COMMIT, INITIAL_KEY_LIST, MAX_FAILED_ATTEMPTS_TO_LOCK);
        for (String key : expectedKVs.keySet()) {

            ikv.write(key, expectedKVs.get(key), FIRST_COMMIT);
        }

        ikv.commit(FIRST_COMMIT);

        //perform a transaction on those keys, but do not commit
        List<String> k2k3 = new ArrayList<String>();
        k2k3.add(KEY_2);
        k2k3.add(KEY_3);

        beginAndWait(ikv, SECOND_COMMIT, k2k3, MAX_FAILED_ATTEMPTS_TO_LOCK);
        int NEW_VALUE_2 = VALUE_2 + 1;
        int NEW_VALUE_3 = VALUE_3 + 1;
        ikv.write(KEY_2, NEW_VALUE_2, SECOND_COMMIT);
        ikv.write(KEY_3, NEW_VALUE_3, SECOND_COMMIT);

        expectedKVs.put(KEY_2, NEW_VALUE_2);
        expectedKVs.put(KEY_3, NEW_VALUE_3);

        //start performing another transaction on another set of keys
        List<String> k4 = new ArrayList<String>();
        k2k3.add(KEY_4);
        beginAndWait(ikv, THIRD_COMMIT, k4, MAX_FAILED_ATTEMPTS_TO_LOCK);
        ikv.write(KEY_4, VALUE_4, THIRD_COMMIT);

        //Commit in reverse order
        ikv.commit(THIRD_COMMIT);
        ikv.commit(SECOND_COMMIT);

        expectedKVs.put(KEY_4, VALUE_4);
        beginAndWait(ikv, FOURTH_COMMIT, new ArrayList(expectedKVs.keySet()), MAX_FAILED_ATTEMPTS_TO_LOCK);

        //verify all data there
        for (String key : expectedKVs.keySet()) {
            logger.fine("Check that KV contains key " + key + " with expected value " + expectedKVs.get(key));
            Assert.assertEquals(expectedKVs.get(key), ikv.read(key, FOURTH_COMMIT));
        }
    }
}
