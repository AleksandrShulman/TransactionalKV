import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by aleks on 10/6/15.
 */


public class TestInteractiveTransactionStore {

    final int MAX_FAILED_ATTEMPTS_TO_LOCK = 5;

    @Test
    public void testIncrement() throws InterruptedException {

        //Initialize with initial value
        InteractiveTransactionalKVStore<String, Integer> ikv = new InteractiveTransactionalKVStore<String, Integer>();
        final int FIRST_COMMIT = 0;
        final String KEY = "key1";
        final int VALUE1 = (int) Math.random() * 50;
        final int INCREMENT_AMOUNT = (int) Math.random() * 100;
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
        int returnedFinalValue = ikv.read(KEY, SECOND_COMMIT);

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
     * Verify that the code that handles the double-buffered data structure does so without losing data.
     * This is a white-box test that tries to expose potential weaknesses in how we handle temporary data
     * structures in the KV store.
     */
    public void testCommitSwapDataIntegrity() throws InterruptedException {

        //Create a few transactions that place data in K1, K2, and K3
        final String KEY_1 = "key1";
        final String KEY_2 = "key2";
        final String KEY_3 = "key3";
        final String KEY_4 = "key4";

        final Integer VALUE_1 =(int) (Math.random() * 50);
        final Integer VALUE_2 =(int) (Math.random() * 50);
        final Integer VALUE_3 =(int) (Math.random() * 50);
        final Integer VALUE_4 =(int) (Math.random() * 50);

        //Initialize with the first set of values
        final Map<String,Integer> initialDataMap = new HashMap<String, Integer>();
        initialDataMap.put(KEY_1, VALUE_1);
        initialDataMap.put(KEY_2, VALUE_2);
        initialDataMap.put(KEY_3, VALUE_3);

        InteractiveTransactionalKVStore<String, Integer> ikv = new InteractiveTransactionalKVStore<String, Integer>();
        final int FIRST_COMMIT = 0;
        final int SECOND_COMMIT = FIRST_COMMIT+1;
        final int THIRD_COMMIT = SECOND_COMMIT+1;
        final int FOURTH_COMMIT = THIRD_COMMIT+1;
        final int FIFTH_COMMIT = FOURTH_COMMIT+1;

        final List<String> INITIAL_KEY_LIST = new ArrayList<String>();
        INITIAL_KEY_LIST.add(KEY_1);
        INITIAL_KEY_LIST.add(KEY_2);
        INITIAL_KEY_LIST.add(KEY_3);

        beginAndWait(ikv, FIRST_COMMIT, INITIAL_KEY_LIST, MAX_FAILED_ATTEMPTS_TO_LOCK);
        for (String key : initialDataMap.keySet()) {

            ikv.write(key, initialDataMap.get(key), FIRST_COMMIT);
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
        for (String key : initialDataMap.keySet()) {

            Assert.assertEquals(initialDataMap.get(key),ikv.read(key, THIRD_COMMIT));
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
        initialDataMap.put(KEY_2, NEW_VALUE_2);
        initialDataMap.put(KEY_3, NEW_VALUE_3);
        initialDataMap.put(KEY_4, VALUE_4);

        INITIAL_KEY_LIST.add(KEY_4);

        beginAndWait(ikv, FIFTH_COMMIT, INITIAL_KEY_LIST, MAX_FAILED_ATTEMPTS_TO_LOCK);
        for (String key : initialDataMap.keySet()) {

            Assert.assertEquals(initialDataMap.get(key),ikv.read(key, FIFTH_COMMIT));
        }

        ikv.commit(FIFTH_COMMIT);
    }
}