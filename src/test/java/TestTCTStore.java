import junit.framework.Assert;
import org.junit.Test;

/**
 * Created by aleks on 10/22/15.
 */
public class TestTCTStore {

    @Test
    public void testBasicWriteAndRetrieve() {

        final int T_ID = 1;
        final String KEY = "MeaningOfLife";
        final Integer VALUE = 42;

        //Write transaction
        TimeConsistentTransactionalKVStore<String, Integer> tctStore = new TimeConsistentTransactionalKVStore<String, Integer>();
        tctStore.begin(T_ID);
        tctStore.write(KEY, VALUE, T_ID);
        try {
            tctStore.commit(T_ID);

        } catch (RetryLaterException rte) {
            throw new IllegalStateException("Test transaction hit a situation where it was asked to retry, but for no apparent reason");
        }

        final int T_ID_2 = T_ID + 1;
        tctStore.begin(T_ID_2);
        Integer result = tctStore.read(KEY, T_ID_2);

        Assert.assertEquals("Results did not match on basic read!", VALUE, result);

        try {
            tctStore.commit(T_ID_2);

        } catch (RetryLaterException rte) {
            throw new IllegalStateException("Test transaction hit a situation where it was asked to retry, but for no apparent reason");
        }
    }

    @Test
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
        tctStore.write(KEY, VALUE, T_ID);
        try {
            tctStore.commit(T_ID);

        } catch (RetryLaterException rte) {
            throw new IllegalStateException("Test transaction hit a situation where it was asked to retry, but for no apparent reason");
        }

        tctStore.begin(T_ID_2);
        tctStore.write(KEY, NEW_VALUE, T_ID_2);
        try {
            tctStore.commit(T_ID_2);

        } catch (RetryLaterException rte) {
            throw new IllegalStateException("Test transaction hit a situation where it was asked to retry, but for no apparent reason");
        }


        tctStore.begin(T_ID_3);
        Integer result = tctStore.read(KEY, T_ID_3);

        Assert.assertEquals("Results did not match on basic read!", NEW_VALUE, result);

        try {
            tctStore.commit(T_ID_3);

        } catch (RetryLaterException rte) {
            throw new IllegalStateException("Test transaction hit a situation where it was asked to retry, but for no apparent reason");
        }
    }

    @Test
    /* Given that we are going to have multiple overlapping writes on non-common keys
     * Verify that all writes get committed
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

        try {
            tctStore.commit(VERIFY_TRANSACTION);
        } catch (RetryLaterException rte) {
            System.out.print(rte.getLocalizedMessage());
            throw new IllegalStateException("Failed while committing verify transaction");
        }
    }
}
