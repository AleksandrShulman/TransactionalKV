import org.junit.Test;

/**
 * Created by aleks on 12/24/15.
 */
public class TestServerSideTransactions {

    @Test
    public void testBasicServerSideWrite() throws InterruptedException {

        final TransactionalKVStore<String, Integer> store = new TransactionalKVStore<String, Integer>();
        final int INITIAL_T_ID = 0;
        final String KEY_1 = "key1";
        final Integer VALUE_1 = 55;

        // Create a replayable transaction
        TransactionalKVStore.ReplayableTransactionWrapper rtw = new TransactionalKVStore.ReplayableTransactionWrapper() {
            @Override
            public void runReplayableTransaction(Object[] arguments, TransactionalKVStore store, int maxAttempts)
                    throws
                    RetryLaterException, InterruptedException {

                System.out.println("Here is a transaction");
                store.begin(INITIAL_T_ID);
                store.read("KEY_1", INITIAL_T_ID);
                store.write(KEY_1, VALUE_1, INITIAL_T_ID);
                store.commit(INITIAL_T_ID);
            }
        };

        store.submitReplayableTransaction(rtw, null, store);
    }
}
