import junit.framework.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by aleks on 1/4/16.
 */
public class TestTransactionsWithRemove {

    final static String KEY_1 = "key1";
    final static Integer VALUE_1 = 1;
    public static AtomicInteger sharedTransactionCounter = new AtomicInteger(0);


    @Test
    public void testBasicRemove() throws InterruptedException {

        //Create a KV store

        // Remove an entry

        // Verify that it's no longer there
        final int T_ID_1 = sharedTransactionCounter.incrementAndGet();

        TransactionalKVStore<String, Integer> store = new TransactionalKVStore<String, Integer>();
        store.begin(T_ID_1);
        store.write(KEY_1, VALUE_1, T_ID_1);
        final Integer returnedValue = store.read(KEY_1, T_ID_1);
        Assert.assertEquals("The written value did not match the value returned", VALUE_1,
                returnedValue);

        store.remove(KEY_1, T_ID_1);


    }
}
