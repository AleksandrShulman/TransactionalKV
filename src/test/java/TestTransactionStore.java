import org.junit.*;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A basic KV store that supports atomic sets of reads and writes. This isn't a truly atomic store because
 * there is no all-or-nothing behavior in the case of failure.
 */
public class TestTransactionStore<K, V> {

    private static final Logger logger =
            Logger.getLogger(TestTransactionStore.class.getName());

    static {
        logger.setLevel(Level.INFO);
    }

    @Before
    public void testSetup() {

        logger.info("Starting test...");
    }

    @After
    public void testTeardown() {


        logger.info("Ending test...");
    }


    //Here's a question - how are we supposed to get intermediate values back from Read if this is one transaction and
    // nothing actually gets executed until the commit?

    // Either the append is done server-side, which would make it more like a trigger/coprocessor, or
    // Otherwise, we'd need to somehow stop, mid-transaction, get the new value, and then continue. Also not
    // a realistic possibility

    // Maybe you can send it back, but then not allow any transactions to execute that have that value as a key??
    // What if the client takes forever? We can add a reasonable timeout. Might involve a callback. Ugly.

    // But it's super-ugly to have a transaction span multiple machines
    @Test
    @Ignore
    public void testSimultaneousAppends() {

        TransactionalKVStore<String, Integer> kvStore = new TransactionalKVStore<String, Integer>();
        //TODO: Finish this when implementing returns from a read operation that hasn't been committed
    }

    @Test
    public void testBasicReadWrite() {

        TransactionalKVStore<String, Integer> kvStore = new TransactionalKVStore<String, Integer>();

        final String KEY1 = "key1";
        final int VALUE1 = 5;

        int transactionId = 0;
        kvStore.begin(transactionId);
        kvStore.write(KEY1, VALUE1, transactionId);
        kvStore.read(KEY1, transactionId);

        List<TransactionalKVStore.TransactionalUnit<String, Integer>> committedTransactions = kvStore.commit(transactionId);
        TransactionalKVStore.TransactionalUnit<String, Integer> readOutput = committedTransactions.get(1);
        Assert.assertEquals((Integer) VALUE1, (Integer) readOutput.getValue());
    }

    @Test
    // Verify that a multi-read transaction conducted at the same time as an atomic set of writes on those values produces
    // results that are on consistent
    public void testAtomicWrites() {

        TransactionalKVStore<String, Integer> kvStore = new TransactionalKVStore<String, Integer>();

        final int INITIAL_WRITE_TRANSACTION = 0;
        final int INITIAL_READ_TRANSACTION = INITIAL_WRITE_TRANSACTION + 1;
        final int INCREMENT_TRANSACTION = INITIAL_READ_TRANSACTION + 1;

        final int INCREMENT = 25;

        final String KEY1 = "key1";
        final String KEY2 = "key2";
        final Integer VALUE1_0 = 5;
        final Integer VALUE2_0 = 5;

        final Integer VALUE1_1 = VALUE1_0 + INCREMENT;
        final Integer VALUE2_1 = VALUE2_0 + INCREMENT;

        kvStore.begin(INITIAL_WRITE_TRANSACTION);
        kvStore.write(KEY1, VALUE1_0, INITIAL_WRITE_TRANSACTION);
        kvStore.write(KEY2, VALUE2_0, INITIAL_WRITE_TRANSACTION);
        kvStore.commit(INITIAL_WRITE_TRANSACTION);

        //begin the read transaction but don't commit it!
        kvStore.begin(INITIAL_READ_TRANSACTION);
        kvStore.read(KEY1, INITIAL_READ_TRANSACTION);
        kvStore.read(KEY2, INITIAL_READ_TRANSACTION);

        kvStore.begin(INCREMENT_TRANSACTION);
        kvStore.write(KEY1, VALUE1_1, INCREMENT_TRANSACTION);
        kvStore.write(KEY2, VALUE2_1, INCREMENT_TRANSACTION);
        kvStore.commit(INCREMENT_TRANSACTION);

        List<TransactionalKVStore.TransactionalUnit<String, Integer>> readResults = kvStore.commit(INITIAL_READ_TRANSACTION);

        final Integer READ_1_OUTPUT = readResults.get(0).getValue();
        final Integer READ_2_OUTPUT = readResults.get(1).getValue();

        //Verify that the values are the same and that the transaction held
        Assert.assertEquals("Read different values", READ_1_OUTPUT, READ_2_OUTPUT);

        //Verify that we pull down the incremented values and not the original, signifying that the commit order was honored
        Assert.assertEquals("Read different values", READ_1_OUTPUT, VALUE2_1);
    }
}