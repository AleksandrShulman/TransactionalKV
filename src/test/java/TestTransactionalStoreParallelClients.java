import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by aleks on 10/29/15.
 */
public class TestTransactionalStoreParallelClients {

    final int INITIAL_VALUE = 0;
    final int INCREMENT_AMOUNT = 10;
    final int INCREMENTS_PER_CLIENT = 50;
    final String KEY_1 = "key1";

    @Test
    /**
     *  Given:  Multiple clients making appends to a single key
     *  Verify: All increments are acknowledged
     *  Note:   - The current issue with the test is that as soon as a client gets rebuffed, it is not re-queued to start again
     *          - We may also not be giving it enough time for the last set of clients to complete
     *          - It may also be that multiple clients are all starting at the same initial time and have the same
     *            initial value, which may be causing problems.
     */
    public void testRepeatedParallelIncrementSingleKey() throws InterruptedException, ExecutionException {

        final int CONCURRENT_CLIENTS = 50;
        final int MAX_FAILED_ATTEMPTS = 100;
        final TransactionalKVStore<String, Integer> store = new TransactionalKVStore<String, Integer>();

        final int INITIAL_TRANSACTION = TestTransactionalStore.sharedTransactionCounter.addAndGet(1);
        store.begin(INITIAL_TRANSACTION);
        Integer initialResult = store.read(KEY_1, INITIAL_TRANSACTION);
        Assert.assertNull("Initial read for this value should be null", initialResult);
        store.write(KEY_1, INITIAL_VALUE, INITIAL_TRANSACTION);
        try {
            store.commit(INITIAL_TRANSACTION);
        } catch (RetryLaterException rte) {
            System.out.println("rte message: " + rte.getLocalizedMessage());
            throw new IllegalStateException("KV store required wait on initial transaction");
        }

        // The test starts here. We assume that we start with initial value. Then we launch a bunch of clients to increment it
        // in parallel.

        ExecutorService execService = Executors.newFixedThreadPool(CONCURRENT_CLIENTS);
        List<Future> futureList = new ArrayList<Future>();
        for (int i = 0; i < CONCURRENT_CLIENTS; i++) {

            Thread.sleep((int) (Math.random() * 5));
            futureList.add(execService.submit(new IncrementerClient(store, i, MAX_FAILED_ATTEMPTS)));
        }

        execService.shutdown();
        final int TIMEOUT_SECONDS = 90;
        Thread.sleep(3000);
        execService.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        for (Future f : futureList) {
            if (f.get() != null || !f.isDone() || f.isCancelled()) {
                System.out.println("Exited in disgrace!");
                throw new RuntimeException("Problem");
            }
        }

        final int FINAL_TRANSACTION_ID = TestTransactionalStore.sharedTransactionCounter.addAndGet(1);
        store.begin(FINAL_TRANSACTION_ID);
        final Integer result = store.read(KEY_1, FINAL_TRANSACTION_ID);
        final Integer EXPECTED_RESULT = INITIAL_VALUE + (INCREMENTS_PER_CLIENT * INCREMENT_AMOUNT * CONCURRENT_CLIENTS);
        Assert.assertEquals("The final output did not match. Check that appends are not overwriting one another", EXPECTED_RESULT, result);
    }

    /*
     *  This simple client will run through and perform a set of increments,
     *  each its own transaction, in series
     */
    private class IncrementerClient implements Runnable {

        final int clientId;
        final TransactionalKVStore<String, Integer> store;
        final int MAX_FAILED_ATTEMPTS;

        public IncrementerClient(TransactionalKVStore<String, Integer> store, final int clientId, final int MAX_FAILED_ATTEMPTS) {

            this.clientId = clientId;
            this.store = store;
            this.MAX_FAILED_ATTEMPTS = MAX_FAILED_ATTEMPTS;
        }

        public void run() {

            for (int i = 1; i < (INCREMENTS_PER_CLIENT + 1); i++) {

                System.out.println("Starting to run increment " + i + " from client " + clientId);
                try {

                    System.out.println("About to run increment " + i + " from client " + clientId);
                    TestTransactionalStore.increment(store, KEY_1, INCREMENT_AMOUNT, this.MAX_FAILED_ATTEMPTS, clientId);
                    System.out.println("Done running increment " + i + " from client " + clientId);

                } catch (InterruptedException e) {

                    System.out.println("ite msg 2: Caught interrupted exception");
                    throw new RuntimeException("Transaction was interrupted. This is bad.");
                }
            }

            // Each thread will sleep 10 seconds just to give the others time to catch up, if necessary
            System.out.println("Client id " + clientId + " done");
        }
    }
}
