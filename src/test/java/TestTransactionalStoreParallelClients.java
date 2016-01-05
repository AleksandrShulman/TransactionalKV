import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by aleks on 10/29/15.
 */
public class TestTransactionalStoreParallelClients {

    final static int INCREMENT_AMOUNT = 10;
    final static int INCREMENTS_PER_CLIENT = 50;
    final static String KEY_1 = "key1";
    final int INITIAL_VALUE = 0;

    @Test
    public void testRepeatedParallelIncrementSingleKeyServerSide()
            throws InterruptedException, ExecutionException {
        runRepeatedParallelIncrementSingleKey(true);
    }

    @Test
    /**
     *  Given:  Multiple clients making appends to a single key
     *  Verify: All increments are acknowledged
     */
    public void testRepeatedParallelIncrementSingleKey()
            throws InterruptedException, ExecutionException {
        runRepeatedParallelIncrementSingleKey(false);
    }

    /**
     * Given:  A KV store and the Fibonacci Transaction, and a set of parallel clients
     * Verify: The KV store maintains a proper Fibonacci sequence
     */
    @Test
    public void testFibonacciSequenceFromParallelClients() throws InterruptedException, ExecutionException {

        final int CONCURRENT_CLIENTS = 6;
        final int FIBONACCI_TRANSACTIONS_PER_CLIENT = 15;
        final int MAX_FAILED_ATTEMPTS = 100;
        final TransactionalKVStore<String, Long> store =
                new TransactionalKVStore<String, Long>();

        ExecutorService execService = Executors.newFixedThreadPool(CONCURRENT_CLIENTS);
        List<Future> futureList = new ArrayList<Future>();
        for (int i = 0; i < CONCURRENT_CLIENTS; i++) {

            // Stagger client starts
            Thread.sleep((int) (Math.random() * 50));

            futureList.add(execService.submit(new FibonacciClient(store, i,
                    FIBONACCI_TRANSACTIONS_PER_CLIENT, MAX_FAILED_ATTEMPTS)));
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

        // Validation of results
        final int FINAL_TRANSACTION_ID =
                TestTransactionalStore.sharedTransactionCounter.addAndGet(1);
        store.begin(FINAL_TRANSACTION_ID);
        final String SIZE_KEY = "size";

        final Long size = store.read(SIZE_KEY, FINAL_TRANSACTION_ID);
        Integer intSize = Integer.valueOf(String.valueOf(size));

        if (size == null) {
            Assert.fail("size should never be null");
        }

        // The index of the last item will be (size - 1)
        final Long finalValue = store.read(String.valueOf(size - 1), FINAL_TRANSACTION_ID);
        final Long previousValue = store.read(String.valueOf(size - 2), FINAL_TRANSACTION_ID);
        final Long prePreviousValue = store.read(String.valueOf(size - 3),
                FINAL_TRANSACTION_ID);

        Assert.assertEquals("The final state did not have the values add up properly. Expected " +
                prePreviousValue + " + " + previousValue + " = " + finalValue, (Long)
                (previousValue +
                        prePreviousValue), finalValue);

        final Integer NUMBER_SUCCESSFUL_TRANSACTIONS =
                (FIBONACCI_TRANSACTIONS_PER_CLIENT) *
                        CONCURRENT_CLIENTS;
        final Integer EXPECTED_SIZE = 1 + NUMBER_SUCCESSFUL_TRANSACTIONS;


        System.out.println("The final state is " +
                prePreviousValue + " + " + previousValue + " = " + finalValue + " with " + intSize +
                " items present.");

        Assert.assertEquals("The final size. " +
                        "Check that Fibonacci transactions are not overwriting one another",
                EXPECTED_SIZE, intSize);
    }

    private void runRepeatedParallelIncrementSingleKey(boolean serverSide)
            throws InterruptedException, ExecutionException {
        final int CONCURRENT_CLIENTS = 50;
        final int MAX_FAILED_ATTEMPTS = 100;
        final TransactionalKVStore<String, Integer> store =
                new TransactionalKVStore<String, Integer>();

        final int INITIAL_TRANSACTION =
                TestTransactionalStore.sharedTransactionCounter.addAndGet(1);

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

        // The test starts here. We assume that we start with initial value.
        // Then we launch a bunch of clients to client_side_increment it
        // in parallel.

        ExecutorService execService = Executors.newFixedThreadPool(CONCURRENT_CLIENTS);
        List<Future> futureList = new ArrayList<Future>();
        for (int i = 0; i < CONCURRENT_CLIENTS; i++) {

            // Stagger client starts
            Thread.sleep((int) (Math.random() * 50));

            futureList.add(execService.submit(new IncrementerClient(store, serverSide, i,
                    KEY_1, INCREMENTS_PER_CLIENT, INCREMENT_AMOUNT, MAX_FAILED_ATTEMPTS)));
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

        final int FINAL_TRANSACTION_ID =
                TestTransactionalStore.sharedTransactionCounter.addAndGet(1);
        store.begin(FINAL_TRANSACTION_ID);
        final Integer result = store.read(KEY_1, FINAL_TRANSACTION_ID);
        final Integer EXPECTED_RESULT = INITIAL_VALUE +
                (INCREMENTS_PER_CLIENT * INCREMENT_AMOUNT * CONCURRENT_CLIENTS);
        Assert.assertEquals("The final output did not match. " +
                "Check that appends are not overwriting one another", EXPECTED_RESULT, result);
    }

    /*
     *  This simple client will run through and perform a set of increments,
     *  each its own transaction, in series
     */
    protected static class IncrementerClient implements Runnable {

        final int CLIENT_ID;
        final int MAX_FAILED_ATTEMPTS;
        final int INCREMENTS_PER_CLIENT;
        final int INCREMENT_AMOUNT;
        final String KEY;
        TransactionalKVStore<String, Integer> store;
        boolean serverSide;

        public IncrementerClient(TransactionalKVStore<String, Integer> store, boolean
                serverSide, final
                                 int clientId,
                                 final String KEY, final int INCREMENTS_PER_CLIENT,
                                 final int INCREMENT_AMOUNT, final int MAX_FAILED_ATTEMPTS) {

            this.CLIENT_ID = clientId;
            this.store = store;
            this.MAX_FAILED_ATTEMPTS = MAX_FAILED_ATTEMPTS;
            this.INCREMENTS_PER_CLIENT = INCREMENTS_PER_CLIENT;
            this.INCREMENT_AMOUNT = INCREMENT_AMOUNT;
            this.KEY = KEY;
            this.serverSide = serverSide;
        }

        public void run() {

            for (int i = 1; i < (INCREMENTS_PER_CLIENT + 1); i++) {
                try {

                    System.out.println(
                            "Starting increment transaction " + i + " from client " + CLIENT_ID);
                    if (serverSide) {

                        TestTransactionalStore.server_side_increment(
                                store, KEY, INCREMENT_AMOUNT, this.MAX_FAILED_ATTEMPTS);

                    } else {
                        TestTransactionalStore.client_side_increment(
                                store, KEY, INCREMENT_AMOUNT, this.MAX_FAILED_ATTEMPTS);
                    }

                    System.out.println(
                            "Done with increment " + i + " from client " + CLIENT_ID);

                } catch (InterruptedException e) {

                    System.out.println("ite msg 2: Caught interrupted exception");
                    throw new RuntimeException("Transaction was interrupted. This is bad.");
                }
            }

            // Each thread will sleep 10 seconds just to give the others time to catch up, if necessary
            System.out.println("Client id " + CLIENT_ID + " done");
        }
    }


    protected static class FibonacciClient implements Runnable {

        final int CLIENT_ID;
        final int MAX_FAILED_ATTEMPTS;
        final int TRANSACTIONS_PER_CLIENT;
        TransactionalKVStore<String, Long> store;

        public FibonacciClient(TransactionalKVStore<String, Long> store, final
        int clientId, final int TRANSACTIONS_PER_CLIENT,
                               final int MAX_FAILED_ATTEMPTS) {
            this.CLIENT_ID = clientId;
            this.MAX_FAILED_ATTEMPTS = MAX_FAILED_ATTEMPTS;
            this.TRANSACTIONS_PER_CLIENT = TRANSACTIONS_PER_CLIENT;
            this.store = store;
        }

        public void run() {

            for (int i = 1; i < (TRANSACTIONS_PER_CLIENT + 1); i++) {
                try {

                    System.out.println(
                            "Starting Fibonacci transaction " + i + " from client " + CLIENT_ID);

                    TransactionalKVStore.submitReplayableTransaction(TestTransactionalStore
                            .FIBONACCI_ACTION, new Object[]{}, store, MAX_FAILED_ATTEMPTS);

                    System.out.println(
                            "Done with Fibonacci transaction " + i + " from client " + CLIENT_ID);

                } catch (InterruptedException e) {

                    System.out.println("ite msg 2: Caught interrupted exception");
                    throw new RuntimeException("Transaction was interrupted. This is bad.");
                }
            }

            // Each thread will sleep 10 seconds just to give the others time to catch up, if necessary
            System.out.println("Client id " + CLIENT_ID + " done");
        }
    }
}
