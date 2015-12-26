/**
 * Created by aleks on 12/24/15.
 */
public class NoSuchTransactionException extends IllegalStateException {

    int transactionId;

    public NoSuchTransactionException(int transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public String getLocalizedMessage() {

        return "Could not find transactionId " + transactionId;

    }
}
