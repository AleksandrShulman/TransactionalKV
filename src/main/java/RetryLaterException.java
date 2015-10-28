/**
 * Created by aleks on 10/5/15.
 */
public class RetryLaterException extends Exception {

    final String LOCALIZED_MESSAGE;
    private int msToWait = 1000;

    public RetryLaterException(int numMatches) {
        this.msToWait = 100 + 50 * numMatches;
        this.LOCALIZED_MESSAGE = "Please wait " + msToWait + " milliseconds before retrying request";
    }

    public RetryLaterException(String msg) {

        this.LOCALIZED_MESSAGE = msg;
    }

    @Override
    public String getLocalizedMessage() {
        return LOCALIZED_MESSAGE;
    }

    public int getWaitTimeMs() {
        return this.msToWait;
    }
}
