/**
 * Created by aleks on 10/5/15.
 */
public class RetryLaterException extends Exception {

    private int msToWait=1000;

    public RetryLaterException(int numMatches) {
        this.msToWait = 100 + 50*numMatches;
    }

    @Override
    public String getLocalizedMessage() {
        return "Please wait " + msToWait + " milliseconds before retrying request";
    }

    public int getWaitTimeMs() {
        return this.msToWait;
    }
}
