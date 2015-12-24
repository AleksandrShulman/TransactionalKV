/**
 * Created by aleks on 10/5/15.
 */
public class RetryLaterException extends Exception {

    final String LOCALIZED_MESSAGE;
    private int msToWait = 250;

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
        this.msToWait = this.msToWait + (int) (Math.random() * 100);
        System.out.println("Directing to wait " + this.msToWait + " milliseconds");
        return this.msToWait;
    }
}
