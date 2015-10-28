import java.util.Date;

/**
 * Created by aleks on 10/22/15.
 */
public class Transaction {

    private final int id;
    private final Date startTime;
    private Date endTime;

    public Transaction(int transactionId) {

        startTime = new Date();
        this.id = transactionId;
    }

    public int getId() {
        return id;
    }

    public Date getStartTime() {
        return startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }


    // TODO: Add public static method to get unique transaction id
}
