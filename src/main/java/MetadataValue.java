import java.util.Date;

public class MetadataValue<V> {

    private V value;
    private Date lastRead;
    private Date lastWritten;

    public MetadataValue(V v) {
        this.value = v;
    }

    public V getValue() {
        return value;
    }

    public Date getLastRead() {
        return lastRead;
    }

    public Date getLastWritten() {
        return lastWritten;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public void setLastRead(Date lastRead) {
        this.lastRead = lastRead;
    }

    public void setLastWritten(Date lastWritten) {
        this.lastWritten = lastWritten;
    }
}
