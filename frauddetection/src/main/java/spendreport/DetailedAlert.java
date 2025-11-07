package spendreport;

import java.util.Objects;

/** A simple alert event. */
@SuppressWarnings("unused")
public final class DetailedAlert {

    private long id;

    private long timestamp;

    private double amount;

    private String zipCode;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public double getAmount() {
        return amount;
    }
    public void setAmount(double amount) {
        this.amount = amount;
    }
    public String getZipCode() {
        return zipCode;
    }
    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DetailedAlert alert = (DetailedAlert) o;
        return id == alert.id
                && timestamp == alert.timestamp
                && Double.compare(alert.amount, amount) == 0
                && Objects.equals(zipCode, alert.zipCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timestamp, amount, zipCode);
    }

    @Override
    public String toString() {
        return "Alert{" + "id=" + id +  ", timestamp=" + timestamp + ", amount=" + amount + ", zipCode=" + zipCode + '}';
    }
}
