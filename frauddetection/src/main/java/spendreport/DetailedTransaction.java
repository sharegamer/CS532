package spendreport;

import java.util.Objects;

/** A simple transaction. */
@SuppressWarnings("unused")
public final class DetailedTransaction {

    private long accountId;

    private long timestamp;

    private double amount;

    private String zipCode;

    public DetailedTransaction() {}

    public DetailedTransaction(long accountId, long timestamp, double amount, String zipCode) {
        this.accountId = accountId;
        this.timestamp = timestamp;
        this.amount = amount;
        this.zipCode=zipCode;
    }

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
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
        DetailedTransaction that = (DetailedTransaction) o;
        return accountId == that.accountId
                && timestamp == that.timestamp
                && Double.compare(that.amount, amount) == 0
                && Objects.equals(zipCode, that.zipCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountId, timestamp, amount,zipCode);
    }

    @Override
    public String toString() {
        return "Transaction{"
                + "accountId="
                + accountId
                + ", timestamp="
                + timestamp
                + ", amount="
                + amount
                +", zipCode="
                + zipCode
                + '}';
    }
}
