package spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

/**
 * A stateful fraud detector that checks for a small transaction (< $10)
 * followed by a large transaction (>= $500) from the *same zip code*
 * within a one-minute window.
 */
public class DetailedFraudDetector extends KeyedProcessFunction<Long, DetailedTransaction, Alert> {

    private static final long serialVersionUID = 1L;

    // --- Configuration Constants ---

    /**
     * The amount (exclusive) below which a transaction is considered "small".
     */
    private static final double SMALL_AMOUNT_THRESHOLD = 10.00;

    /**
     * The amount (inclusive) above which a transaction is considered "large".
     */
    private static final double LARGE_AMOUNT_THRESHOLD = 500.00;

    /**
     * The time window (in milliseconds) for the fraud pattern.
     */
    private static final long ONE_MINUTE = 60 * 1000;

    // --- State Variables ---

    /**
     * Stores the zip code of a recent small transaction.
     * If this state is null, no small transaction has occurred recently.
     * Type: transient ValueState<String>
     */
    private transient ValueState<String> smallTransactionZipState;

    /**
     * Stores the timestamp of the timer set to clear the state.
     * This helps prevent old timers from clearing new state.
     * Type: transient ValueState<Long>
     */
    private transient ValueState<Long> timerState;

    /**
     * Initializes the state descriptors when the function is opened.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // Descriptor for storing the zip code
        ValueStateDescriptor<String> zipDescriptor =
                new ValueStateDescriptor<>("small-tx-zip", String.class);
        smallTransactionZipState = getRuntimeContext().getState(zipDescriptor);

        // Descriptor for storing the timer's timestamp
        ValueStateDescriptor<Long> timerDescriptor =
                new ValueStateDescriptor<>("timer-ts", Long.class);
        timerState = getRuntimeContext().getState(timerDescriptor);
    }

    /**
     * Processes each incoming DetailedTransaction for the current key (accountId).
     */
    @Override
    public void processElement(
            DetailedTransaction transaction,
            Context context,
            Collector<Alert> collector) throws Exception {

        // Get the current state values
        String lastSmallTxZip = smallTransactionZipState.value();
        Long activeTimer = timerState.value();

        double amount = transaction.getAmount();
        String currentZip = transaction.getZipCode();

        if (amount < SMALL_AMOUNT_THRESHOLD) {
            // --- SMALL TRANSACTION ---
            // This is a new small transaction; it starts a new potential fraud window.

            // 1. If an old timer was set, delete it.
            if (activeTimer != null) {
                context.timerService().deleteEventTimeTimer(activeTimer);
            }

            // 2. Store the zip code of this new small transaction.
            smallTransactionZipState.update(currentZip);

            // 3. Set a new 1-minute timer based on the event's timestamp.
            long newTimerTs = context.timestamp() + ONE_MINUTE;
            context.timerService().registerEventTimeTimer(newTimerTs);
            timerState.update(newTimerTs);

        } else if (amount >= LARGE_AMOUNT_THRESHOLD) {
            // --- LARGE TRANSACTION ---

            if (lastSmallTxZip != null) {
                // A small transaction flag is active.

                // 4. Check if the zip code matches the stored one.
                if (lastSmallTxZip.equals(currentZip)) {
                    // FRAUD DETECTED! Both small and large tx had the same zip.
                    Alert alert = new Alert();
                    alert.setId(transaction.getAccountId());
                    collector.collect(alert);

                    // 5. Clean up: The fraud pattern is complete.
                    // Clear state and delete the timer.
                    if (activeTimer != null) {
                        context.timerService().deleteEventTimeTimer(activeTimer);
                    }
                    smallTransactionZipState.clear();
                    timerState.clear();
                }
                // If zip codes do NOT match, we do nothing.
                // The small transaction flag remains active, waiting for
                // either its timer to expire or a *matching* large tx.
            }
        }
        // Transactions with amounts between $10 and $500 are ignored
    }

    /**
     * Called when an event-time timer (set in processElement) fires.
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        // The timer fired, meaning 1 minute passed since the small transaction
        // without a matching large transaction.

        // Get the timestamp of the timer we are currently waiting for
        Long activeTimer = timerState.value();

        // Only clear the state if this timer call corresponds to the
        // timer we last registered. This prevents race conditions.
        if (activeTimer != null && activeTimer.equals(timestamp)) {
            smallTransactionZipState.clear();
            timerState.clear();
        }
    }
}