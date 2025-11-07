package spendreport;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.configuration.Configuration;

import java.util.Random;

/**
 * A streaming source function that randomly generates DetailedTransaction events.
 *
 * This source generates transactions with the following properties:
 * - accountId: Uniformly random from {1, 2, 3, 4, 5}
 * - timestamp: Increments by 1 second for each event
 * - zipCode: Uniformly random from {"01003", "02115", "78712"}
 * - amount: Uniformly random from ($0, $1000]
 */
public class DetailedTransactionSource extends RichParallelSourceFunction<DetailedTransaction> {

    private static final long serialVersionUID = 1L;

    /** Flag to control the main loop */
    private volatile boolean isRunning = true;

    /** The current event timestamp, starting from the current system time */
    private transient long currentTimestamp;

    /** Random number generator */
    private transient Random random;

    /** The set of zip codes to pick from */
    private static final String[] ZIP_CODES = {"01003", "02115", "78712"};

    /** The number of account IDs to choose from (1 to 5) */
    private static final int ACCOUNT_COUNT = 5;

    /** The maximum amount for a transaction */
    private static final double MAX_AMOUNT = 1000.0;

    /** The time to sleep between generating events (in milliseconds) */
    private static final long SLEEP_TIME_MS = 1000;

    /**
     * Initializes the source's state. Called once before run().
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.random = new Random();
        this.currentTimestamp = System.currentTimeMillis();
    }

    /**
     * The main loop of the source function.
     */
    @Override
    public void run(SourceContext<DetailedTransaction> ctx) throws Exception {
        while (isRunning) {
            // 1. Generate account id: {1, 2, 3, 4, 5}
            long accountId = (long) (random.nextInt(ACCOUNT_COUNT) + 1);

            // 2. Increment timestamp by 1 second
            currentTimestamp += 1000;
            long timestamp = currentTimestamp;

            // 3. Generate zip code: {"01003", "02115", "78712"}
            String zipCode = ZIP_CODES[random.nextInt(ZIP_CODES.length)];

            // 4. Generate amount: ($0, $1000]
            // random.nextDouble() returns [0.0, 1.0)
            // 1.0 - random.nextDouble() returns (0.0, 1.0]
            // This ensures the amount is never zero.
            double amount = (1.0 - random.nextDouble()) * MAX_AMOUNT;

            // Create the transaction
            DetailedTransaction transaction = new DetailedTransaction(
                    accountId,
                    timestamp,
                    amount,
                    zipCode
            );

            // Emit the transaction with its event timestamp.
            // We use the checkpoint lock for thread-safety.
            synchronized (ctx.getCheckpointLock()) {
                ctx.collectWithTimestamp(transaction, transaction.getTimestamp());

                // Emit a watermark to advance event time.
                // This is a simple watermark strategy that assumes events
                // are generated roughly in order, lagging by 1ms.
                ctx.emitWatermark(new Watermark(transaction.getTimestamp() - 1));
            }

            // Pause slightly to pace the generator.
            // This helps avoid overwhelming the CPU and makes the
            // stream easier to observe.
            Thread.sleep(SLEEP_TIME_MS);
        }
    }

    /**
     * Signals the source to stop running.
     */
    @Override
    public void cancel() {
        isRunning = false;
    }
}