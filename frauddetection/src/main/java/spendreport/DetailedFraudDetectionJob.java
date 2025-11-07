package spendreport;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.walkthrough.common.entity.Alert;

/**
 * Flink job to run the detailed fraud detection pipeline.
 *
 * This job connects:
 * 1. DetailedTransactionSource (randomly generates DetailedTransaction events)
 * 2. DetailedFraudDetector (stateful logic to find small then large tx from same zip)
 * 3. DetailedAlertSink (logs alerts to the console)
 */
public class DetailedFraudDetectionJob {

    public static void main(String[] args) throws Exception {
        // 1. Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Add the new DetailedTransactionSource
        DataStream<DetailedTransaction> transactions = env
                .addSource(new DetailedTransactionSource())
                .name("detailed-transactions");

        // 3. Process the stream with the new DetailedFraudDetector
        // The stream is keyed by account ID so that state is maintained per account.
        DataStream<Alert> alerts = transactions
                .keyBy(DetailedTransaction::getAccountId)
                .process(new DetailedFraudDetector())
                .name("fraud-detector");

        // 4. Send the resulting alerts to the new DetailedAlertSink
        alerts
                .addSink(new DetailedAlertSink())
                .name("send-alerts");

        // 5. Execute the job
        env.execute("Detailed Fraud Detection");
    }
}