package spendreport;



import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.walkthrough.common.entity.Alert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A sink for outputting alerts. */
@PublicEvolving
@SuppressWarnings("unused")
public class DetailedAlertSink implements SinkFunction<Alert> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DetailedAlertSink.class);

    @Override
    public void invoke(Alert value, Context context) {
        LOG.info(value.toString());
    }
}
