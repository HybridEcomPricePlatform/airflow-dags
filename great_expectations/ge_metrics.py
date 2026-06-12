# ge_metrics.py
import statsd

def push_ge_result_to_statsd(result, suite_name):
    c = statsd.StatsClient(host="statsd_exporter", port=8125, prefix="great_expectations")
    success = 1 if result["success"] else 0
    c.gauge(f"{suite_name}.success", success)

    stats = result["run_results"]
    for run_result in stats.values():
        validation_result = run_result["validation_result"]
        c.gauge(f"{suite_name}.evaluated_expectations", validation_result["statistics"]["evaluated_expectations"])
        c.gauge(f"{suite_name}.successful_expectations", validation_result["statistics"]["successful_expectations"])