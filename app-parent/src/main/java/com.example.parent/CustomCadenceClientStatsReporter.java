package com.example.parent;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AtomicDouble;
import com.uber.m3.tally.Buckets;
import com.uber.m3.tally.Capabilities;
import com.uber.m3.tally.CapableOf;
import com.uber.m3.tally.StatsReporter;
import com.uber.m3.util.Duration;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomCadenceClientStatsReporter implements StatsReporter {

  // com.uber.cadence.internal.metrics.MetricsTag
  public static final String ACTIVITY_TYPE = "ActivityType";
  public static final String DOMAIN = "Domain";
  public static final String TASK_LIST = "TaskList";
  public static final String WORKFLOW_TYPE = "WorkflowType";

  private final Predicate<String> metricsFilter;
  private final Map<String, AtomicDouble> gauges = new ConcurrentHashMap<>();

  public CustomCadenceClientStatsReporter() {
    this(always -> true);
  }

  public CustomCadenceClientStatsReporter(Predicate<String> metricsFilter) {
    Preconditions.checkNotNull(
        metricsFilter,
        "No predicate for metrics filtering specified. "
            + "Set property 'monitoring.cadence.stats-reporter.predicate=ALL'");
    this.metricsFilter = metricsFilter;
  }

  @Override
  public Capabilities capabilities() {
    return CapableOf.REPORTING;
  }

  @Override
  public void flush() {
    // NOOP
  }

  @Override
  public void close() {
    // NOOP
  }

  @Override
  public void reportCounter(String name, Map<String, String> tags, long value) {
    log.debug("CounterImpl {}: {} {}", name, tags, value);
    if (metricsFilter.test(name)) {
      Metrics.counter(name, getTags(tags)).increment(value);
    }
  }

  @Override
  public void reportGauge(String name, Map<String, String> tags, double value) {
    log.debug("GaugeImpl {}: {} {}", name, tags, value);
    if (metricsFilter.test(name)) {
      AtomicDouble gauge =
          gauges.computeIfAbsent(
              name,
              metricName -> {
                AtomicDouble result = Metrics.gauge(name, getTags(tags), new AtomicDouble());
                Preconditions.checkNotNull(result, "Metrics.gauge should not return null ever");
                return result;
              });
      gauge.set(value);
    }
  }

  @Override
  public void reportTimer(String name, Map<String, String> tags, Duration interval) {
    log.debug("TimerImpl {}: {} {}", name, tags, interval.getSeconds());
    if (metricsFilter.test(name)) {
      Metrics.timer(name, getTags(tags)).record(interval.getNanos(), TimeUnit.NANOSECONDS);
    }
  }

  @Override
  public void reportHistogramValueSamples(
      String name,
      Map<String, String> tags,
      Buckets buckets,
      double bucketLowerBound,
      double bucketUpperBound,
      long samples) {
    // NOOP
  }

  @Override
  public void reportHistogramDurationSamples(
      String name,
      Map<String, String> tags,
      Buckets buckets,
      Duration bucketLowerBound,
      Duration bucketUpperBound,
      long samples) {
    // NOOP
  }

  private Iterable<Tag> getTags(Map<String, String> tags) {
    return ImmutableList.of(
        Tag.of(ACTIVITY_TYPE, Strings.nullToEmpty(tags.get(ACTIVITY_TYPE))),
        Tag.of(DOMAIN, Strings.nullToEmpty(tags.get(DOMAIN))),
        Tag.of(TASK_LIST, Strings.nullToEmpty(tags.get(TASK_LIST))),
        Tag.of(WORKFLOW_TYPE, Strings.nullToEmpty(tags.get(WORKFLOW_TYPE))));
  }
}
