/*
 * MIT License
 *
 * Copyright (c) 2022 John Mazzitelli
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/*
 * Adapted from jmazzitelli/prometheus-scraper (MIT).
 * Modified for AtalantaDB integration.
 * See THIRD-PARTY-SOURCES.md and NOTICE for provenance and attribution.
 */

package prometheus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prometheus.types.Counter;
import prometheus.types.Gauge;
import prometheus.types.MetricFamily;
import prometheus.walkers.PrometheusMetricsWalker;

import java.io.InputStream;

/**
 * A processor is responsible for iterating over a collection of metric families found in a specific
 * data format and invoking a walker during the iteration so the metric families can be processed.
 */
public abstract class PrometheusMetricsProcessor<T> {
    private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsProcessor.class);

    private final InputStream inputStream;
    private final PrometheusMetricsWalker walker;

    /**
     * @param inputStream where the Prometheus metrics are that the walker will traverse.
     * @param theWalker the actual object that will be notified about the metrics as they are encountered
     */
    public PrometheusMetricsProcessor(InputStream inputStream, PrometheusMetricsWalker theWalker) {
        if (inputStream == null) {
            throw new IllegalArgumentException("Stream must not be null");
        }
        this.inputStream = inputStream;

        if (theWalker == null) {
            throw new IllegalArgumentException("Walker must not be null");
        }
        this.walker = theWalker;
    }

    /**
     * This will iterate over a set of metrics that are produced by the
     * {@link #createPrometheusMetricDataParser() parser} and will notify the {@link #getWalker() walker}
     * of each metric found.
     */
    public void walk() {
        // tell the walker we are starting
        walker.walkStart();

        int totalMetrics = 0;
        int familyIndex = 0;

        try {
            PrometheusMetricDataParser<T> parser = createPrometheusMetricDataParser();
            T metricFamily = parser.parse(); // prime the pump

            while (metricFamily != null) {
                MetricFamily convertedMetricFamily = convert(metricFamily);

                // let the walker know we are traversing a new family of metrics
                walker.walkMetricFamily(convertedMetricFamily, familyIndex++);

                // walk through each metric in the family
                int metricIndex = 0;

                for (prometheus.types.Metric metric : convertedMetricFamily.getMetrics()) {
                    switch (convertedMetricFamily.getType()) {
                        case COUNTER:
                            walker.walkCounterMetric(convertedMetricFamily, (Counter) metric, metricIndex);
                            break;

                        case GAUGE:
                            walker.walkGaugeMetric(convertedMetricFamily, (Gauge) metric, metricIndex);
                            break;

                        case SUMMARY:
                            walker.walkSummaryMetric(convertedMetricFamily,
                                    ((prometheus.types.Summary) metric), metricIndex);
                            break;

                        case HISTOGRAM:
                            walker.walkHistogramMetric(convertedMetricFamily,
                                    ((prometheus.types.Histogram) metric), metricIndex);
                            break;
                    }

                    metricIndex++;
                }

                // finished processing the metrics for the current family
                totalMetrics += convertedMetricFamily.getMetrics().size();

                // go to the next metric family
                metricFamily = parser.parse();
            }
        } catch (Exception e) {
            log.debug("Error while processing binary data", e);
        }

        // tell the walker we have finished
        walker.walkFinish(familyIndex, totalMetrics);
    }

    /**
     * @return the input stream where the metric family data in a specific data format is found
     */
    protected InputStream getInputStream() {
        return inputStream;
    }

    /**
     * @return the object that will iterate over the found metric data
     */
    protected PrometheusMetricsWalker getWalker() {
        return walker;
    }

    /**
     * @return a new parser instance that can be used to parse the formatted data
     *         found in the {@link #getInputStream() input stream}.
     */
    protected abstract PrometheusMetricDataParser<T> createPrometheusMetricDataParser();

    /**
     * This method converts the metrics from the specific data format found in the input stream
     * to the common metric format.
     *
     * @param metricFamily the metric family (and its metrics) that need to be converted
     * @return the common MetricFamily object
     */
    protected abstract MetricFamily convert(T metricFamily);
}
