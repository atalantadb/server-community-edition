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

package prometheus.walkers;

import prometheus.types.*;

import java.net.URL;

public class SimplePrometheusMetricsWalker implements PrometheusMetricsWalker {

    private final URL url;

    public SimplePrometheusMetricsWalker() {
        this(null);
    }

    /**
     * Use this constructor if you know the URL where the metric data came from.
     *
     * @param url the protocol endpoint that supplied the Prometheus metric data
     */
    public SimplePrometheusMetricsWalker(URL url) {
        this.url = url;
    }

    @Override
    public void walkStart() {
        if (url != null) {
            System.out.println("Scraping metrics from Prometheus protocol endpoint: " + url);
        }
    }

    @Override
    public void walkFinish(int familiesProcessed, int metricsProcessed) {
        if (metricsProcessed == 0) {
            System.out.println("There are no metrics");
        }
    }

    @Override
    public void walkMetricFamily(MetricFamily family, int index) {
        System.out.printf("* %s (%s): %s\n", family.getName(), family.getType(), family.getHelp());
    }

    @Override
    public void walkCounterMetric(MetricFamily family, Counter metric, int index) {
        System.out.printf("  +%2d. %s%s [%f]\n",
                index,
                metric.getName(),
                buildLabelListString(metric.getLabels(), "{", "}"),
                metric.getValue());
    }

    @Override
    public void walkGaugeMetric(MetricFamily family, Gauge metric, int index) {
        System.out.printf("  +%2d. %s%s [%f]\n",
                index,
                metric.getName(),
                buildLabelListString(metric.getLabels(), "{", "}"),
                metric.getValue());
    }

    @Override
    public void walkSummaryMetric(MetricFamily family, Summary metric, int index) {
        System.out.printf("  +%2d. %s%s [%d/%f] {%s}\n",
                index,
                metric.getName(),
                buildLabelListString(metric.getLabels(), "{", "}"),
                metric.getSampleCount(),
                metric.getSampleSum(),
                metric.getQuantiles());
    }

    @Override
    public void walkHistogramMetric(MetricFamily family, Histogram metric, int index) {
        System.out.printf("  +%2d. %s%s [%d/%f] {%s}\n",
                index,
                metric.getName(),
                buildLabelListString(metric.getLabels(), "{", "}"),
                metric.getSampleCount(),
                metric.getSampleSum(),
                metric.getBuckets());
    }
}
