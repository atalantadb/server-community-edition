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

import java.util.Map;

/**
 * Implementors iterate a collection of metric families and their metrics.
 */
public interface PrometheusMetricsWalker {

    /**
     * Called when a walk has been started.
     */
    void walkStart();

    /**
     * Called when a walk has traversed all the metrics.
     * @param familiesProcessed total number of families processed
     * @param metricsProcessed total number of metrics across all families processed
     */
    void walkFinish(int familiesProcessed, int metricsProcessed);

    /**
     * Called when a new metric family is about to be traversed.
     *
     * @param family information about the family being traversed such as the name, help description, etc.
     * @param index index of the family being processed, where 0 is the first one.
     */
    void walkMetricFamily(MetricFamily family, int index);

    /**
     * Called when a new counter metric is found.
     *
     * @param family information about the family being traversed such as the name, help description, etc.
     * @param counter the metric being processed
     * @param index index of the metric being processed, where 0 is the first one.
     */
    void walkCounterMetric(MetricFamily family, Counter counter, int index);

    /**
     * Called when a new gauge metric is found.
     *
     * @param family information about the family being traversed such as the name, help description, etc.
     * @param gauge the metric being processed
     * @param index index of the metric being processed, where 0 is the first one.
     */
    void walkGaugeMetric(MetricFamily family, Gauge gauge, int index);

    /**
     * Called when a new summary metric is found.
     *
     * @param family information about the family being traversed such as the name, help description, etc.
     * @param summary the metric being processed
     * @param index index of the metric being processed, where 0 is the first one.
     */
    void walkSummaryMetric(MetricFamily family, Summary summary, int index);

    /**
     * Called when a new histogram metric is found.
     *
     * @param family information about the family being traversed such as the name, help description, etc.
     * @param histogram the metric being processed
     * @param index index of the metric being processed, where 0 is the first one.
     */
    void walkHistogramMetric(MetricFamily family, Histogram histogram, int index);

    /**
     * Convienence method that takes the given label list and returns a string in the form of
     * "labelName1=labelValue1,labelName2=labelValue2,..."
     *
     * @param labels the label list
     * @param prefix if not null, these characters will prefix the label list
     * @param suffix if not null, these characters will suffix the label list
     * @return the string form of the labels, optionally prefixed and suffixed
     */
    default String buildLabelListString(Map<String, String> labels, String prefix, String suffix) {
        if (prefix == null) {
            prefix = "";
        }
        if (suffix == null) {
            suffix = "";
        }
        if (labels == null) {
            return String.format("%s%s", prefix, suffix);
        }

        StringBuilder str = new StringBuilder("");
        for (Map.Entry<String, String> pair : labels.entrySet()) {
            if (str.length() > 0) {
                str.append(",");
            }
            str.append(pair.getKey()).append("=").append(pair.getValue());
        }
        return String.format("%s%s%s", prefix, str, suffix);
    }

}
