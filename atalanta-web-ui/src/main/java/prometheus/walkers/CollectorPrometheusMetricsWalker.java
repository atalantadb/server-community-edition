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

import java.util.ArrayList;
import java.util.List;

/**
 * This simply collects all metrics in all families and provides a list to the families.
 */
public class CollectorPrometheusMetricsWalker implements PrometheusMetricsWalker {

    private List<MetricFamily> finishedList;
    private boolean finished;

    /**
     * @return indicates if this walker has finished processing all metric families.
     */
    public boolean isFinished() {
        return finished;
    }

    /**
     * @return if this walker has finished processing all metric families, this will return the list of the
     *         metric families processed. If the walker hasn't finished yet, null is returned.
     */
    public List<MetricFamily> getAllMetricFamilies() {
        return (finished) ? finishedList : null;
    }

    @Override
    public void walkStart() {
        finished = false;
        finishedList = new ArrayList<>();
    }

    @Override
    public void walkFinish(int familiesProcessed, int metricsProcessed) {
        finished = true;
    }

    @Override
    public void walkMetricFamily(MetricFamily family, int index) {
        finishedList.add(family);
    }

    @Override
    public void walkCounterMetric(MetricFamily family, Counter metric, int index) {
    }

    @Override
    public void walkGaugeMetric(MetricFamily family, Gauge metric, int index) {
    }

    @Override
    public void walkSummaryMetric(MetricFamily family, Summary metric, int index) {
    }

    @Override
    public void walkHistogramMetric(MetricFamily family, Histogram metric, int index) {
    }
}
