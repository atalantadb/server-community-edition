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

package prometheus.binary;

import io.prometheus.client.Metrics.*;
import prometheus.PrometheusMetricsProcessor;
import prometheus.types.MetricType;
import prometheus.walkers.PrometheusMetricsWalker;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * This will iterate over a list of Prometheus metrics that are given as binary protocol buffer data.
 */
public class BinaryPrometheusMetricsProcessor extends PrometheusMetricsProcessor<MetricFamily> {
    public BinaryPrometheusMetricsProcessor(InputStream inputStream, PrometheusMetricsWalker theWalker) {
        super(inputStream, theWalker);
    }

    @Override
    public BinaryPrometheusMetricDataParser createPrometheusMetricDataParser() {
        return new BinaryPrometheusMetricDataParser(getInputStream());
    }

    @Override
    protected prometheus.types.MetricFamily convert(MetricFamily family) {
        prometheus.types.MetricFamily.Builder convertedFamilyBuilder;
        MetricType convertedFamilyType = MetricType.valueOf(family.getType().name());

        convertedFamilyBuilder = new prometheus.types.MetricFamily.Builder();
        convertedFamilyBuilder.setName(family.getName());
        convertedFamilyBuilder.setHelp(family.getHelp());
        convertedFamilyBuilder.setType(convertedFamilyType);

        for (Metric metric : family.getMetricList()) {
            prometheus.types.Metric.Builder<?> convertedMetricBuilder = null;
            switch (convertedFamilyType) {
                case COUNTER:
                    convertedMetricBuilder = new prometheus.types.Counter.Builder()
                            .setValue(metric.getCounter().getValue());
                    break;
                case GAUGE:
                    convertedMetricBuilder = new prometheus.types.Gauge.Builder()
                            .setValue(metric.getGauge().getValue());
                    break;
                case SUMMARY:
                    Summary summary = metric.getSummary();
                    List<Quantile> pqList = summary.getQuantileList();
                    List<prometheus.types.Summary.Quantile> hqList;
                    hqList = new ArrayList<>(pqList.size());
                    for (Quantile pq : pqList) {
                        prometheus.types.Summary.Quantile hq;
                        hq = new prometheus.types.Summary.Quantile(
                                pq.getQuantile(), pq.getValue());
                        hqList.add(hq);
                    }
                    convertedMetricBuilder = new prometheus.types.Summary.Builder()
                            .setSampleCount(metric.getSummary().getSampleCount())
                            .setSampleSum(metric.getSummary().getSampleSum())
                            .addQuantiles(hqList);
                    break;
                case HISTOGRAM:
                    /* NO HISTOGRAM SUPPORT IN PROMETHEUS JAVA MODEL API 0.0.2. Uncomment when 0.0.3 is released
                    Histogram histo = metric.getHistogram();
                    List<Bucket> pbList = histo.getBucketList();
                    List<prometheus.types.Histogram.Bucket> hbList;
                    hbList = new ArrayList<>(pbList.size());
                    for (Bucket pb : pbList) {
                        prometheus.types.Histogram.Bucket hb;
                        hb = new prometheus.types.Histogram.Bucket(pb.getUpperBound(),
                                pb.getCumulativeCount());
                        hbList.add(hb);
                    }
                    convertedMetricBuilder = new prometheus.types.Histogram.Builder()
                            .setSampleCount(metric.getHistogram().getSampleCount())
                            .setSampleSum(metric.getHistogram().getSampleSum())
                            .addBuckets(hbList);
                    */
                    break;
            }
            convertedMetricBuilder.setName(family.getName());
            for (LabelPair labelPair : metric.getLabelList()) {
                convertedMetricBuilder.addLabel(labelPair.getName(), labelPair.getValue());
            }
            convertedFamilyBuilder.addMetric(convertedMetricBuilder.build());
        }

        return convertedFamilyBuilder.build();
    }
}
