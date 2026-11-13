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

package prometheus.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Contains all metrics within a family (that is, of the same name). All metrics in a family have the same type.
 */
public class MetricFamily {

    public static class Builder {
        private String name;
        private String help;
        private MetricType type;
        private List<Metric> metrics;

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setHelp(String help) {
            this.help = help;
            return this;
        }

        public Builder setType(MetricType type) {
            this.type = type;
            return this;
        }

        public Builder addMetric(Metric metric) {
            if (metrics == null) {
                metrics = new ArrayList<>();
            }
            metrics.add(metric);
            return this;
        }

        public MetricFamily build() {
            return new MetricFamily(this);
        }
    }

    private final String name;
    private final String help;
    private final MetricType type;
    private final List<Metric> metrics;

    protected MetricFamily(Builder builder) {
        if (builder.name == null) {
            throw new IllegalArgumentException("Need to set name");
        }
        if (builder.type == null) {
            throw new IllegalArgumentException("Need to set type");
        }

        Class<? extends Metric> expectedMetricClassType;
        switch (builder.type) {
            case COUNTER:
                expectedMetricClassType = Counter.class;
                break;
            case GAUGE:
                expectedMetricClassType = Gauge.class;
                break;
            case SUMMARY:
                expectedMetricClassType = Summary.class;
                break;
            case HISTOGRAM:
                expectedMetricClassType = Histogram.class;
                break;
            default:
                throw new IllegalArgumentException("Invalid type: " + builder.type);
        }

        // make sure all the metrics in the family are of the expected type
        if (builder.metrics != null && !builder.metrics.isEmpty()) {
            for (Metric metric : builder.metrics) {
                if (!expectedMetricClassType.isInstance(metric)) {
                    throw new IllegalArgumentException(
                            String.format("Metric type is [%s] so instances of class [%s] are expected, "
                                    + "but got metric object of type [%s]",
                                    builder.type, expectedMetricClassType.getName(), metric.getClass().getName()));
                }
            }

        }

        this.name = builder.name;
        this.help = builder.help;
        this.type = builder.type;
        this.metrics = builder.metrics;
    }

    public String getName() {
        return name;
    }

    public String getHelp() {
        return help;
    }

    public MetricType getType() {
        return type;
    }

    public List<Metric> getMetrics() {
        if (metrics == null) {
            return Collections.emptyList();
        }
        return metrics;
    }
}
