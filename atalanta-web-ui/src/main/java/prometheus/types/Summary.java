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

import prometheus.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Summary extends Metric {

    public static class Builder extends Metric.Builder<Builder> {
        private long sampleCount = 0;
        private double sampleSum = Double.NaN;
        private List<Quantile> quantiles;

        public Summary build() {
            return new Summary(this);
        }

        public Builder setSampleCount(long sampleCount) {
            this.sampleCount = sampleCount;
            return this;
        }

        public Builder setSampleSum(double sampleSum) {
            this.sampleSum = sampleSum;
            return this;
        }

        public Builder addQuantile(double quantile, double value) {
            if (quantiles == null) {
                quantiles = new ArrayList<>();
            }
            quantiles.add(new Quantile(quantile, value));
            return this;
        }

        public Builder addQuantiles(List<Quantile> quantiles) {
            if (this.quantiles == null) {
                this.quantiles = new ArrayList<>();
            }
            this.quantiles.addAll(quantiles);
            return this;
        }
    }

    public static class Quantile {
        private final double quantile;
        private final double value;

        public Quantile(double quantile, double value) {
            this.quantile = quantile;
            this.value = value;

        }

        public double getQuantile() {
            return quantile;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.format("%s:%s", Util.convertDoubleToString(quantile), Util.convertDoubleToString(value));
        }
    }

    private final long sampleCount;
    private final double sampleSum;
    private final List<Quantile> quantiles;

    private Summary(Builder builder) {
        super(builder);
        getLabels().remove("quantile");
        this.sampleCount = builder.sampleCount;
        this.sampleSum = builder.sampleSum;
        this.quantiles = builder.quantiles;
    }

    public long getSampleCount() {
        return sampleCount;
    }

    public double getSampleSum() {
        return sampleSum;
    }

    public List<Quantile> getQuantiles() {
        if (quantiles == null) {
            return Collections.emptyList();
        }
        return quantiles;
    }
}
