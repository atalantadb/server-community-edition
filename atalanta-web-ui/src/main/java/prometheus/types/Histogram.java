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

public class Histogram extends Metric {

    public static class Builder extends Metric.Builder<Builder> {
        private long sampleCount = 0;
        private double sampleSum = Double.NaN;
        private List<Bucket> buckets;

        public Histogram build() {
            return new Histogram(this);
        }

        public Builder setSampleCount(long sampleCount) {
            this.sampleCount = sampleCount;
            return this;
        }

        public Builder setSampleSum(double sampleSum) {
            this.sampleSum = sampleSum;
            return this;
        }

        public Builder addBucket(double upperBound, long cumulativeCount) {
            if (buckets == null) {
                buckets = new ArrayList<>();
            }
            buckets.add(new Bucket(upperBound, cumulativeCount));
            return this;
        }

        public Builder addBuckets(List<Bucket> buckets) {
            if (this.buckets == null) {
                this.buckets = new ArrayList<>();
            }
            this.buckets.addAll(buckets);
            return this;
        }
    }

    public static class Bucket {
        private final double upperBound;
        private final long cumulativeCount;

        public Bucket(double upperBound, long cumulativeCount) {
            this.upperBound = upperBound;
            this.cumulativeCount = cumulativeCount;
        }

        public double getUpperBound() {
            return upperBound;
        }

        public long getCumulativeCount() {
            return cumulativeCount;
        }

        @Override
        public String toString() {
            return String.format("%s:%d", Util.convertDoubleToString(upperBound), cumulativeCount);
        }
    }

    private final long sampleCount;
    private final double sampleSum;
    private final List<Bucket> buckets;

    private Histogram(Builder builder) {
        super(builder);
        getLabels().remove("le");
        this.sampleCount = builder.sampleCount;
        this.sampleSum = builder.sampleSum;
        this.buckets = builder.buckets;
    }

    public long getSampleCount() {
        return sampleCount;
    }

    public double getSampleSum() {
        return sampleSum;
    }

    public List<Bucket> getBuckets() {
        if (buckets == null) {
            return Collections.emptyList();
        }
        return buckets;
    }
}
