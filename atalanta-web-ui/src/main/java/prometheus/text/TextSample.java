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

package prometheus.text;

import prometheus.types.Metric;

/**
 * This represents a sample as found in the text data. This may or may not represent
 * a full metric. In the case of a counter or gauge, it will represent the full metric.
 * In the case of a summary or histogram, this only represents one quantile or one bucket
 * in a full summary or histogram metric. For those two cases, additional processing needs to
 * be made to combine multiple TextMetric objects into a single SummaryMetric or HistogramMetric.
 */
public class TextSample extends Metric {

    public static class Builder extends Metric.Builder<Builder> {
        private String value;
        private String line;

        public TextSample build() {
            return new TextSample(this);
        }

        public Builder setValue(String value) {
            this.value = value;
            return this;
        }

        public Builder setLine(String line) {
            this.line = line;
            return this;
        }
    }

    private final String value;
    private final String line;

    public TextSample(Builder builder) {
        super(builder);
        this.value = builder.value;
        this.line = builder.line;
    }

    public String getValue() {
        return value;
    }

    /**
     * This is the line of text in the text data where this sample came from.
     * This can be used for debugging purposes so you know what the sample
     * looked like before being parsed.
     *
     * @return the sample text line
     */
    public String getLine() {
        return line;
    }
}
