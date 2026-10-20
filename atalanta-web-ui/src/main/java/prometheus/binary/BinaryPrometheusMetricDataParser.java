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

import io.prometheus.client.Metrics;
import io.prometheus.client.Metrics.MetricFamily;
import prometheus.PrometheusMetricDataParser;

import java.io.IOException;
import java.io.InputStream;

/**
 * Provides a method that can scrape Permetheus binary metric data from input streams.
 */
public class BinaryPrometheusMetricDataParser extends PrometheusMetricDataParser<MetricFamily> {

    /**
     * Provides the input stream where the parser will look for metric data.
     * NOTE: this object will not own this stream - it will never attempt to close it.
     *
     * @param inputStream the stream where the metric data can be found
     */
    public BinaryPrometheusMetricDataParser(InputStream inputStream) {
        super(inputStream);
    }

    public MetricFamily parse() throws IOException {
        return Metrics.MetricFamily.parseDelimitedFrom(getInputStream());
    }
}
