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

import java.io.IOException;
import java.io.InputStream;

/**
 * An object that can parse Prometheus found in a specific data format in an input stream.
 * The type <T> is the metric family object for the specific data format.
 * It is the job of the associated {@link PrometheusMetricsProcessor} to process
 * the parsed data.
 *
 * @param <T> the metric family object type that the parser produces
 */
public abstract class PrometheusMetricDataParser<T> {
    private InputStream inputStream;

    /**
     * Provides the input stream where the parser will look for metric data.
     * NOTE: this object will not own this stream - it should never attempt to close it.
     *
     * @param inputStream the stream where the metric data can be found
     */
    public PrometheusMetricDataParser(InputStream inputStream) {
        if (inputStream == null) {
            throw new IllegalArgumentException("Stream must not be null");
        }
        this.inputStream = inputStream;
    }

    protected InputStream getInputStream() {
        return this.inputStream;
    }

    /**
     * Reads a single metric family from the Prometheus metric data stream and returns it.
     * Returns null when no more data is in the stream.
     *
     * This method is designed to be called several times, each time it returns the next metric family
     * found in the input stream.
     *
     * @return the metric family data found in the stream, or null
     * @throws IOException if failed to read the data from the stream
     */
    public abstract T parse() throws IOException;
}
