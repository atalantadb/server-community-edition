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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Superclass to all metrics. All metrics have name and labels.
 */
public abstract class Metric {

    public abstract static class Builder<B extends Builder<?>> {
        private String name;
        private Map<String, String> labels;

        public B setName(String name) {
            this.name = name;
            return (B) this;
        }

        public B addLabel(String name, String value) {
            if (labels == null) {
                labels = new LinkedHashMap<>(); // used linked hash map to retain ordering
            }
            labels.put(name, value);
            return (B) this;
        }

        public B addLabels(Map<String, String> map) {
            if (labels == null) {
                labels = new LinkedHashMap<>(); // used linked hash map to retain ordering
            }
            labels.putAll(map);
            return (B) this;
        }

        public abstract <T extends Metric> T build();
    }

    private final String name;
    private final Map<String, String> labels;

    protected Metric(Builder<?> builder) {
        if (builder.name == null) {
            throw new IllegalArgumentException("Need to set name");
        }

        this.name = builder.name;
        this.labels = builder.labels;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getLabels() {
        if (labels == null) {
            return Collections.emptyMap();
        }
        return labels;
    }
}
