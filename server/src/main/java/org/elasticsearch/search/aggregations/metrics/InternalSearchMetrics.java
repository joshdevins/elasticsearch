/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.*;

public class InternalSearchMetrics extends InternalNumericMetricsAggregation.MultiValue implements SearchMetrics {

    enum SearchMetrics {
        numClicks, maxReciprocalRank, meanReciprocalRank, sumReciprocalRanks;

        public static SearchMetrics resolve(String name) {
            return SearchMetrics.valueOf(name);
        }
    }

    private static class Fields {
        static final String NUM_CLICKS = "numClicks";
        static final String MAX_RECIPROCAL_RANK = "maxReciprocalRank";
        static final String MEAN_RECIPROCAL_RANK = "meanReciprocalRank";
    }

    private final long numClicks;
    private final double maxReciprocalRank;
    private final double sumReciprocalRanks;

    InternalSearchMetrics(String name, long numClicks, double maxReciprocalRank, double sumReciprocalRanks,
                          DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators,
                          Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);

        this.numClicks = numClicks;
        this.maxReciprocalRank = maxReciprocalRank;
        this.sumReciprocalRanks = sumReciprocalRanks;
        this.format = formatter;
    }

    /**
     * Read from a stream.
     */
    public InternalSearchMetrics(StreamInput in) throws IOException {
        super(in);

        format = in.readNamedWriteable(DocValueFormat.class);
        numClicks = in.readVLong();
        maxReciprocalRank = in.readDouble();
        sumReciprocalRanks = in.readDouble();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeVLong(numClicks);
        out.writeDouble(maxReciprocalRank);
        out.writeDouble(sumReciprocalRanks);
    }

    @Override
    public String getWriteableName() {
        return SearchMetricsAggregationBuilder.NAME;
    }

    @Override
    public long getNumClicks() {
        return numClicks;
    }

    @Override
    public double getMaxReciprocalRank() {
        return maxReciprocalRank;
    }

    @Override
    public double getMeanReciprocalRank() {
        return sumReciprocalRanks / numClicks;
    }

    @Override
    public double getSumReciprocalRanks() {
        return sumReciprocalRanks;
    }

    private boolean hasClicks() {
        return numClicks > 0;
    }

    @Override
    public InternalSearchMetrics reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        long numClicks = 0;
        double maxReciprocalRank = 0;
        CompensatedSum sumReciprocalRanks = new CompensatedSum(0, 0);

        for (InternalAggregation aggregation : aggregations) {
            InternalSearchMetrics metrics = (InternalSearchMetrics) aggregation;
            numClicks += metrics.getNumClicks();

            // reciprocal rank metrics
            maxReciprocalRank = Math.max(maxReciprocalRank, metrics.getMaxReciprocalRank());
            sumReciprocalRanks.add(metrics.getSumReciprocalRanks());
        }

        return new InternalSearchMetrics(name, numClicks, maxReciprocalRank, sumReciprocalRanks.value(),
            format, pipelineAggregators(), getMetaData());
    }

    @Override
    public double value(String name) {
        SearchMetrics metrics = SearchMetrics.valueOf(name);
        switch (metrics) {
            case numClicks: return this.numClicks;
            case maxReciprocalRank: return this.maxReciprocalRank;
            case meanReciprocalRank: return this.getMeanReciprocalRank();
            case sumReciprocalRanks: return this.getSumReciprocalRanks();
            default:
                throw new IllegalArgumentException("Unknown value [" + name + "] in search metrics aggregation");
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.NUM_CLICKS, numClicks);

        if (hasClicks()) {
            builder.field(Fields.MAX_RECIPROCAL_RANK, maxReciprocalRank);
            builder.field(Fields.MEAN_RECIPROCAL_RANK, getMeanReciprocalRank());
        } else {
            builder.nullField(Fields.MAX_RECIPROCAL_RANK);
            builder.nullField(Fields.MEAN_RECIPROCAL_RANK);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), numClicks, maxReciprocalRank, sumReciprocalRanks);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;

        InternalSearchMetrics other = (InternalSearchMetrics) obj;
        return numClicks == other.numClicks &&
            Double.compare(maxReciprocalRank, other.maxReciprocalRank) == 0 &&
            Double.compare(sumReciprocalRanks, other.sumReciprocalRanks) == 0;
    }
}
