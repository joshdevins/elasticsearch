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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class SearchMetricsAggregator extends NumericMetricsAggregator.MultiValue {

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat format;

    private LongArray numClicksArray;
    private DoubleArray maxReciprocalRankArray;
    private DoubleArray sumReciprocalRanksArray;
    private DoubleArray sumReciprocalRanksCompensations;

    SearchMetricsAggregator(String name, ValuesSource.Numeric valuesSource, DocValueFormat format,
                            SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                            Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);

        this.valuesSource = valuesSource;
        if (valuesSource != null) {
            final BigArrays bigArrays = context.bigArrays();
            numClicksArray = bigArrays.newLongArray(1, true);
            maxReciprocalRankArray = bigArrays.newDoubleArray(1, false);
            maxReciprocalRankArray.fill(0, maxReciprocalRankArray.size(), Double.NEGATIVE_INFINITY);
            sumReciprocalRanksArray = bigArrays.newDoubleArray(1, true);
            sumReciprocalRanksCompensations = bigArrays.newDoubleArray(1, true);
        }
        this.format = format;
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub)
            throws IOException {

        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        final CompensatedSum sumReciprocalRanksKahanSummation = new CompensatedSum(0, 0);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bucket >= numClicksArray.size()) {
                    final long from = numClicksArray.size();
                    final long overSize = BigArrays.overSize(bucket + 1);
                    numClicksArray = bigArrays.resize(numClicksArray, overSize);
                    maxReciprocalRankArray = bigArrays.resize(maxReciprocalRankArray, overSize);
                    maxReciprocalRankArray.fill(from, overSize, Double.NEGATIVE_INFINITY);
                    sumReciprocalRanksArray = bigArrays.resize(sumReciprocalRanksArray, overSize);
                    sumReciprocalRanksCompensations = bigArrays.resize(sumReciprocalRanksCompensations, overSize);
                }

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    numClicksArray.increment(bucket, valuesCount);
                    double maxReciprocalRank = maxReciprocalRankArray.get(bucket);

                    double sumReciprocalRanks = sumReciprocalRanksArray.get(bucket);
                    double sumReciprocalRanksCompensation = sumReciprocalRanksCompensations.get(bucket);
                    sumReciprocalRanksKahanSummation.reset(sumReciprocalRanks, sumReciprocalRanksCompensation);

                    for (int i = 0; i < valuesCount; i++) {
                        double value = values.nextValue();
                        maxReciprocalRank = Math.max(maxReciprocalRank, value);
                        sumReciprocalRanksKahanSummation.add(value);
                    }

                    maxReciprocalRankArray.set(bucket, maxReciprocalRank);
                    sumReciprocalRanksArray.set(bucket, sumReciprocalRanksKahanSummation.value());
                    sumReciprocalRanksCompensations.set(bucket, sumReciprocalRanksKahanSummation.delta());
                }
            }
        };
    }

    @Override
    public boolean hasMetric(String name) {
        try {
            InternalSearchMetrics.SearchMetrics.resolve(name);
            return true;
        } catch (IllegalArgumentException iae) {
            return false;
        }
    }

    @Override
    public double metric(String name, long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= numClicksArray.size()) {
            switch(InternalSearchMetrics.SearchMetrics.resolve(name)) {
                case numClicks: return 0;
                case maxReciprocalRank: return Double.NEGATIVE_INFINITY;
                case meanReciprocalRank: return Double.NaN;
                case sumReciprocalRanks: return 0.0;
                default:
                    throw new IllegalArgumentException("Unknown value [" + name + "] in search metrics aggregation");
            }
        }
        switch(InternalSearchMetrics.SearchMetrics.resolve(name)) {
            case numClicks: return numClicksArray.get(owningBucketOrd);
            case maxReciprocalRank: return maxReciprocalRankArray.get(owningBucketOrd);
            case meanReciprocalRank:
                return sumReciprocalRanksArray.get(owningBucketOrd) / numClicksArray.get(owningBucketOrd);
            case sumReciprocalRanks: return sumReciprocalRanksArray.get(owningBucketOrd);
            default:
                throw new IllegalArgumentException("Unknown value [" + name + "] in search metrics aggregation");
        }
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= sumReciprocalRanksArray.size()) {
            return buildEmptyAggregation();
        }
        return new InternalSearchMetrics(name, numClicksArray.get(bucket), maxReciprocalRankArray.get(bucket),
            sumReciprocalRanksArray.get(bucket), format, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalSearchMetrics(name, 0L, Double.NEGATIVE_INFINITY, 0.0d, format, pipelineAggregators(),
            metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(numClicksArray, maxReciprocalRankArray, sumReciprocalRanksArray,
            sumReciprocalRanksCompensations);
    }
}
