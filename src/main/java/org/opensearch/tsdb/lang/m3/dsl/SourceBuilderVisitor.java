/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.dsl;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.LabelConstants;
import org.opensearch.tsdb.lang.m3.common.AggregationType;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AbsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AsPercentPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.DiffPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.DivideScalarPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.DividePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ExcludeByTagPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TagSubPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FallbackSeriesBinaryPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.IntersectPlanNode;
import org.opensearch.tsdb.lang.m3.stage.AbsStage;
import org.opensearch.tsdb.lang.m3.stage.AliasByTagsStage;
import org.opensearch.tsdb.lang.m3.stage.AliasStage;
import org.opensearch.tsdb.lang.m3.stage.AsPercentStage;
import org.opensearch.tsdb.lang.m3.stage.CopyStage;
import org.opensearch.tsdb.lang.m3.stage.ExcludeByTagStage;
import org.opensearch.tsdb.lang.m3.stage.TagSubStage;
import org.opensearch.tsdb.lang.m3.stage.FallbackSeriesBinaryStage;
import org.opensearch.tsdb.lang.m3.stage.FallbackSeriesUnaryStage;
import org.opensearch.tsdb.lang.m3.stage.AvgStage;
import org.opensearch.tsdb.lang.m3.stage.CountStage;
import org.opensearch.tsdb.lang.m3.stage.DivideScalarStage;
import org.opensearch.tsdb.lang.m3.stage.DivideStage;
import org.opensearch.tsdb.lang.m3.stage.HistogramPercentileStage;
import org.opensearch.tsdb.lang.m3.stage.IntersectStage;
import org.opensearch.tsdb.lang.m3.stage.KeepLastValueStage;
import org.opensearch.tsdb.lang.m3.stage.LogarithmStage;
import org.opensearch.tsdb.lang.m3.stage.MaxStage;
import org.opensearch.tsdb.lang.m3.stage.MinStage;
import org.opensearch.tsdb.lang.m3.stage.OffsetStage;
import org.opensearch.tsdb.lang.m3.stage.MovingStage;
import org.opensearch.tsdb.lang.m3.stage.PerSecondRateStage;
import org.opensearch.tsdb.lang.m3.stage.PerSecondStage;
import org.opensearch.tsdb.lang.m3.stage.PercentileOfSeriesStage;
import org.opensearch.tsdb.lang.m3.stage.IsNonNullStage;
import org.opensearch.tsdb.lang.m3.stage.RemoveEmptyStage;
import org.opensearch.tsdb.lang.m3.stage.DerivativeStage;
import org.opensearch.tsdb.lang.m3.stage.IntegralStage;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.lang.m3.stage.ScaleToSecondsStage;
import org.opensearch.tsdb.lang.m3.stage.HeadStage;
import org.opensearch.tsdb.lang.m3.stage.ShowTagsStage;
import org.opensearch.tsdb.lang.m3.stage.SortStage;
import org.opensearch.tsdb.lang.m3.stage.SqrtStage;
import org.opensearch.tsdb.lang.m3.stage.SustainStage;
import org.opensearch.tsdb.lang.m3.stage.SubtractStage;
import org.opensearch.tsdb.lang.m3.stage.SummarizeStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.lang.m3.stage.MultiplyStage;
import org.opensearch.tsdb.lang.m3.stage.TimeshiftStage;
import org.opensearch.tsdb.lang.m3.stage.TransformNullStage;
import org.opensearch.tsdb.lang.m3.stage.TruncateStage;
import org.opensearch.tsdb.lang.m3.stage.UnionStage;
import org.opensearch.tsdb.lang.m3.stage.summarize.BucketMapper;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AggregationPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AliasByTagsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AliasPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.BinaryPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.DerivativePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FallbackSeriesConstantPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FetchPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.HeadPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.HistogramPercentilePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.IntegralPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.KeepLastValuePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.LogarithmPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.M3PlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.MovingPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.OffsetPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.PerSecondPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.PerSecondRatePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.PercentileOfSeriesPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.IsNonNullPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.RemoveEmptyPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ScalePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ScaleToSecondsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ShowTagsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SortPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SqrtPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SustainPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SummarizePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TimeshiftPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TransformNullPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.UnionPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ValueFilterPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;
import org.opensearch.tsdb.lang.m3.stage.ValueFilterStage;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.opensearch.tsdb.query.search.CachedWildcardQueryBuilder;
import org.opensearch.tsdb.query.search.TimeRangePruningQueryBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregator;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;
import org.opensearch.tsdb.query.utils.AggregationConstants;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Executor for M3QL plans that produces a QueryComponentHolder.
 */
public class SourceBuilderVisitor extends M3PlanVisitor<SourceBuilderVisitor.ComponentHolder> {
    private static final LinkedHashMap<String, TimeSeriesCoordinatorAggregator.MacroDefinition> EMPTY_MAP = new LinkedHashMap<>();
    private static final String UNFOLD_NAME_SUFFIX = "_unfold";
    private static final String COORDINATOR_NAME_SUFFIX = "_coordinator";
    private static final char MULTI_CHAR_WILDCARD = '*';
    private static final char SINGLE_CHAR_WILDCARD = '?';
    private static final Metrics METRICS = new Metrics();

    private final Stack<UnaryPipelineStage> stageStack; // accumulate stages per fetch pipeline
    private final M3OSTranslator.Params params;
    private final Context context;
    private final boolean isRootVisitor; // true if this is the top-level visitor (not a child visitor)

    /**
     * Constructor for QueryComponentsVisitor.
     *
     * @param params params for query translation
     */
    public SourceBuilderVisitor(M3OSTranslator.Params params) {
        this(params, Context.newContext(), true);
    }

    public static TSDBMetrics.MetricsInitializer getMetricsInitializer() {
        return METRICS;
    }

    /**
     * Private constructor to allow passing context for recursive calls. To when processing sub-plans.
     *
     * @param params  params to inherit from parent
     * @param context context to inherit from parent
     * @param isRootVisitor whether this is the root visitor (vs a child visitor for sub-plans)
     */
    private SourceBuilderVisitor(M3OSTranslator.Params params, Context context, boolean isRootVisitor) {
        this.params = params;
        this.context = context;
        this.isRootVisitor = isRootVisitor;
        this.stageStack = new Stack<>();
    }

    private static class Context {

        // Buffer time needed for stages like moving, keepLastValue, etc.
        private long timeBuffer;

        // Cumulative time shift applied by timeshift stages
        private long timeShift;

        // Flag to track if time buffer was ever adjusted (non-zero)
        private boolean timeBufferAdjusted;

        // Adjusted truncate start time (may be earlier than query start due to summarize alignment)
        private Long truncateStartTime; // null means not yet set, use query start time
        private final Map<CacheableUnfoldAggregation, String> cacheableUnfoldReferences;

        private Context(long timeBuffer, long timeShift, boolean timeBufferAdjusted, Long truncateStartTime) {
            this.timeBuffer = timeBuffer;
            this.timeShift = timeShift;
            this.timeBufferAdjusted = timeBufferAdjusted;
            this.truncateStartTime = truncateStartTime;
            this.cacheableUnfoldReferences = new HashMap<>();
        }

        private static Context newContext() {
            return new Context(0L, 0L, false, null);
        }

        private void setTimeBuffer(long timeBuffer) {
            if (timeBuffer > 0 && timeBuffer != this.timeBuffer) {
                this.timeBufferAdjusted = true;
            }
            this.timeBuffer = timeBuffer;
        }

        private void setTimeShift(long timeShift) {
            this.timeShift = timeShift;
        }

        private long getTimeBuffer() {
            return timeBuffer;
        }

        private long getTimeShift() {
            return timeShift;
        }

        private boolean isTimeBufferAdjusted() {
            return timeBufferAdjusted;
        }

        private void setTruncateStartTime(long truncateStartTime) {
            // Only set if it's earlier than current value (or not yet set)
            if (this.truncateStartTime == null || truncateStartTime < this.truncateStartTime) {
                this.truncateStartTime = truncateStartTime;
            }
        }

        private Long getTruncateStartTime() {
            return truncateStartTime;
        }
    }

    @Override
    public ComponentHolder process(M3PlanNode planNode) {
        return planNode.accept(this);
    }

    @Override
    public ComponentHolder visit(AbsPlanNode planNode) {
        validateChildCountExact(planNode, 1);

        stageStack.add(new AbsStage());

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(AggregationPlanNode planNode) {
        validateChildCountExact(planNode, 1);

        UnaryPipelineStage stage = switch (planNode.getAggregationType()) {
            case AggregationType.SUM -> new SumStage(planNode.getTags());
            case AggregationType.AVG -> new AvgStage(planNode.getTags());
            case AggregationType.MIN -> new MinStage(planNode.getTags());
            case AggregationType.MAX -> new MaxStage(planNode.getTags());
            case AggregationType.MULTIPLY -> new MultiplyStage(planNode.getTags());
            case AggregationType.COUNT -> new CountStage(planNode.getTags());
        };

        stageStack.add(stage);

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(AliasPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        stageStack.add(new AliasStage(planNode.getAlias()));

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(AliasByTagsPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        stageStack.add(new AliasByTagsStage(planNode.getTagNames()));

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(BinaryPlanNode planNode) {
        validateChildCountExact(planNode, 2);

        return unionAndBinaryHandler(planNode);
    }

    @Override
    public ComponentHolder visit(ExcludeByTagPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        stageStack.add(new ExcludeByTagStage(planNode.getTagName(), planNode.getPatterns()));
        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(TagSubPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        stageStack.add(new TagSubStage(planNode.getTagName(), planNode.getSearchPattern(), planNode.getReplacement()));
        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(FetchPlanNode planNode) {
        // If this is the root visitor and time buffer was adjusted, add TruncateStage at the bottom of the stack
        // This ensures it executes LAST in the final coordinator pipeline
        if (isRootVisitor && context.isTimeBufferAdjusted()) {
            // Check if the first element in the stack is already a TruncateStage to avoid duplicates
            if (stageStack.isEmpty() || !(stageStack.get(0) instanceof TruncateStage)) {
                // Use adjusted truncate start time if set (e.g., by summarize with alignToFrom=false)
                // Otherwise use query start time
                long truncateStart = context.getTruncateStartTime() != null ? context.getTruncateStartTime() : params.startTime();
                stageStack.add(0, new TruncateStage(truncateStart, params.endTime()));
            }
        }

        String unfoldName = planNode.getId() + UNFOLD_NAME_SUFFIX;
        TimeRange fetchTimeRange = getAdjustedFetchTimeRange();

        // Add stages to UnfoldPipelineAggregation based on pushdown setting
        List<UnaryPipelineStage> unfoldStages = new ArrayList<>();

        if (!shouldDisablePushdown(params)) {
            TSDBMetrics.incrementCounter(METRICS.pushdownRequestsTotal, 1, Tags.create().addTag("mode", "enabled"));

            // Normal pushdown behavior: add stages until we hit a coordinator-only or global aggregation
            while (!stageStack.isEmpty() && !stageStack.peek().isCoordinatorOnly() && !stageStack.peek().isGlobalAggregation()) {
                unfoldStages.add(stageStack.pop());
            }

            // If we hit a coordinator-only stage, don't add it to unfold - it will go to coordinator
            // If we hit a global aggregation stage, add it to unfold as the last stage
            if (!stageStack.isEmpty() && !stageStack.peek().isCoordinatorOnly()) {
                unfoldStages.add(stageStack.pop());
            }
        } else {
            // emit metric
            TSDBMetrics.incrementCounter(METRICS.pushdownRequestsTotal, 1, Tags.create().addTag("mode", "disabled"));
        }

        ComponentHolder holder = new ComponentHolder(planNode.getId());

        QueryBuilder query = buildQueryForFetch(planNode, fetchTimeRange);

        // Set the query for the FetchPlanNode
        holder.addQuery(query);

        CacheableUnfoldAggregation cacheEntry = new CacheableUnfoldAggregation(query, unfoldStages, fetchTimeRange);

        String finalUnfoldName = unfoldName;
        if (context.cacheableUnfoldReferences.containsKey(cacheEntry)) {
            // we have seen exactly the same unfold query + stages before, reuse it
            finalUnfoldName = context.cacheableUnfoldReferences.get(cacheEntry);
            // we need to copy the cached result, as later there could be some in place modification of the time series
            stageStack.push(new CopyStage());
        } else {
            TimeSeriesUnfoldAggregationBuilder unfoldPipelineAggregationBuilder = new TimeSeriesUnfoldAggregationBuilder(
                unfoldName,
                unfoldStages,
                fetchTimeRange.start(),
                fetchTimeRange.end(),
                params.step()
            );

            // Set the unfold aggregation builder for the FetchPlanNode
            holder.setUnfoldAggregationBuilder(unfoldPipelineAggregationBuilder);
            // the reference name here should be the name after lifting
            // since if there's any chance this cache is useful, then this unfold will be eventually lifted to a filter aggregator
            // and whoever refer to this will need a fully qualified name, e.g. '0>0_unfold'
            context.cacheableUnfoldReferences.put(cacheEntry, planNode.getId() + AggregationConstants.BUCKETS_PATH_SEPARATOR + unfoldName);
        }

        // Add remaining stages to coordinator aggregation
        if (!stageStack.isEmpty()) {
            List<PipelineStage> stages = new ArrayList<>();
            while (!stageStack.isEmpty()) {
                stages.add(stageStack.pop());
            }

            // Set the coordinator aggregation builder for the FetchPlanNode
            holder.addPipelineAggregationBuilder(
                new TimeSeriesCoordinatorAggregationBuilder(
                    planNode.getId() + COORDINATOR_NAME_SUFFIX,
                    stages,
                    EMPTY_MAP,
                    Map.of(unfoldName, finalUnfoldName),
                    unfoldName
                )
            );
        }

        return holder;
    }

    @Override
    public ComponentHolder visit(FallbackSeriesConstantPlanNode planNode) {
        validateChildCountExact(planNode, 1);

        // Create FallbackSeriesUnaryStage with query metadata (time range and step)
        // Note: The step size uses FALLBACK_SERIES_STEP_MS for consistent granularity
        TimeRange timeRange = getAdjustedFetchTimeRange();

        FallbackSeriesUnaryStage fallbackStage = new FallbackSeriesUnaryStage(
            planNode.getConstantValue(),
            timeRange.start(),
            timeRange.end(),
            FallbackSeriesConstantPlanNode.FALLBACK_SERIES_STEP_MS
        );
        stageStack.add(fallbackStage);

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(KeepLastValuePlanNode planNode) {
        validateChildCountExact(planNode, 1);

        long originalTimeBuffer = context.getTimeBuffer();

        // Create KeepLastValueStage with optional interval
        KeepLastValueStage keepLastValueStage;
        if (planNode.duration().equals(ChronoUnit.FOREVER.getDuration())) {
            keepLastValueStage = new KeepLastValueStage();
        } else {
            keepLastValueStage = new KeepLastValueStage(getDurationAsLong(planNode.duration()));

            // TODO: M3 doesn't look back for keepLastValue, align on behavior (including for non-specified duration)
            // We currently disable this for validation. If we want to enable this, uncomment the line below.
            // context.setTimeBuffer(Math.max(context.getTimeBuffer(), getDurationAsLong(planNode.duration())));
        }

        stageStack.add(keepLastValueStage);

        try {
            return planNode.getChildren().getFirst().accept(this);
        } finally {
            context.setTimeBuffer(originalTimeBuffer);
        }
    }

    @Override
    public ComponentHolder visit(LogarithmPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        stageStack.add(new LogarithmStage());
        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(MovingPlanNode planNode) {
        validateChildCountExact(planNode, 1);

        // Record the current buffer to re-set later.
        long originalTimeBuffer = context.getTimeBuffer();

        long window;
        if (planNode.isPointBased()) {
            // Point-based: moving N means N data points, so window = N * step
            int numPoints = planNode.getPointDuration();
            window = (long) numPoints * params.step();
            MovingStage movingStage = new MovingStage(window, planNode.getAggregationType());
            stageStack.add(movingStage);

            // Extend the time buffer to fetch enough historical data for the moving window
            context.setTimeBuffer(Math.max(context.getTimeBuffer(), window));
        } else {
            // Time-based: moving 1h means 1 hour window
            window = getDurationAsLong(planNode.getTimeDuration());
            MovingStage movingStage = new MovingStage(window, planNode.getAggregationType());
            stageStack.add(movingStage);

            // Extend the time buffer to fetch enough historical data for the moving window
            context.setTimeBuffer(Math.max(context.getTimeBuffer(), window));
        }

        try {
            return planNode.getChildren().getFirst().accept(this);
        } finally {
            context.setTimeBuffer(originalTimeBuffer);
        }
    }

    @Override
    public ComponentHolder visit(DerivativePlanNode planNode) {
        validateChildCountExact(planNode, 1);

        stageStack.add(new DerivativeStage());

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(DivideScalarPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        stageStack.add(new DivideScalarStage(planNode.getDivisor()));
        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(OffsetPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        stageStack.add(new OffsetStage(planNode.getOffset()));
        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(PerSecondPlanNode planNode) {
        validateChildCountExact(planNode, 1);

        stageStack.add(new PerSecondStage());

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(PerSecondRatePlanNode planNode) {
        validateChildCountExact(planNode, 1);

        // Record the current buffer to re-set later.
        long originalTimeBuffer = context.getTimeBuffer();

        // Get interval from plan node and convert to appropriate time unit
        long interval = getDurationAsLong(planNode.getInterval());

        // unitsPerSecond=1000 for milliseconds (1 second = 1000 milliseconds)
        PerSecondRateStage perSecondRateStage = new PerSecondRateStage(interval, 1000);
        stageStack.add(perSecondRateStage);

        // Update time buffer since perSecondRate looks back by interval
        context.setTimeBuffer(Math.max(context.getTimeBuffer(), interval));

        try {
            return planNode.getChildren().getFirst().accept(this);
        } finally {
            context.setTimeBuffer(originalTimeBuffer);
        }
    }

    @Override
    public ComponentHolder visit(PercentileOfSeriesPlanNode planNode) {
        validateChildCountExact(planNode, 1);

        PercentileOfSeriesStage stage = new PercentileOfSeriesStage(
            planNode.getPercentiles(),
            planNode.isInterpolate(),
            planNode.getTags()
        );
        stageStack.add(stage);

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(HeadPlanNode planNode) {
        validateChildCountExact(planNode, 1);

        // HeadStage is a coordinator-only stage
        HeadStage headStage = new HeadStage(planNode.getLimit());
        stageStack.add(headStage);

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(SortPlanNode planNode) {
        validateChildCountExact(planNode, 1);

        // SortStage is a global aggregation that should be used as a coordinator stage
        SortStage sortStage = new SortStage(planNode.getSortBy(), planNode.getSortOrder());
        stageStack.add(sortStage);

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(SummarizePlanNode planNode) {
        validateChildCountExact(planNode, 1);

        long interval = getDurationAsLong(planNode.getInterval());
        boolean alignToFrom = planNode.isAlignToFrom();
        SummarizeStage summarizeStage = new SummarizeStage(interval, planNode.getFunction(), alignToFrom);

        // Only set reference time constant when using fixed alignment (alignToFrom=false)
        if (!alignToFrom) {
            summarizeStage.setReferenceTimeConstant(SummarizePlanNode.GO_ZERO_TIME_MILLIS);

            // When alignToFrom=false, buckets align to fixed intervals from GO_ZERO_TIME
            // This can produce samples earlier than query start time
            // Use BucketMapper's API to calculate the adjusted start time
            long adjustedStartTime = BucketMapper.calculateBucketStart(params.startTime(), interval, SummarizePlanNode.GO_ZERO_TIME_MILLIS);

            // Track this adjusted start time for TruncateStage
            context.setTruncateStartTime(adjustedStartTime);
        }

        stageStack.add(summarizeStage);

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(HistogramPercentilePlanNode planNode) {
        validateChildCountExact(planNode, 1);

        HistogramPercentileStage histogramStage = new HistogramPercentileStage(
            planNode.getBucketId(),
            planNode.getBucketRange(),
            planNode.getPercentiles()
        );
        stageStack.add(histogramStage);

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(IntegralPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        stageStack.add(new IntegralStage(planNode.isResetOnNull()));

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(IsNonNullPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        stageStack.add(new IsNonNullStage());

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(RemoveEmptyPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        stageStack.add(new RemoveEmptyStage());

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(ScalePlanNode planNode) {
        validateChildCountExact(planNode, 1);
        stageStack.add(new ScaleStage(planNode.getScaleFactor()));

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(SustainPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        long duration = getDurationAsLong(planNode.getDuration());
        stageStack.add(new SustainStage(duration));

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(SqrtPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        stageStack.add(new SqrtStage());
        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(ScaleToSecondsPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        stageStack.add(new ScaleToSecondsStage(planNode.getSeconds()));

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(ShowTagsPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        stageStack.add(new ShowTagsStage(planNode.isShowKeys(), planNode.getTags()));

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(TimeshiftPlanNode planNode) {
        validateChildCountExact(planNode, 1);
        long timeshift = getDurationAsLong(planNode.getDuration());
        stageStack.add(new TimeshiftStage(timeshift));

        context.setTimeShift(context.getTimeShift() + timeshift);

        try {
            return planNode.getChildren().getFirst().accept(this);
        } finally {
            context.setTimeShift(context.getTimeShift() - timeshift);
        }
    }

    @Override
    public ComponentHolder visit(TransformNullPlanNode planNode) {
        validateChildCountExact(planNode, 1);

        // Note: metadata parameters no longer needed - TransformNullStage reads from TimeSeries metadata
        TransformNullStage transformNullStage = new TransformNullStage(planNode.getFillValue());
        stageStack.add(transformNullStage);

        return planNode.getChildren().getFirst().accept(this);
    }

    @Override
    public ComponentHolder visit(UnionPlanNode planNode) {
        validateChildCountAtLeast(planNode, 2);

        return unionAndBinaryHandler(planNode);
    }

    @Override
    public ComponentHolder visit(ValueFilterPlanNode planNode) {
        validateChildCountExact(planNode, 1);

        ValueFilterStage valueFilterStage = new ValueFilterStage(planNode.getFilter(), planNode.getTargetValue());
        stageStack.add(valueFilterStage);

        return planNode.getChildren().getFirst().accept(this);
    }

    private static void validateChildCountExact(M3PlanNode node, int expected) {
        if (node.getChildren().size() != expected) {
            throw new IllegalStateException(
                node.getClass().getSimpleName()
                    + " must have exactly "
                    + (expected == 1 ? "one child" : (expected == 2 ? "two children" : expected + " children"))
            );
        }
    }

    private static void validateChildCountAtLeast(M3PlanNode node, int minimum) {
        if (node.getChildren().size() < minimum) {
            throw new IllegalStateException(
                node.getClass().getSimpleName()
                    + " must have at least "
                    + (minimum == 1 ? "one child" : (minimum == 2 ? "two children" : minimum + " children"))
            );
        }
    }

    private ComponentHolder unionAndBinaryHandler(M3PlanNode planNode) {
        ComponentHolder[] childComponents = new ComponentHolder[planNode.getChildren().size()];
        for (int i = 0; i < planNode.getChildren().size(); i++) {
            M3PlanNode child = planNode.getChildren().get(i);
            // Create child visitors with isRootVisitor=false since these are sub-plans
            childComponents[i] = new SourceBuilderVisitor(params, context, false).process(child);
        }
        ComponentHolder merged = ComponentHolder.merge(planNode.getId(), childComponents);

        // Add a UnionStage for each additional child beyond the first
        List<PipelineStage> stages = new ArrayList<>();
        NavigableMap<String, String> referencesMap = new TreeMap<>();

        for (int i = 1; i < childComponents.length; i++) {
            ComponentHolder rhsComponents = childComponents[i];

            String rhsId = String.valueOf(rhsComponents.getId());

            PipelineStage stage = getStageFor(planNode, rhsId);
            stages.add(stage);

            String refChain = getTerminalReference(rhsComponents);
            referencesMap.put(rhsId, refChain);
        }

        while (!stageStack.isEmpty()) {
            stages.add(stageStack.pop());
        }

        // Add TruncateStage at the end if time buffer was adjusted AND this is the root visitor
        // This is the final coordinator for multi-fetch queries (union/binary operations)
        if (isRootVisitor && context.isTimeBufferAdjusted()) {
            // Use adjusted truncate start time if set (e.g., by summarize with alignToFrom=false)
            // Otherwise use query start time
            long truncateStart = context.getTruncateStartTime() != null ? context.getTruncateStartTime() : params.startTime();
            stages.add(new TruncateStage(truncateStart, params.endTime()));
        }

        // Add the left-hand side reference
        ComponentHolder lhsComponents = childComponents[0];
        String lhsId = String.valueOf(lhsComponents.getId());
        referencesMap.put(lhsId, getTerminalReference(lhsComponents));
        merged.addPipelineAggregationBuilder(
            new TimeSeriesCoordinatorAggregationBuilder(String.valueOf(planNode.getId()), stages, EMPTY_MAP, referencesMap, lhsId)
        );

        return merged;
    }

    private PipelineStage getStageFor(M3PlanNode planNode, String rhsReferenceName) {
        if (planNode instanceof DiffPlanNode diffPlanNode) {
            return new SubtractStage(rhsReferenceName, diffPlanNode.isKeepNans(), diffPlanNode.getTags());
        }
        if (planNode instanceof AsPercentPlanNode asPercentPlanNode) {
            return new AsPercentStage(rhsReferenceName, asPercentPlanNode.getTags());
        }
        if (planNode instanceof DividePlanNode dividePlanNode) {
            return new DivideStage(rhsReferenceName, dividePlanNode.getTags());
        }
        if (planNode instanceof FallbackSeriesBinaryPlanNode) {
            return new FallbackSeriesBinaryStage(rhsReferenceName);
        }
        if (planNode instanceof IntersectPlanNode intersectPlanNode) {
            return new IntersectStage(rhsReferenceName, intersectPlanNode.getTags());
        }
        if (planNode instanceof UnionPlanNode) {
            return new UnionStage(rhsReferenceName);
        }
        throw new IllegalArgumentException("Unsupported plan node type for binary operation: " + planNode.getClass().getSimpleName());
    }

    /* package-private for test purpose */
    static QueryBuilder buildQueryForFetch(FetchPlanNode planNode, TimeRange range) {
        // In the format field => [value1, value2, ...]
        Map<String, List<String>> matchFilters = planNode.getMatchFilters();
        Map<String, List<String>> inverseMatchFilters = planNode.getInverseMatchFilters();

        if (matchFilters.isEmpty()) {
            throw new IllegalArgumentException("FetchPlanNode must have at least one match");
        }

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // Add time filtering before tag filtering, so segments can be pruned without collecting terms in certain query paths
        boolQuery.filter(QueryBuilders.rangeQuery(Constants.IndexSchema.TIMESTAMP_RANGE).gte(range.start).lt(range.end));

        // Add filter clauses from matchFilters
        for (Map.Entry<String, List<String>> entry : matchFilters.entrySet()) {
            String field = entry.getKey();
            List<String> values = entry.getValue();
            boolQuery.filter(createFieldQuery(field, values));
        }

        // Add must_not clauses from inverseMatchFilters
        for (Map.Entry<String, List<String>> entry : inverseMatchFilters.entrySet()) {
            String field = entry.getKey();
            List<String> values = entry.getValue();
            boolQuery.mustNot(createFieldQuery(field, values));
        }

        // Wrap with TimeRangePruningQueryBuilder to prune non-overlapping leaves.
        return new TimeRangePruningQueryBuilder(boolQuery, range.start, range.end);
    }

    static private QueryBuilder createFieldQuery(String field, List<String> values) {
        // If multiple values, build a should query (OR)
        if (values.size() == 1) {
            String value = values.getFirst();
            if (containsWildcard(value)) {
                return new CachedWildcardQueryBuilder(Constants.IndexSchema.LABELS, labelFilterString(field, value));
            } else {
                return QueryBuilders.termsQuery(Constants.IndexSchema.LABELS, labelFilterString(field, value));
            }
        } else {
            // Multiple values: separate wildcard and non-wildcard values
            List<String> wildcardValues = new ArrayList<>();
            List<String> exactValues = new ArrayList<>();

            for (String value : values) {
                if (containsWildcard(value)) {
                    wildcardValues.add(value);
                } else {
                    exactValues.add(value);
                }
            }

            // If all values are exact (no wildcards), use single terms query for better performance
            if (wildcardValues.isEmpty()) {
                List<String> labelFilters = exactValues.stream().map(v -> labelFilterString(field, v)).collect(Collectors.toList());
                return QueryBuilders.termsQuery(Constants.IndexSchema.LABELS, labelFilters);
            }

            // Mixed or all wildcards: build bool should query
            BoolQueryBuilder innerBool = QueryBuilders.boolQuery();

            // Add single terms query for all exact values (instead of N term queries)
            if (!exactValues.isEmpty()) {
                List<String> labelFilters = exactValues.stream().map(v -> labelFilterString(field, v)).collect(Collectors.toList());
                innerBool.should(QueryBuilders.termsQuery(Constants.IndexSchema.LABELS, labelFilters));
            }

            // Add wildcard queries
            for (String value : wildcardValues) {
                innerBool.should(new CachedWildcardQueryBuilder(Constants.IndexSchema.LABELS, labelFilterString(field, value)));
            }

            innerBool.minimumShouldMatch(1);
            return innerBool;
        }
    }

    private static boolean containsWildcard(String value) {
        for (int i = 0, len = value.length(); i < len; i++) {
            char c = value.charAt(i);
            if (c == MULTI_CHAR_WILDCARD || c == SINGLE_CHAR_WILDCARD) {
                return true;
            }
        }
        return false;
    }

    private static String labelFilterString(String label, String filter) {
        return label + LabelConstants.LABEL_DELIMITER + filter;
    }

    private String getTerminalReference(ComponentHolder queryComponentHolder) {
        // If there is at least one pipeline stage, the last one is guaranteed to be the final aggregation stage
        if (!queryComponentHolder.getPipelineAggregationBuilders().isEmpty()) {
            return queryComponentHolder.getPipelineAggregationBuilders().getLast().getName();
        }

        // Otherwise, return the unfold aggregation reference
        return queryComponentHolder.getId() + AggregationConstants.BUCKETS_PATH_SEPARATOR + queryComponentHolder
            .getUnfoldAggregationBuilder()
            .getName();
    }

    private TimeRange getAdjustedFetchTimeRange() {
        long adjustedStart = params.startTime() - context.getTimeShift() - context.getTimeBuffer();
        long adjustedEnd = params.endTime() - context.getTimeShift();
        return new TimeRange(adjustedStart, adjustedEnd);
    }

    /* package-private for testing purpose */
    record TimeRange(long start, long end) {
    }

    /**
     * Determines whether pushdown should be disabled based on query parameters and federation metadata.
     *
     * <p>Pushdown is disabled if:
     * <ul>
     *   <li>The pushdown flag is explicitly set to false in params, OR</li>
     *   <li>Partitions overlap (same time series exists across multiple partitions with temporal overlap,
     *       which would produce incorrect results for operations requiring historical context)</li>
     * </ul>
     *
     * @param params the translator parameters containing pushdown flag and federation metadata
     * @return true if pushdown should be disabled, false otherwise
     */
    public static boolean shouldDisablePushdown(M3OSTranslator.Params params) {
        return !params.pushdown() || (params.federationMetadata() != null && params.federationMetadata().hasOverlappingPartitions());
    }

    private long getDurationAsLong(Duration duration) {
        switch (params.timeUnit()) {
            case MILLISECONDS:
                return duration.toMillis();
            case SECONDS:
                return duration.getSeconds();
            default:
                throw new IllegalArgumentException("Unsupported time unit: " + params.timeUnit().name());
        }
    }

    /**
     * Holds the components needed to construct a query or sub-query. This is used to collect opensearch aggregations while
     * traversing the M3QL plan, and allows us to defer construction the final query to ensure components are used in the
     * optimal place. For example, we defer creating a parent QueryBuilder until we've visited the children as the query may or may not
     * be needed in a filter aggregation depending on the plan structure.
     */
    public static class ComponentHolder {
        private final int id;
        private final List<FilterAggregationBuilder> filterAggregationBuilders;
        private final List<TimeSeriesCoordinatorAggregationBuilder> pipelineAggregationBuilders;
        private final Set<QueryBuilder> dnfQueries; // disjunctive normal form of fetch queries
        private TimeSeriesUnfoldAggregationBuilder unfoldAggregationBuilder;

        public ComponentHolder(int id) {
            this.id = id;
            this.filterAggregationBuilders = new ArrayList<>();
            this.pipelineAggregationBuilders = new ArrayList<>();
            this.dnfQueries = new LinkedHashSet<>(); // preserve the order to make writing test easier
        }

        public void addQuery(QueryBuilder queryBuilder) {
            this.dnfQueries.add(queryBuilder);
        }

        public QueryBuilder getFullQuery() {
            return getMergedQuery(dnfQueries);
        }

        public int getId() {
            return id;
        }

        private static ComponentHolder merge(int id, ComponentHolder... holders) {
            assert holders.length >= 2 : "should only merge multiple ComponentHolders";

            ComponentHolder merged = new ComponentHolder(id);

            // Collect filtered aggregations (per unfolds)
            for (ComponentHolder holder : holders) {
                // Add an existing, or build a new FilterAggregationBuilder for each QueryComponentHolder
                if (!holder.getFilterAggregationBuilders().isEmpty()) {
                    for (FilterAggregationBuilder existing : holder.getFilterAggregationBuilders()) {
                        merged.addFilterAggregationBuilder(existing); // lift the existing FilterAggregationBuilder
                        merged.addQuery(existing.getFilter());
                    }
                } else if (holder.getUnfoldAggregationBuilder() != null) {
                    // the unfold aggregation builder could be null as we may just refer to a cached unfold aggregation
                    FilterAggregationBuilder filterAgg = new FilterAggregationBuilder(
                        String.valueOf(holder.getId()),
                        holder.getFullQuery()
                    );
                    filterAgg.subAggregation(holder.getUnfoldAggregationBuilder());
                    merged.addFilterAggregationBuilder(filterAgg);
                    merged.addQuery(filterAgg.getFilter());
                }

                // Lift all PipelineAggregationBuilders if present
                for (TimeSeriesCoordinatorAggregationBuilder existing : holder.getPipelineAggregationBuilders()) {
                    // Lifting a pipeline agg change the scope, and the existing reference must be updated
                    Map<String, String> references = existing.getReferences()
                        .entrySet()
                        .stream()
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue().equals(holder.getId() + UNFOLD_NAME_SUFFIX)
                                    ? holder.getId() + AggregationConstants.BUCKETS_PATH_SEPARATOR + holder.getUnfoldAggregationBuilder()
                                        .getName()
                                    : e.getValue()
                            )
                        );

                    TimeSeriesCoordinatorAggregationBuilder lifted = new TimeSeriesCoordinatorAggregationBuilder(
                        existing.getName(),
                        existing.getStages(),
                        EMPTY_MAP,
                        references,
                        existing.getInputReference()
                    );

                    merged.addPipelineAggregationBuilder(lifted);
                }
            }

            return merged;
        }

        // TODO: explore using matchAll (with filtered aggs doing post-filtering)
        private static QueryBuilder getMergedQuery(Collection<QueryBuilder> queries) {
            if (queries.size() == 1) {
                return queries.iterator().next();
            }
            BoolQueryBuilder mergedQuery = QueryBuilders.boolQuery();
            for (QueryBuilder qb : queries) {
                mergedQuery.should(qb);
            }
            mergedQuery.minimumShouldMatch(1);
            return mergedQuery;
        }

        private void addFilterAggregationBuilder(FilterAggregationBuilder filterAggregationBuilder) {
            filterAggregationBuilders.add(filterAggregationBuilder);
        }

        private void addPipelineAggregationBuilder(TimeSeriesCoordinatorAggregationBuilder pipelineAggregationBuilder) {
            pipelineAggregationBuilders.add(pipelineAggregationBuilder);
        }

        private List<FilterAggregationBuilder> getFilterAggregationBuilders() {
            return filterAggregationBuilders;
        }

        private List<TimeSeriesCoordinatorAggregationBuilder> getPipelineAggregationBuilders() {
            return pipelineAggregationBuilders;
        }

        private TimeSeriesUnfoldAggregationBuilder getUnfoldAggregationBuilder() {
            return unfoldAggregationBuilder;
        }

        private void setUnfoldAggregationBuilder(TimeSeriesUnfoldAggregationBuilder unfoldAggregationBuilder) {
            this.unfoldAggregationBuilder = unfoldAggregationBuilder;
        }

        public SearchSourceBuilder toSearchSourceBuilder() {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0);
            searchSourceBuilder.query(getFullQuery());

            if (unfoldAggregationBuilder != null) {
                searchSourceBuilder.aggregation(unfoldAggregationBuilder);
            }
            for (FilterAggregationBuilder filterAgg : filterAggregationBuilders) {
                searchSourceBuilder.aggregation(filterAgg);
            }

            // TODO: Leverage TimeSeriesCoordinatorAggregationBuilder's macro functionality to merge multiple pipeline aggs into one
            for (PipelineAggregationBuilder pipelineAgg : reorderPipelineAggregations()) {
                searchSourceBuilder.aggregation(pipelineAgg);
            }
            return searchSourceBuilder;
        }

        private List<TimeSeriesCoordinatorAggregationBuilder> reorderPipelineAggregations() {
            // reorder such that the pipeline aggregation with CopyStages will be executed first
            // It's necessary since otherwise the CopyStage may not be able to copy the original copy
            // And it is safe since currently all CopyStage will only be in unary pipelines and won't affect
            // the evaluation order of the binary (or more operands) operations, as they will be after the unary operations anyway
            List<TimeSeriesCoordinatorAggregationBuilder> reordered = new ArrayList<>();
            for (TimeSeriesCoordinatorAggregationBuilder pipelineAgg : pipelineAggregationBuilders) {
                if (pipelineAgg.getStages().getFirst() instanceof CopyStage) {
                    reordered.add(pipelineAgg);
                }
            }
            for (TimeSeriesCoordinatorAggregationBuilder pipelineAgg : pipelineAggregationBuilders) {
                if (!(pipelineAgg.getStages().getFirst() instanceof CopyStage)) {
                    reordered.add(pipelineAgg);
                }
            }
            return reordered;
        }
    }

    /**
     * Metrics container for SourceBuilderVisitor.
     */
    static class Metrics implements TSDBMetrics.MetricsInitializer {
        static final String PUSHDOWN_REQUESTS_TOTAL_METRIC_NAME = "tsdb.lang.m3ql.source_builder_visitor.pushdown.total";
        Counter pushdownRequestsTotal;

        @Override
        public void register(MetricsRegistry registry) {
            pushdownRequestsTotal = registry.createCounter(
                PUSHDOWN_REQUESTS_TOTAL_METRIC_NAME,
                "total number of pushdown vs non-pushdown m3ql fetch statements processed",
                TSDBMetricsConstants.UNIT_COUNT
            );
        }

        @Override
        public void cleanup() {
            pushdownRequestsTotal = null;
        }
    }

    private record CacheableUnfoldAggregation(QueryBuilder query, List<UnaryPipelineStage> stages, TimeRange fetchTimeRange) {
    }
}
