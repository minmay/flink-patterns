package mvillalobos.flink.patterns.timeseries.average;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Keyed time series that uses a graph and priority queue to represent
 * the time series.
 *
 * In this time-series, every element has a name, value, and timestamp.
 *
 * The graph is keyed by name. The priority queue orders the time stamps of the time series.
 * For example:
 * <pre>
 * a -&gt; (55, 2020-06-23 00:30:00.0) -&gt; (15, 2020-06-23 00:00:00.0) -&gt; (75, 2020-06-22 23:45:00.0) -&gt; null
 * b -&gt; (35, 2020-06-23 00:15:00.0) -&gt; (30, 2020-06-23 00:00:00.0) -&gt; null
 * </pre>
 *
 * @param <S> The element containing the time series.
 * @param <N> The symbol of the time series.
 * @param <T> The timestamp of the time series.
 */
public class TimeSeriesGraph<S, N, T extends Comparable<T>> {

    private final static Logger logger = LoggerFactory.getLogger(TimeSeriesGraph.class);

    /**
     * Map the time series element into its name symbol.
     */
    private final Function<S, N> mapToNameSymbol;

    /**
     * Maps the time series element into its time series timestamp.
     */
    private final Function<S, T> mapToTimestamp;

    /**
     * Persistent data-structure that contains name symbols.
     */
    private final ValueState<Set<N>> nameSymbolTable;

    /**
     * Persistent data-structure that contains adjacency list.
     */
    private final MapState<N, LinkedPriorityQueue<S, T>> adj;

    public TimeSeriesGraph(ValueState<Set<N>> nameSymbolTable, MapState<N, LinkedPriorityQueue<S, T>> adj, Function<S, N> mapToNameSymbol, Function<S, T> mapToTimesamp) {
        this.mapToNameSymbol = mapToNameSymbol;
        this.mapToTimestamp = mapToTimesamp;
        this.nameSymbolTable = nameSymbolTable;
        this.adj = adj;
    }

    /**
     * Adds the time series element to the graph and name symbol table.
     *
     * @param value The time series element to add.
     * @throws Exception When there is a persistence error.
     */
    public void add(S value) throws Exception {

        final N name = mapToNameSymbol.apply(value);
        nameSymbolTable.value().add(name);

        if (adj.contains(name)) {
            adj.get(name).enqueue(value);
        } else {
            LinkedPriorityQueue<S, T> list = new LinkedPriorityQueue<>(
                    mapToTimestamp
            );
            list.enqueue(value);
            adj.put(name, list);
        }

        logger.info("Added time series {} to pq: {}", value, adj.get(name));
    }

    /**
     * Given a timestamp, then for every name symbol in the graph, this
     * finds the first time series element that has a time stamp that is less than or equal to the given timestamp,
     * then this exclusively cuts the time series elements after the found element in that priority queue,
     * then this passes it on to the consumer if there is a match, however, if the match is not exact it
     * the found element is mapped to a back fill before it passed on to the consumer.
     *
     * Given the following graph, and the timestamp 2020-06-23 00:30:00.0
     * <pre>
     * a -&gt; (55, 2020-06-23 00:30:00.0) -&gt; (15, 2020-06-23 00:00:00.0) -&gt; (75, 2020-06-22 23:45:00.0) -&gt; null
     * b -&gt; (35, 2020-06-23 00:15:00.0) -&gt; (30, 2020-06-23 00:00:00.0) -&gt; null
     * </pre>
     * Then this would alter the graph to:
     * <pre>
     * a -&gt; (55, 2020-06-23 00:30:00.0) -&gt; null
     * b -&gt; (35, 2020-06-23 00:15:00.0) -&gt; null
     * </pre>
     * and return the values
     * <pre>
     * (a, 55, 2020-06-23 00:30:00.0)
     * (b, 35, 2020-06-23 00:30:00.0)
     * </pre> to the consumer.  Notice that time series with symbol b was backfilled.
     *
     * @param timestamp The timestamp to process.
     * @param consumer The consumer used to collect time series events.
     * @param mapToBackFill A bi-function that transforms an element and timestamp into a back fill.
     * @throws Exception When there is a persistence error.
     */
    public void findThenCutRemainder(T timestamp, Consumer<S> consumer, BiFunction<S, T, S> mapToBackFill) throws Exception {
        for (N name : nameSymbolTable.value()) {

            if (adj.contains(name)) {
                final LinkedPriorityQueue<S, T> list = adj.get(name);

                final Optional<S> optionalValue =
                        list.findThenTruncate(timestamp);

                if (optionalValue.isEmpty()) {
                    logger.info("search for timestamp: {} with name: {} has no value", timestamp, name);
                } else {
                    final S candidate = optionalValue.get();

                    if (mapToTimestamp.apply(candidate).equals(timestamp)) {
                        consumer.accept(candidate);
                        logger.info("search for timestamp: {} with name: {} found: {}", timestamp, name, candidate);
                    } else {
                        final S backfill = mapToBackFill.apply(candidate, timestamp);
                        consumer.accept(backfill);
                        logger.info("search for timestamp: {} with name: {} found back fill: {}", timestamp, name, backfill);
                    }
                }
            }
        }
    }
}
