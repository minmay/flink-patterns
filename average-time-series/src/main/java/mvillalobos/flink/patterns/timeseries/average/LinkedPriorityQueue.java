package mvillalobos.flink.patterns.timeseries.average;

import java.util.Optional;
import java.util.function.Function;

/**
 * A linked priority queue.
 *
 * @param <T> The type that contains a priority.
 * @param <F> The priority type.
 */
public class LinkedPriorityQueue<T, F extends Comparable<F>> {

    /** Maps an instance into a priority. */
    private final Function<T, F> mapToPriority;

    /** The head of the priority queue. */
    private Node<T> head = null;

    /** The number elements in this queue. */
    private int n;

    /**
     * Creates an instance of this priority queue.
     *
     * @param mapToPriority A lambda function that maps instance into a priority.
     */
    public LinkedPriorityQueue(Function<T, F> mapToPriority) {
        this.mapToPriority = mapToPriority;
    }

    /**
     * Enqueues the value. The value with the highest priority will be first in the list.
     *
     * @param value The value to enqueue.
     */
    public void enqueue(T value) {
        Node<T> node = new Node<>(value);
        if (head == null) {
            head = node;
        } else {
            Node<T> runner = head;
            Node<T> trailer = null;
            while (runner != null) {
                if (mapToPriority.apply(node.value).compareTo(mapToPriority.apply(runner.value)) > 0) {
                    if (trailer == null) {
                        head = node;
                    } else {
                        trailer.next = node;
                    }
                    node.next = runner;
                    break;
                }

                trailer = runner;
                runner = runner.next;
                if (runner == null) {
                    trailer.next = node;
                }
            }
        }
        n++;
    }

    /**
     * Dequeues the value with the highest priority.
     *
     * @return The value with the highest priority, or empty if the list is empty.
     */
    public Optional<T> dequeue() {
        if (head == null) {
            return Optional.empty();
        } else {
            T value = head.value;
            head = head.next;
            return Optional.of(value);
        }
    }

    /**
     * Finds an element in the list that has an equal or less priority than the given priority.
     * If such an element exists, then this cuts the remaining children (exclusive).
     *
     * @param priority The priority of equal or lesser value to find in the priority queue.
     * @return An element with a priority that is less than or equal to the given priority.
     */
    public  Optional<T> findThenTruncate(F priority) {
        Node<T> runner = head;
        int counter = 0;
        while (runner != null) {

            if (mapToPriority.apply(runner.value).compareTo(priority) <= 0) {
                runner.next = null;
                n -= counter;
                return Optional.of(runner.value);
            }
            runner = runner.next;
            counter++;
        }
        return Optional.empty();
    }

    /**
     * Evaluates whether this priority queue is empty.
     * @return true if this priority queue is empty, false otherwise.
     */
    public boolean isEmpty() {
        return head == null;
    }

    /**
     * Retrieves the size of this priority queue.
     * @return the size.
     */
    public int getSize() {
        return n;
    }

    @Override
    public String toString() {
        Node<T> runner = head;
        StringBuilder builder = new StringBuilder();
        builder.append("head -> ");
        while (runner != null) {
            builder.append(runner.value);
            builder.append(" -> ");
            runner = runner.next;
        }
        builder.append("null");
        return builder.toString();
    }

    /**
     * A node element in the priority queue.
     * @param <T> The type that contains a priority.
     */
    private static class Node<T> {

        /** The node value. */
        private T value;

        /** A pointer to the next value. */
        private Node<T> next;

        /**
         * Creates a node instance.
         * @param value The value of this node.
         */
        public Node(T value) {
            this.value = value;
        }
    }
}
