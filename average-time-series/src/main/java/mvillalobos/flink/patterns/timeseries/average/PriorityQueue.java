package mvillalobos.flink.patterns.timeseries.average;

import java.util.Optional;
import java.util.function.Function;

public class PriorityQueue<T, F extends Comparable<F>> {

    private final Function<T, F> mapToComparable;
    private Node<T> head = null;
    private int n;

    public PriorityQueue(Function<T, F> mapToComparable) {
        this.mapToComparable = mapToComparable;
    }

    public void enqueue(T value) {
        Node<T> node = new Node<>(value);
        if (head == null) {
            head = node;
        } else {
            Node<T> runner = head;
            Node<T> trailer = null;
            while (runner != null) {
                if (mapToComparable.apply(node.value).compareTo(mapToComparable.apply(runner.value)) > 0) {
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

    public Optional<T> dequeue() {
        if (head == null) {
            return Optional.empty();
        } else {
            T value = head.value;
            head = head.next;
            return Optional.of(value);
        }
    }

    public  Optional<T> findThenTruncate(F value) {
        Node<T> runner = head;
        int counter = 0;
        while (runner != null) {

            if (mapToComparable.apply(runner.value).compareTo(value) <= 0) {
                runner.next = null;
                n -= counter;
                return Optional.of(runner.value);
            }
            runner = runner.next;
            counter++;
        }
        return Optional.empty();
    }

    public boolean isEmpty() {
        return head == null;
    }

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

    private static class Node<T> {
        private T value;
        private Node<T> next;

        public Node(T value) {
            this.value = value;
        }
    }
}
