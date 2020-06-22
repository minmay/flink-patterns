package mvillalobos.flink.patterns.timeseries.average;

import java.util.Comparator;
import java.util.Optional;
import java.util.function.Function;

public class PriorityQueue<T, F> {

    private final Function<T, F> mapToField;
    private final Comparator<F> comparator;
    private Node<T> head = null;
    private int n;

    public PriorityQueue(Function<T, F> mapToField, Comparator<F> comparator) {
        this.mapToField = mapToField;
        this.comparator = comparator;
    }

    public void enqueue(T value) {
        Node<T> node = new Node<>(value);
        if (head == null) {
            head = node;
        } else {
            Node<T> runner = head;
            Node<T> trailer = null;
            while (runner != null) {
                if (comparator.compare(mapToField.apply(node.value), mapToField.apply(runner.value)) > 0) {
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

            if (comparator.compare(mapToField.apply(runner.value), value) <= 0) {
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
