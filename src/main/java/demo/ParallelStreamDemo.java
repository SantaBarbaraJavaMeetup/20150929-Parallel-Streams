package demo;

import java.util.Spliterator;
import java.util.concurrent.RecursiveAction;
import java.util.function.Consumer;

public class ParallelStreamDemo {

    static String[] alphabet = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"};

    public static void main(String[] args) {
        Consumer<String> consumer = (s) -> System.out.println("[" + Thread.currentThread().getName() + "] " + s);

//        Stream.of(alphabet).parallel().forEach(consumer);

        parallelize(alphabet, consumer);
    }

    static <T> void parallelize(T[] array, Consumer<T> consumer) {
        Spliterator<T> spliterator = new ArraySpliterator<>(array);
        SpliteratorTask<T> task = new SpliteratorTask<>(spliterator, consumer, array.length / Runtime.getRuntime().availableProcessors());
        task.compute();
    }

    static class ArraySpliterator<T> implements Spliterator<T> {
        final T[] array;
        int position;
        final int end;

        ArraySpliterator(T[] array) {
            this(array, 0, array.length);
        }

        ArraySpliterator(T[] array, int position, int end) {
            this.array = array;
            this.position = position;
            this.end = end;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if (position == end) {
                return false;
            }

            action.accept(array[position++]);
            return true;
        }

        @Override
        public Spliterator<T> trySplit() {
            int range = end - position;
            if (range <= 1) {
                return null;
            }

            Spliterator<T> split = new ArraySpliterator<>(array, position, position + range / 2);
            position += range / 2;
            return split;
        }

        @Override
        public long estimateSize() {
            return end - position;
        }

        @Override
        public int characteristics() {
            return 0;
        }
    }

    static class SpliteratorTask<T> extends RecursiveAction {
        final Spliterator<T> spliterator;
        final Consumer<T> consumer;
        final int targetSize;

        SpliteratorTask(Spliterator<T> spliterator, Consumer<T> consumer, int targetSize) {
            this.spliterator = spliterator;
            this.consumer = consumer;
            this.targetSize = targetSize;
        }

        @Override
        protected void compute() {
            Spliterator<T> split;
            if (spliterator.estimateSize() <= targetSize || (split = spliterator.trySplit()) == null) {
                spliterator.forEachRemaining(consumer);
                return;
            }

            invokeAll(new SpliteratorTask<>(spliterator, consumer, targetSize),
                    new SpliteratorTask<>(split, consumer, targetSize));
        }
    }

}
