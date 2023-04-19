import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Sorter {
    
    public static Map<String, Book> sort(Set<Order> orders) {
        Spliterator<Order> spliterator1 = orders.spliterator();
        Spliterator<Order> spliterator2 = spliterator1.trySplit();
        Spliterator<Order> spliterator3 = spliterator1.trySplit();
        Spliterator<Order> spliterator4 = spliterator2.trySplit();
        
        Callable<Map<String, Book>> task1 = () -> {
            return sortPart(spliterator1);
        };
        Callable<Map<String, Book>> task2 = () -> {
            return sortPart(spliterator2);
        };
        Callable<Map<String, Book>> task3 = () -> {
            return sortPart(spliterator3);
        };
        Callable<Map<String, Book>> task4 = () -> {
            return sortPart(spliterator4);
        };

        List<Callable<Map<String, Book>>> tasks = List.of(task1, task2, task3, task4);

        List<Map<String, Book>> parts = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<Map<String, Book>>> results = executor.invokeAll(tasks);
            
            for (Future<Map<String, Book>> result: results) {
                parts.add(result.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        executor.shutdown();

        return joinParts(parts);
    }

    private static Map<String, Book> joinParts(List<Map<String, Book>> parts) {
        Map<String, Book> result = new HashMap<>(parts.get(0));
        for (int i = 1; i < parts.size(); i++) {
            Map<String, Book> part = parts.get(i);
            for (String key: part.keySet()) {
                Book newBook = part.get(key);
                Book resultBook = result.get(key);
                if (resultBook == null) {
                    result.put(key, newBook);
                } else {
                    resultBook.merge(newBook);
                }
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private static Map<String, Book> sortPart(Spliterator<Order> spliterator) {
        Map<String, Book> books = new HashMap<>();

        spliterator.forEachRemaining((order) -> {
            Book book = books.get(order.book);
            if (book == null) {
                book = new Book(order.book);
                books.put(order.book, book);
            }
            book.put(order);
        });
        return books;
    }
}
