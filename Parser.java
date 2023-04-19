import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Collections;

public class Parser {

    public static Set<Order> parse(List<String> lines) {
        Spliterator<String> spliterator1 = lines.spliterator();
        Spliterator<String> spliterator2 = spliterator1.trySplit();
        Spliterator<String> spliterator3 = spliterator1.trySplit();
        Spliterator<String> spliterator4 = spliterator2.trySplit();

        Callable<List<Set<Order>>> task1 = () -> {
            return parsePart(spliterator1);
        };
        Callable<List<Set<Order>>> task2 = () -> {
            return parsePart(spliterator2);
        };
        Callable<List<Set<Order>>> task3 = () -> {
            return parsePart(spliterator3);
        };
        Callable<List<Set<Order>>> task4 = () -> {
            return parsePart(spliterator4);
        };

        List<Callable<List<Set<Order>>>> tasks = List.of(task1, task2, task3, task4);

        Set<Order> addOrders = new HashSet<>();
        Set<Order> deleteOrders = new HashSet<>();

        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<List<Set<Order>>>> results = executor.invokeAll(tasks);
            
            for (Future<List<Set<Order>>> result: results) {
                List<Set<Order>> orders = result.get();
                addOrders.addAll(orders.get(0));
                deleteOrders.addAll(orders.get(1));
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        executor.shutdown();

        addOrders.removeAll(deleteOrders);
        return Collections.unmodifiableSet(addOrders);
    }

    private static List<Set<Order>> parsePart(Spliterator<String> spliterator) {
        Set<Order> addOrders = new HashSet<>();
        Set<Order> deleteOrders = new HashSet<>();

        spliterator.forEachRemaining(line -> {     
            if (line.startsWith("\t<A")) {
                addOrders.add(parseAddOrderLine(line));
            } else if (line.startsWith("\t<D")) {
                deleteOrders.add(parseDeleteOrderLine(line));
            }
        });

        return List.of(addOrders, deleteOrders);
    }

    // <AddOrder book="stock-32" operation="SELL" price="76.53" volume="207" orderId="29" />
    private static Order parseAddOrderLine(String line) {
        String[] values = new String[5];

        for (int i = 0, start = 0; i < values.length; i++) {
            start = line.indexOf('\"', start);
            start++;
            int end = line.indexOf('\"', start);
            values[i] = line.substring(start, end);
            start = end + 1;
        }

        String book = values[0];
        Order.Operation operation = Order.Operation.valueOf(values[1]);
        float price = Float.parseFloat(values[2]);
        int volume = Integer.parseInt(values[3]);
        int id = Integer.parseInt(values[4]);

        return new Order(id, book, operation, price, volume);
    }

    // <DeleteOrder orderId="16" />
    private static Order parseDeleteOrderLine(String line) {
        int start = line.indexOf('\"');
        int end = line.indexOf('\"', start + 1);
        
        return new Order(Integer.parseInt(line.substring(start + 1, end)));
    }
}
