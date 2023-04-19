import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Main {
    private static final String PATH = "stock_orders.xml";

    public static void main(String[] args) throws Exception {

        String path = (args.length > 0) ? args[0] : PATH;
 
        List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);

        Set<Order> orders = Parser.parse(lines);
        System.out.println("Orders: " + orders.size());

        Map<String, Book> books = Sorter.sort(orders);
        System.out.println("Books: " + books.size());

        books.values().forEach(System.out::println);        
    }
}