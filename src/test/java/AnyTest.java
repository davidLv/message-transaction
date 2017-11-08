import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author elvis.xu
 * @since 2017-11-07 18:38
 */
public class AnyTest {
	public static void main(String[] args) {
		ExecutorService s = Executors.newFixedThreadPool(2);
		s.submit(() -> System.out.println(1));
		s.submit(() -> System.out.println(2));
		s.submit(() -> System.out.println(3));
		s.submit(() -> System.out.println(4));
		s.submit(() -> System.out.println(5));
		s.shutdown();
	}
}
