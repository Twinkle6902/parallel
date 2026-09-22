import java.util.Arrays;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        myFunc light = x -> 2 * x;

        myFunc heavy = x -> {
            double acc = 0;
            for (int i = 0; i < 2000; i++) {
                acc += Math.sin(x + i) * Math.cos(x - i);
            }
            return 2 * x + acc * 1e-9;
        };

        int n_1 = 20_000_000;
        int n_2 = 20_000;

        System.out.println("=== Лёгкая функция ===");
        benchmark(light, 0, 2, n_1);

        System.out.println("\n=== Тяжёлая функция (CPU-bound) ===");
        benchmark(heavy, 0, 2, n_2);
    }

    private static void benchmark(myFunc function, double a, double b, int n) throws InterruptedException {
        long t0 = System.nanoTime();
        double resultSeq = integrate(function, a, b, n);
        long t1 = System.nanoTime();
        double timeSeq = (t1 - t0) / 1e6;

        int threads = Runtime.getRuntime().availableProcessors();

        long t2 = System.nanoTime();
        double resultPar = integrateParallel(function, a, b, n, threads);
        long t3 = System.nanoTime();
        double timePar = (t3 - t2) / 1e6;

        System.out.printf("Последовательно: result=%.6f, time=%.2f ms%n", resultSeq, timeSeq);
        System.out.printf("Параллельно (%d потоков): result=%.6f, time=%.2f ms%n", threads, resultPar, timePar);
        System.out.printf("Ускорение: %.2fx%n", timeSeq / timePar);
    }

    public static double integrate(myFunc function, double a, double b, int n) {
        double h = (b - a) / n;
        double sum = 0;
        for (int i = 0; i < n; i++) {
            double x = a + i * h;
            sum += function.calculate(x);
        }
        return sum * h;
    }

    public static double integrateParallel(myFunc function, double a, double b, int n, int numThreads) throws InterruptedException {
        double h = (b - a) / n;
        Thread[] threads = new Thread[numThreads];
        double[] partialSums = new double[numThreads];

        int chunk = n / numThreads;

        for (int t = 0; t < numThreads; t++) {
            final int start = t * chunk;
            final int end = (t == numThreads - 1) ? n : start + chunk;
            final int idx = t;

            threads[t] = new Thread(() -> {
                double localSum = 0;
                for (int i = start; i < end; i++) {
                    double x = a + i * h;
                    localSum += function.calculate(x);
                }
                partialSums[idx] = localSum;
            });
            threads[t].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        double totalSum = Arrays.stream(partialSums).sum();
        return totalSum * h;
    }
}