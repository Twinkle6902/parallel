package pr3;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MandelbrotExercise {

    private static final int WIDTH = 1200;
    private static final int HEIGHT = 800;

    private static final int MAX_ITERATIONS = 1000;

    private static final int THREADS = Runtime.getRuntime().availableProcessors();

    private static final double MIN_REAL = -2.5;
    private static final double MAX_REAL = 1.0;

    private static final double MIN_IMAGINARY = -1.2;
    private static final double MAX_IMAGINARY = 1.2;


    private static boolean isInside(double real, double imaginary) {

        double zReal = 0.0;
        double zImaginary = 0.0;

        for (int iteration = 0; iteration < MAX_ITERATIONS; iteration++) {

            if (zReal * zReal + zImaginary * zImaginary > 4.0) {
                return false;
            }

            double newReal = zReal * zReal - zImaginary * zImaginary + real;

            double newImaginary = 2.0 * zReal * zImaginary + imaginary;

            zReal = newReal;
            zImaginary = newImaginary;
        }

        return true;
    }


    private static double pixelToReal(int x) {

        return MIN_REAL + x * (MAX_REAL - MIN_REAL) / (WIDTH - 1);
    }


    private static double pixelToImaginary(int y) {

        return MIN_IMAGINARY + y * (MAX_IMAGINARY - MIN_IMAGINARY) / (HEIGHT - 1);
    }

    private static int calculateRows(int startRow, int endRow) {

        int insideCount = 0;

        for (int y = startRow; y < endRow; y++) {

            double imaginary = pixelToImaginary(y);

            for (int x = 0; x < WIDTH; x++) {

                double real = pixelToReal(x);

                if (isInside(real, imaginary)) {
                    insideCount++;
                }
            }
        }

        return insideCount;
    }


    public static void main(String[] args) throws Exception {

        System.out.println("Количество потоков: " + THREADS);

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);

        List<Future<Integer>> results = new ArrayList<>();

        int rowsPerThread = (HEIGHT + THREADS - 1) / THREADS;


        long startTime = System.nanoTime();


        for (int thread = 0; thread < THREADS; thread++) {

            int startRow = thread * rowsPerThread;

            int endRow = Math.min(
                            startRow + rowsPerThread,
                            HEIGHT
                    );

            if (startRow >= HEIGHT) {
                break;
            }


            Future<Integer> result = executor.submit(
                            () -> calculateRows(
                                    startRow,
                                    endRow
                            )
                    );

            results.add(result);
        }


        int totalInside = 0;

        for (Future<Integer> result : results) {
            totalInside += result.get();
        }


        executor.shutdown();


        long endTime = System.nanoTime();

        double seconds = (endTime - startTime)
                        / 1_000_000_000.0;


        System.out.println("Всего пикселей: " + (WIDTH * HEIGHT));

        System.out.println("Пикселей в множестве Мандельброта: " + totalInside);

        System.out.printf("Время расчёта: %.3f секунд%n", seconds);
    }
}
