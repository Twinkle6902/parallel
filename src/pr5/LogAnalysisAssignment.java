package pr5;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/*
 * ПРАКТИЧЕСКОЕ ЗАДАНИЕ: МНОГОПОТОЧНЫЙ АНАЛИЗ ЛОГОВ ВЕБ-СЕРВЕРА
 *
 * Крупные части задания:
 * 1. Сгенерировать набор настоящих файлов с логами.
 * 2. Реализовать разбор строк и последовательный отчёт.
 * 3. Распараллелить обработку нескольких файлов через ExecutorService.
 * 4. Сделать общую статистику потокобезопасной.
 * 5. Ограничить число одновременно читаемых файлов через Semaphore.
 * 6. Добавить Atomic-счётчики, CAS и корректное ожидание завершения.
 * 7. Реализовать досрочную остановку анализа через volatile.
 * 8. Сформулировать и доказать инварианты корректности и отсутствия дедлоков.
 * 9. Реализовать обработку повреждённых файлов и частичный отчёт.
 *
 * ФАЙЛ МОЖНО ЗАПУСТИТЬ САМОСТОЯТЕЛЬНО:
 *   java LogAnalysisAssignment generate logs 20 1000
 *   java LogAnalysisAssignment check
 *
 * Первая команда создаёт каталог logs и файлы access-000.log, access-001.log и т.д.
 * Аргументы: каталог, количество файлов, количество строк в каждом файле.
 * Вторая команда выполняет встроенную проверку генератора и итератора строк.
 *
 * ФОРМАТ СТРОКИ:
 *   METHOD PATH STATUS RESPONSE_BYTES DURATION_MILLIS
 * Например:
 *   GET /api/users 200 820 4
 *
 * Поля разделены одним или несколькими пробелами.
 * Успешный запрос имеет статус от 200 до 399 включительно.
 * Ошибочный запрос имеет статус от 400 и выше.
 *
 * ГОТОВЫЕ СТРУКТУРЫ:
 *   LogEntry       — одна разобранная запись.
 *   LogFile        — путь к одному файлу.
 *   PathStatistics — неизменяемая статистика одного URL.
 *   Report         — итоговый отчёт.
 *   LogFileLines   — итератор строк файла.
 *   GenerateLogs   — генератор входных файлов.
 *
 * ВАЖНО ПРО LogFileLines:
 * Итератор потоконебезопасен. Один экземпляр Iterator<String> нельзя читать
 * из нескольких потоков одновременно. Каждый поток должен получить собственный
 * итератор для своего файла. Нельзя разделить один итератор между задачами.
 * Сам файл можно читать параллельно разными потоками только при условии, что
 * каждый поток создаёт отдельный итератор.
 *
 * ЧАСТИ ЗАДАНИЯ:
 *
 * 1. РАЗБОР СТРОКИ
 * Реализуйте преобразование одной строки в LogEntry. Проверьте количество
 * полей, преобразуйте числовые значения и выбрасывайте понятную ошибку для
 * повреждённой строки.
 *
 * 2. ПОСЛЕДОВАТЕЛЬНЫЙ ОТЧЁТ
 * Напишите обработку всех файлов одним потоком. Для каждого URL собирайте
 * количество запросов, количество успешных запросов и ошибок, объём ответов,
 * суммарное и максимальное время обработки. Этот вариант станет эталоном для
 * проверки многопоточной версии.
 *
 * 3. ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ФАЙЛОВ
 * Создайте фиксированный ExecutorService. Каждый файл обрабатывается отдельной
 * задачей. Рабочий поток создаёт собственный LogFileLines.Iterator, читает свой
 * файл и передаёт записи в общий аккумулятор. Главный поток должен дождаться
 * завершения всех файлов и только потом построить Report.
 *
 * 4. ПОТОКОБЕЗОПАСНАЯ СТАТИСТИКА
 * Реализуйте аккумулятор статистики. Сначала допустимо использовать один общий
 * synchronized-метод. Затем реализуйте вариант с отдельным synchronized-аккумулятором
 * для каждого URL и ConcurrentHashMap. Сравните эти варианты на данных, где почти
 * все запросы относятся к одному URL, и на данных с большим числом URL.
 *
 * 5. ОГРАНИЧЕНИЕ ЧТЕНИЯ ФАЙЛОВ
 * Добавьте Semaphore, ограничивающий число файлов, которые могут обрабатываться
 * одновременно. Рабочих потоков может быть больше разрешений семафора. Разрешение
 * нужно получить перед чтением файла и гарантированно вернуть после завершения.
 * Добавьте наблюдение за текущим и максимальным числом активных файлов.
 *
 * 6. ATOMIC-СЧЁТЧИКИ И CAS
 * Добавьте счётчики обработанных файлов, прочитанных строк, корректных записей,
 * повреждённых записей, ошибок чтения и активных файлов. Для максимального числа
 * активных файлов используйте AtomicInteger и compareAndSet. Обычные изменяемые
 * int/long для этих глобальных счётчиков не используйте.
 *
 * 7. ОЖИДАНИЕ ЗАВЕРШЕНИЯ
 * Организуйте ожидание всех файлов через CountDownLatch либо через набор Future.
 * Завершение задачи должно учитываться даже при исключении. ExecutorService нужно
 * корректно закрыть и дождаться его завершения.
 *
 * CountDownLatch — это одноразовый счётчик-сигнал. Его создают с начальным
 * числом, например равным количеству файлов. Главный поток вызывает await()
 * и блокируется, пока счётчик не станет равен нулю. Каждая завершившаяся задача
 * один раз вызывает countDown(), уменьшая счётчик. Когда последний счётчик
 * уменьшен до нуля, все ожидающие потоки продолжают работу. Увеличить latch
 * обратно или использовать его повторно нельзя, поэтому для нового запуска
 * создаётся новый объект. Вызов countDown() нужно помещать в finally, чтобы
 * исключение в задаче не оставило главный поток ждать бесконечно.
 *
 * 8. ДОСРОЧНАЯ ОСТАНОВКА
 * Добавьте volatile-флаг остановки. После остановки новые строки не начинают
 * обрабатываться, а уже начатая операция может завершиться. Опишите, какие данные
 * входят в частичный отчёт. Не оставляйте рабочие потоки и разрешения семафора
 * в подвешенном состоянии.
 *
 * 9. ПОВРЕЖДЁННЫЕ ФАЙЛЫ И ЧАСТИЧНЫЙ ОТЧЁТ
 * Сделайте так, чтобы ошибка одного файла не отменяла обработку остальных файлов.
 * В итоговом результате отдельно храните список или число файлов, которые не удалось
 * обработать. Решите, что происходит при повреждённой строке: прекращается обработка
 * всего файла или пропускается только эта строка. Выберите один вариант и явно
 * зафиксируйте его в документации. Для полного запуска корректные файлы должны
 * давать полный отчёт, а ошибка должна быть видна пользователю.
 *
 * ИНВАРИАНТЫ, КОТОРЫЕ НУЖНО ДОКАЗАТЬ:
 * - каждая строка полного запуска обработана ровно один раз;
 * - для каждого URL requests = successful + errors;
 * - maxDuration <= totalDuration при наличии запросов;
 * - 0 <= activeFiles <= semaphoreLimit;
 * - после завершения анализа activeFiles равно нулю;
 * - один повреждённый файл не мешает обработке остальных;
 * - в программе нет дедлока.
 *
 * В доказательстве отсутствия дедлока укажите порядок захвата мониторов и
 * семафоров. Отдельно объясните, может ли поток ждать acquire(), удерживая
 * synchronized-монитор, и почему разрешение семафора возвращается при исключении.
 * Объясните, почему volatile нужен для флага остановки.
 *
 * НЕБОЛЬШИЕ ЭКСПЕРИМЕНТЫ ПОСЛЕ РЕАЛИЗАЦИИ:
 * - сравнить 1 и 4 рабочих потока;
 * - сравнить лимит семафора 1 и 4;
 * - сравнить глобальную блокировку и блокировки по URL;
 * - добавить одну повреждённую строку или один повреждённый файл и проверить,
 *   что остальные данные всё равно попадают в отчёт.
 * Эксперименты не требуют дополнительной архитектуры или новых классов.
 */
public class LogAnalysisAssignment {

    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis
    ) {}

    public record LogFile(Path path) {}

    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {}

    public record Report(
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long maxDurationMillis
    ) {}


    public record AnalysisResult(
            Report report,
            Map<String, PathStatistics> paths,
            int filesTotal,
            long filesProcessed,
            long filesFailed,
            List<String> fileErrors,
            long linesRead,
            long validEntries,
            long invalidLines,
            long readErrors,
            int maxActiveFiles,
            int activeFiles,
            int semaphoreLimit,
            boolean stoppedEarly,
            long elapsedMillis
    ) {}

    public enum StatisticsMode {
        GLOBAL,
        PER_URL
    }

    public interface StatisticsAccumulator {
        void add(LogEntry entry);

        Map<String, PathStatistics> snapshot();
    }

    private static final class PathTotals {
        private long requests;
        private long successful;
        private long errors;
        private long responseBytes;
        private long totalDurationMillis;
        private long maxDurationMillis;

        private void add(LogEntry entry) {
            requests++;

            if (entry.status() >= 200 && entry.status() <= 399) {
                successful++;
            } else {
                errors++;
            }

            responseBytes += entry.responseBytes();
            totalDurationMillis += entry.durationMillis();

            if (entry.durationMillis() > maxDurationMillis) {
                maxDurationMillis = entry.durationMillis();
            }
        }

        private PathStatistics snapshot() {
            return new PathStatistics(
                    requests,
                    successful,
                    errors,
                    responseBytes,
                    totalDurationMillis,
                    maxDurationMillis
            );
        }
    }

    public static final class GlobalStatistics implements StatisticsAccumulator {
        private final Map<String, PathTotals> byPath = new HashMap<>();

        @Override
        public synchronized void add(LogEntry entry) {
            PathTotals totals = byPath.get(entry.path());
            if (totals == null) {
                totals = new PathTotals();
                byPath.put(entry.path(), totals);
            }
            totals.add(entry);
        }

        @Override
        public synchronized Map<String, PathStatistics> snapshot() {
            Map<String, PathStatistics> result = new TreeMap<>();
            for (Map.Entry<String, PathTotals> entry : byPath.entrySet()) {
                result.put(entry.getKey(), entry.getValue().snapshot());
            }
            return Collections.unmodifiableMap(result);
        }
    }

    public static final class PerUrlStatistics implements StatisticsAccumulator {
        private final ConcurrentHashMap<String, PathTotals> byPath =
                new ConcurrentHashMap<>();

        @Override
        public void add(LogEntry entry) {
            PathTotals totals = byPath.get(entry.path());

            if (totals == null) {
                PathTotals newTotals = new PathTotals();
                PathTotals oldTotals = byPath.putIfAbsent(entry.path(), newTotals);
                totals = oldTotals == null ? newTotals : oldTotals;
            }

            synchronized (totals) {
                totals.add(entry);
            }
        }

        @Override
        public Map<String, PathStatistics> snapshot() {
            Map<String, PathStatistics> result = new TreeMap<>();

            for (Map.Entry<String, PathTotals> entry : byPath.entrySet()) {
                PathTotals totals = entry.getValue();
                synchronized (totals) {
                    result.put(entry.getKey(), totals.snapshot());
                }
            }

            return Collections.unmodifiableMap(result);
        }
    }


    public static final class StopSignal {
        private volatile boolean stopped;

        public void stop() {
            stopped = true;
        }

        public boolean isStopped() {
            return stopped;
        }
    }

    public static final class AnalysisCounters {
        private final AtomicLong filesProcessed = new AtomicLong();
        private final AtomicLong filesFailed = new AtomicLong();
        private final AtomicLong linesRead = new AtomicLong();
        private final AtomicLong validEntries = new AtomicLong();
        private final AtomicLong invalidLines = new AtomicLong();
        private final AtomicLong readErrors = new AtomicLong();
        private final AtomicInteger activeFiles = new AtomicInteger();
        private final AtomicInteger maxActiveFiles = new AtomicInteger();

        public void fileStarted() {
            int current = activeFiles.incrementAndGet();

            while (true) {
                int oldMaximum = maxActiveFiles.get();
                if (current <= oldMaximum) {
                    return;
                }
                if (maxActiveFiles.compareAndSet(oldMaximum, current)) {
                    return;
                }
            }
        }

        public void fileFinished() {
            activeFiles.decrementAndGet();
        }

        public void fileProcessed() {
            filesProcessed.incrementAndGet();
        }

        public void fileFailed() {
            filesFailed.incrementAndGet();
        }

        public void lineRead() {
            linesRead.incrementAndGet();
        }

        public void validEntry() {
            validEntries.incrementAndGet();
        }

        public void invalidLine() {
            invalidLines.incrementAndGet();
        }

        public void readError() {
            readErrors.incrementAndGet();
        }

        public long filesProcessed() {
            return filesProcessed.get();
        }

        public long filesFailed() {
            return filesFailed.get();
        }

        public long linesRead() {
            return linesRead.get();
        }

        public long validEntries() {
            return validEntries.get();
        }

        public long invalidLines() {
            return invalidLines.get();
        }

        public long readErrors() {
            return readErrors.get();
        }

        public int activeFiles() {
            return activeFiles.get();
        }

        public int maxActiveFiles() {
            return maxActiveFiles.get();
        }
    }

    public static LogEntry parseLine(String line) {
        if (line == null) {
            throw new IllegalArgumentException("Line is null");
        }

        String[] fields = line.trim().split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException(
                    "Expected 5 fields, got " + fields.length + ": " + line
            );
        }

        try {
            int status = Integer.parseInt(fields[2]);
            long responseBytes = Long.parseLong(fields[3]);
            long durationMillis = Long.parseLong(fields[4]);

            if (status < 100 || status > 599) {
                throw new IllegalArgumentException(
                        "Status must be from 100 to 599: " + line
                );
            }
            if (responseBytes < 0) {
                throw new IllegalArgumentException(
                        "Response bytes must be non-negative: " + line
                );
            }
            if (durationMillis < 0) {
                throw new IllegalArgumentException(
                        "Duration must be non-negative: " + line
                );
            }

            return new LogEntry(
                    fields[0],
                    fields[1],
                    status,
                    responseBytes,
                    durationMillis
            );
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "Invalid number in line: " + line,
                    exception
            );
        }
    }

    public static Report buildReport(Map<String, PathStatistics> paths) {
        long totalRequests = 0;
        long successfulRequests = 0;
        long errorRequests = 0;
        long responseBytes = 0;
        long maxDurationMillis = 0;

        for (PathStatistics statistics : paths.values()) {
            totalRequests += statistics.requests();
            successfulRequests += statistics.successful();
            errorRequests += statistics.errors();
            responseBytes += statistics.responseBytes();

            if (statistics.maxDurationMillis() > maxDurationMillis) {
                maxDurationMillis = statistics.maxDurationMillis();
            }
        }

        return new Report(
                totalRequests,
                successfulRequests,
                errorRequests,
                responseBytes,
                maxDurationMillis
        );
    }

    private static StatisticsAccumulator createAccumulator(StatisticsMode mode) {
        if (mode == StatisticsMode.GLOBAL) {
            return new GlobalStatistics();
        }
        return new PerUrlStatistics();
    }

    private static boolean processFile(
            Path file,
            StatisticsAccumulator statistics,
            AnalysisCounters counters,
            StopSignal stopSignal,
            List<String> fileErrors
    ) {
        try {
            Iterator<String> iterator = LogFileLines.iterator(file);

            while (iterator.hasNext()) {
                if (stopSignal.isStopped()) {
                    break;
                }

                String line = iterator.next();
                counters.lineRead();

                try {
                    LogEntry entry = parseLine(line);
                    statistics.add(entry);
                    counters.validEntry();
                } catch (IllegalArgumentException exception) {
                    counters.invalidLine();
                }
            }

            return true;
        } catch (RuntimeException exception) {
            counters.readError();
            synchronized (fileErrors) {
                fileErrors.add(file + ": " + exception.getMessage());
            }
            return false;
        }
    }

    private static AnalysisResult makeResult(
            List<Path> files,
            StatisticsAccumulator statistics,
            AnalysisCounters counters,
            List<String> fileErrors,
            int semaphoreLimit,
            boolean stoppedEarly,
            long startNanos
    ) {
        Map<String, PathStatistics> paths = statistics.snapshot();
        Report report = buildReport(paths);

        List<String> errorsCopy;
        synchronized (fileErrors) {
            errorsCopy = new ArrayList<>(fileErrors);
        }

        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;

        return new AnalysisResult(
                report,
                paths,
                files.size(),
                counters.filesProcessed(),
                counters.filesFailed(),
                Collections.unmodifiableList(errorsCopy),
                counters.linesRead(),
                counters.validEntries(),
                counters.invalidLines(),
                counters.readErrors(),
                counters.maxActiveFiles(),
                counters.activeFiles(),
                semaphoreLimit,
                stoppedEarly,
                elapsedMillis
        );
    }

    public static AnalysisResult analyzeSequential(List<Path> files) {
        StatisticsAccumulator statistics = new PerUrlStatistics();
        AnalysisCounters counters = new AnalysisCounters();
        List<String> fileErrors = new ArrayList<>();
        StopSignal stopSignal = new StopSignal();
        long startNanos = System.nanoTime();

        for (Path file : files) {
            counters.fileStarted();
            boolean fileReadCompletely = processFile(
                    file,
                    statistics,
                    counters,
                    stopSignal,
                    fileErrors
            );
            counters.fileFinished();
            counters.fileProcessed();

            if (!fileReadCompletely) {
                counters.fileFailed();
            }
        }

        return makeResult(
                files,
                statistics,
                counters,
                fileErrors,
                1,
                false,
                startNanos
        );
    }

    public static AnalysisResult analyzeParallel(
            List<Path> files,
            int threadCount,
            int semaphoreLimit,
            StatisticsMode mode,
            StopSignal stopSignal
    ) throws InterruptedException {
        if (threadCount < 1) {
            throw new IllegalArgumentException("threadCount must be >= 1");
        }
        if (semaphoreLimit < 1) {
            throw new IllegalArgumentException("semaphoreLimit must be >= 1");
        }
        if (stopSignal == null) {
            throw new IllegalArgumentException("stopSignal must not be null");
        }

        List<Path> filesCopy = new ArrayList<>(files);
        StatisticsAccumulator statistics = createAccumulator(mode);
        AnalysisCounters counters = new AnalysisCounters();
        List<String> fileErrors = Collections.synchronizedList(new ArrayList<>());

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        Semaphore semaphore = new Semaphore(semaphoreLimit);
        CountDownLatch latch = new CountDownLatch(filesCopy.size());
        long startNanos = System.nanoTime();

        try {
            for (Path file : filesCopy) {
                executor.execute(() -> {
                    boolean hasPermit = false;
                    boolean fileStarted = false;

                    try {
                        semaphore.acquire();
                        hasPermit = true;

                        if (stopSignal.isStopped()) {
                            return;
                        }

                        fileStarted = true;
                        counters.fileStarted();

                        boolean fileReadCompletely = processFile(
                                file,
                                statistics,
                                counters,
                                stopSignal,
                                fileErrors
                        );

                        if (!fileReadCompletely) {
                            counters.fileFailed();
                        }
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    } finally {
                        if (fileStarted) {
                            counters.fileFinished();
                            counters.fileProcessed();
                        }
                        if (hasPermit) {
                            semaphore.release();
                        }
                        latch.countDown();
                    }
                });
            }

            latch.await();
        } catch (InterruptedException exception) {
            stopSignal.stop();
            executor.shutdownNow();
            throw exception;
        } finally {
            executor.shutdown();
        }

        if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
            executor.shutdownNow();
        }

        return makeResult(
                filesCopy,
                statistics,
                counters,
                fileErrors,
                semaphoreLimit,
                stopSignal.isStopped(),
                startNanos
        );
    }

    public static void checkInvariants(AnalysisResult result) {
        Report report = result.report();

        if (report.successfulRequests() + report.errorRequests()
                != report.totalRequests()) {
            throw new AssertionError("Global requests invariant is broken");
        }

        if (!result.stoppedEarly()
                && result.filesProcessed() != result.filesTotal()) {
            throw new AssertionError(
                    "Not all files were processed without early stop"
            );
        }

        if (!result.stoppedEarly()
                && report.totalRequests() != result.validEntries()) {
            throw new AssertionError(
                    "Every valid line must be counted exactly once"
            );
        }

        if (result.activeFiles() != 0) {
            throw new AssertionError(
                    "activeFiles must be 0 after analysis, got "
                            + result.activeFiles()
            );
        }

        if (result.maxActiveFiles() < 0
                || result.maxActiveFiles() > result.semaphoreLimit()) {
            throw new AssertionError(
                    "maxActiveFiles must be between 0 and semaphoreLimit"
            );
        }

        long totalDurationForAllPaths = 0;

        for (Map.Entry<String, PathStatistics> entry : result.paths().entrySet()) {
            PathStatistics statistics = entry.getValue();

            if (statistics.requests()
                    != statistics.successful() + statistics.errors()) {
                throw new AssertionError(
                        "Requests invariant is broken for " + entry.getKey()
                );
            }

            if (statistics.requests() > 0
                    && statistics.maxDurationMillis()
                    > statistics.totalDurationMillis()) {
                throw new AssertionError(
                        "maxDuration > totalDuration for " + entry.getKey()
                );
            }

            totalDurationForAllPaths += statistics.totalDurationMillis();
        }

        if (report.totalRequests() > 0
                && report.maxDurationMillis() > totalDurationForAllPaths) {
            throw new AssertionError(
                    "Global maxDuration > sum of totalDuration"
            );
        }
    }

    public static List<Path> listLogFiles(Path directory) throws IOException {
        List<Path> result = new ArrayList<>();

        try (Stream<Path> paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                if (Files.isRegularFile(path)
                        && path.getFileName().toString().endsWith(".log")) {
                    result.add(path);
                }
            }
        }

        Collections.sort(result);
        return result;
    }

    public static void printResult(String title, AnalysisResult result) {
        Report report = result.report();

        System.out.println();
        System.out.println("=== " + title + " ===");
        System.out.printf(
                "Files: total=%d processed=%d failed=%d active=%d maxActive=%d limit=%d%n",
                result.filesTotal(),
                result.filesProcessed(),
                result.filesFailed(),
                result.activeFiles(),
                result.maxActiveFiles(),
                result.semaphoreLimit()
        );
        System.out.printf(
                "Lines: read=%d valid=%d invalid=%d readErrors=%d%n",
                result.linesRead(),
                result.validEntries(),
                result.invalidLines(),
                result.readErrors()
        );
        System.out.printf(
                "Requests: total=%d successful=%d errors=%d bytes=%d maxDuration=%d ms%n",
                report.totalRequests(),
                report.successfulRequests(),
                report.errorRequests(),
                report.responseBytes(),
                report.maxDurationMillis()
        );
        System.out.printf(
                "Elapsed: %d ms, stoppedEarly=%s%n",
                result.elapsedMillis(),
                result.stoppedEarly()
        );

        System.out.println("Per path:");

        for (Map.Entry<String, PathStatistics> entry : result.paths().entrySet()) {
            PathStatistics statistics = entry.getValue();
            System.out.printf(
                    "  %-18s requests=%d successful=%d errors=%d bytes=%d totalDuration=%d maxDuration=%d%n",
                    entry.getKey(),
                    statistics.requests(),
                    statistics.successful(),
                    statistics.errors(),
                    statistics.responseBytes(),
                    statistics.totalDurationMillis(),
                    statistics.maxDurationMillis()
            );
        }

        if (!result.fileErrors().isEmpty()) {
            System.out.println("File errors:");
            for (String error : result.fileErrors()) {
                System.out.println("  " + error);
            }
        }
    }


    public static final class LogFileLines {
        private LogFileLines() {}

        public static Iterator<String> iterator(Path path) {
            try {
                BufferedReader reader = Files.newBufferedReader(
                        path,
                        StandardCharsets.UTF_8
                );

                return new Iterator<>() {
                    private String nextLine = readNext();
                    private boolean closed;

                    private String readNext() {
                        try {
                            String line = reader.readLine();
                            if (line == null) {
                                close();
                            }
                            return line;
                        } catch (IOException exception) {
                            close();
                            throw new RuntimeException(
                                    "Cannot read " + path,
                                    exception
                            );
                        }
                    }

                    private void close() {
                        if (closed) {
                            return;
                        }
                        closed = true;
                        try {
                            reader.close();
                        } catch (IOException exception) {
                            throw new RuntimeException(
                                    "Cannot close " + path,
                                    exception
                            );
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return nextLine != null;
                    }

                    @Override
                    public String next() {
                        if (nextLine == null) {
                            throw new NoSuchElementException();
                        }
                        String result = nextLine;
                        nextLine = readNext();
                        return result;
                    }
                };
            } catch (IOException exception) {
                throw new RuntimeException(
                        "Cannot open " + path,
                        exception
                );
            }
        }
    }

    /** Генерирует реальные текстовые файлы с детерминированными данными. */
    public static final class GenerateLogs {
        private static final String[] METHODS = {
                "GET", "POST", "PUT", "DELETE"
        };
        private static final String[] PATHS = {
                "/", "/index.html", "/login", "/api/users",
                "/api/orders", "/api/products", "/search", "/health"
        };
        private static final int[] STATUSES = {
                200, 200, 200, 201, 204,
                301, 400, 401, 404, 500
        };

        private GenerateLogs() {}

        public static void generate(
                Path outputDirectory,
                int fileCount,
                int linesPerFile
        ) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) {
                throw new IllegalArgumentException(
                        "Counts must be non-negative"
                );
            }

            Files.createDirectories(outputDirectory);
            Random random = new Random(42);

            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = outputDirectory.resolve(
                        "access-%03d.log".formatted(fileId)
                );

                try (BufferedWriter writer = Files.newBufferedWriter(
                        file,
                        StandardCharsets.UTF_8
                )) {
                    for (int line = 0; line < linesPerFile; line++) {
                        String method =
                                METHODS[random.nextInt(METHODS.length)];
                        String path =
                                PATHS[random.nextInt(PATHS.length)];
                        int status =
                                STATUSES[random.nextInt(STATUSES.length)];
                        long responseBytes =
                                100L + random.nextInt(100_000);
                        long durationMillis =
                                1L + random.nextInt(500);

                        writer.write(
                                "%s %s %d %d %d%n".formatted(
                                        method,
                                        path,
                                        status,
                                        responseBytes,
                                        durationMillis
                                )
                        );
                    }
                }
            }
        }
    }

    public static void runExperiments(Path directory)
            throws IOException, InterruptedException {
        List<Path> files = listLogFiles(directory);

        if (files.isEmpty()) {
            System.out.println("No .log files in " + directory);
            return;
        }

        System.out.println("Files: " + files.size());

        AnalysisResult sequential = analyzeSequential(files);
        checkInvariants(sequential);
        System.out.printf(
                "sequential: %d ms, requests=%d%n",
                sequential.elapsedMillis(),
                sequential.report().totalRequests()
        );

        int[] threadCounts = {1, 4};
        int[] semaphoreLimits = {1, 4};

        for (int threadCount : threadCounts) {
            for (int semaphoreLimit : semaphoreLimits) {
                AnalysisResult parallel = analyzeParallel(
                        files,
                        threadCount,
                        semaphoreLimit,
                        StatisticsMode.PER_URL,
                        new StopSignal()
                );

                checkInvariants(parallel);

                if (!sequential.report().equals(parallel.report())) {
                    throw new AssertionError(
                            "Parallel report differs from sequential report"
                    );
                }

                System.out.printf(
                        "threads=%d semaphore=%d: %d ms, maxActive=%d, reportMatches=true%n",
                        threadCount,
                        semaphoreLimit,
                        parallel.elapsedMillis(),
                        parallel.maxActiveFiles()
                );
            }
        }

        System.out.println("Statistics variants:");

        for (StatisticsMode mode : StatisticsMode.values()) {
            AnalysisResult parallel = analyzeParallel(
                    files,
                    4,
                    4,
                    mode,
                    new StopSignal()
            );

            checkInvariants(parallel);

            if (!sequential.report().equals(parallel.report())) {
                throw new AssertionError(
                        "Statistics mode " + mode + " gives a different report"
                );
            }

            System.out.printf(
                    "mode=%s: %d ms, reportMatches=true%n",
                    mode,
                    parallel.elapsedMillis()
            );
        }
    }

    private static void runDamagedDemo()
            throws IOException, InterruptedException {
        Path directory = Files.createTempDirectory("log-assignment-demo-");
        GenerateLogs.generate(directory, 3, 100);

        Path damagedFile = directory.resolve("access-000.log");
        Files.writeString(
                damagedFile,
                "THIS IS A DAMAGED LINE\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND
        );

        List<Path> files = listLogFiles(directory);
        files.add(directory.resolve("missing.log"));

        AnalysisResult sequential = analyzeSequential(files);
        AnalysisResult parallel = analyzeParallel(
                files,
                4,
                2,
                StatisticsMode.PER_URL,
                new StopSignal()
        );

        checkInvariants(sequential);
        checkInvariants(parallel);

        printResult("Damaged input: sequential", sequential);
        printResult("Damaged input: parallel", parallel);

        System.out.println(
                "Reports match: "
                        + sequential.report().equals(parallel.report())
        );
        System.out.println("Demo directory: " + directory);
    }

    private static void runStopDemo(Path directory, long delayMillis)
            throws IOException, InterruptedException {
        List<Path> files = listLogFiles(directory);
        StopSignal stopSignal = new StopSignal();

        Thread stopper = new Thread(() -> {
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return;
            }
            stopSignal.stop();
        });

        stopper.start();

        AnalysisResult result = analyzeParallel(
                files,
                4,
                2,
                StatisticsMode.PER_URL,
                stopSignal
        );

        stopper.join();
        checkInvariants(result);
        printResult("Early stop after " + delayMillis + " ms", result);
    }

    /** Проверяет генератор и чтение строк без участия студенческого анализатора. */
    private static void selfCheck() throws IOException {
        Path directory = Files.createTempDirectory("log-assignment-");
        GenerateLogs.generate(directory, 3, 7);

        int files = 0;
        int lines = 0;

        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                files++;
                Iterator<String> iterator = LogFileLines.iterator(path);

                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length != 5) {
                        throw new AssertionError(
                                "Unexpected line: " + line
                        );
                    }
                    Integer.parseInt(fields[2]);
                    Long.parseLong(fields[3]);
                    Long.parseLong(fields[4]);
                    lines++;
                }
            }
        }

        if (files != 3 || lines != 21) {
            throw new AssertionError(
                    "Expected 3 files and 21 lines, got "
                            + files + " files and " + lines + " lines"
            );
        }

        System.out.println(
                "OK: generated " + files
                        + " files and read " + lines + " lines"
        );
    }

    private static StatisticsMode parseMode(String value) {
        if (value.equalsIgnoreCase("global")) {
            return StatisticsMode.GLOBAL;
        }
        if (value.equalsIgnoreCase("perurl")
                || value.equalsIgnoreCase("per_url")) {
            return StatisticsMode.PER_URL;
        }
        throw new IllegalArgumentException(
                "Unknown mode: " + value + " (use global or perurl)"
        );
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("generate")) {
            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );
            int fileCount = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 20;
            int linesPerFile = args.length > 3
                    ? Integer.parseInt(args[3])
                    : 1_000;

            GenerateLogs.generate(
                    directory,
                    fileCount,
                    linesPerFile
            );

            System.out.println(
                    "Generated " + fileCount
                            + " files in " + directory
            );
            return;
        }

        if (args.length > 0 && args[0].equals("check")) {
            selfCheck();
            return;
        }

        if (args.length > 0 && args[0].equals("analyze")) {
            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );
            int threadCount = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 4;
            int semaphoreLimit = args.length > 3
                    ? Integer.parseInt(args[3])
                    : 2;
            StatisticsMode mode = args.length > 4
                    ? parseMode(args[4])
                    : StatisticsMode.PER_URL;

            List<Path> files = listLogFiles(directory);
            AnalysisResult sequential = analyzeSequential(files);
            AnalysisResult parallel = analyzeParallel(
                    files,
                    threadCount,
                    semaphoreLimit,
                    mode,
                    new StopSignal()
            );

            checkInvariants(sequential);
            checkInvariants(parallel);
            printResult("Sequential", sequential);
            printResult("Parallel", parallel);

            if (!sequential.report().equals(parallel.report())) {
                throw new AssertionError(
                        "Parallel report differs from sequential report"
                );
            }

            System.out.println("Reports match.");
            return;
        }

        if (args.length > 0 && args[0].equals("experiments")) {
            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );
            runExperiments(directory);
            return;
        }

        if (args.length > 0 && args[0].equals("demo")) {
            runDamagedDemo();
            return;
        }

        if (args.length > 0 && args[0].equals("stop")) {
            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );
            long delayMillis = args.length > 2
                    ? Long.parseLong(args[2])
                    : 5;
            runStopDemo(directory, delayMillis);
            return;
        }

        System.out.println(
                "Usage: java LogAnalysisAssignment "
                        + "generate <directory> <files> <lines>"
        );
        System.out.println(
                "       java LogAnalysisAssignment check"
        );
        System.out.println(
                "       java LogAnalysisAssignment analyze <directory> "
                        + "[threads] [semaphoreLimit] [global|perurl]"
        );
        System.out.println(
                "       java LogAnalysisAssignment experiments <directory>"
        );
        System.out.println(
                "       java LogAnalysisAssignment demo"
        );
        System.out.println(
                "       java LogAnalysisAssignment stop <directory> "
                        + "[delayMillis]"
        );
    }
}
