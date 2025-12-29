package example.runners;

import io.jiouring.file.Buffers;
import io.jiouring.file.IoUringFileIoHandle;
import io.jiouring.file.NativeConstants;
import io.netty.buffer.ByteBuf;
import io.netty.channel.IoEventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class TransactionalJournal implements Runnable {

    private static final Logger logger = LogManager.getLogger(TransactionalJournal.class);

    @Override
    public void run() {
        try {
            runAsync().join();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static CompletableFuture<Void> runAsync() throws IOException {
        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
        IoEventLoop loop = group.next();

        ExecutorService worker = Executors.newSingleThreadExecutor(
            new DefaultThreadFactory("journal-benchmark-worker")
        );

        logger.info("Starting journal engine...");
        Path path = Path.of("transactional_journal.dat");
        Files.deleteIfExists(path);

        CompletableFuture<Void> promise = new CompletableFuture<>();

        IoUringFileIoHandle
            .open(path, loop, READ, WRITE, CREATE_NEW)
            .whenComplete((file, err) -> {
                if (err != null) promise.completeExceptionally(err);
                else {
                    promise
                        .whenComplete((ignored1, t) -> file
                            .closeAsync()
                            .addListener((f) -> {
                                Throwable finalErr = t != null ? t : f.cause();
                                shutdown(group, worker, promise, finalErr);
                            }));
                    worker.submit(() -> runInternal(file, promise));
                }
            });

        promise.exceptionally(t -> {
            logger.error("Failed to create temp file", t);
            shutdown(group, worker, promise, t);
            return null;
        });

        return promise;
    }

    private static void shutdown(
        MultiThreadIoEventLoopGroup group,
        ExecutorService worker,
        CompletableFuture<Void> promise,
        Throwable err
    ) {
        worker.shutdown();
        group.shutdownGracefully().addListener(f -> {
            if (err != null) {
                promise.completeExceptionally(err);
            } else if (!f.isSuccess()) {
                promise.completeExceptionally(f.cause());
            } else {
                promise.complete(null);
            }
        });
    }

    private static CompletableFuture<Void> runInternal(IoUringFileIoHandle file, CompletableFuture<Void> promise) {
        long totalRecords = 100 * 1024 * 1024;
        long logBatch = 40 * 1024;
        int recordSize = 128;
        int recordsPerBatch = 65536;
        int batchSize = recordSize * recordsPerBatch;

        long totalBatches = totalRecords / recordsPerBatch;
        long actualTotalRecords = totalBatches * recordsPerBatch;
        int maxInFlightBatches = 256;

        AtomicLong completedBatches = new AtomicLong(0);
        Semaphore backpressure = new Semaphore(maxInFlightBatches);

        logger.info("Writing {} transactions in {} batches ({} bytes per write)...",
            actualTotalRecords, totalBatches, batchSize);

        long startTime = System.currentTimeMillis();

        try {
            for (long i = 0; i < totalBatches; i++) {
                backpressure.acquire();

                long fileOffset = i * batchSize;
                ByteBuf bb = Buffers.direct(batchSize);

                for (int r = 0; r < recordsPerBatch; r++) {
                    long recordId = (i * recordsPerBatch) + r;
                    bb.writeLong(recordId);
                    bb.writeDouble(99.99);
                    bb.writeLong(System.nanoTime());
                    bb.writeZero(recordSize - 24); // Padding
                }

                try {
                    file
                        .writeAsync(bb, fileOffset, false)
                        .addListener((f) -> {
                            bb.release();
                            backpressure.release();

                            // Sync the range and forget it
                            file.syncRangeAsync(fileOffset, batchSize, NativeConstants.SyncFileRangeFlags.SYNC_FILE_RANGE_WRITE_AND_WAIT);
                            file.fadviseAsync(fileOffset, batchSize, NativeConstants.FadviseAdvice.POSIX_FADV_DONTNEED);

                            if (!f.isSuccess()) promise.completeExceptionally(f.cause());
                            else {
                                long c = completedBatches.incrementAndGet();
                                if (c % logBatch == 0L) {
                                    long progress = c * recordsPerBatch;
                                    logger.info("Progress: {} / {} records", progress, actualTotalRecords);
                                }
                                if (c == totalBatches) promise.complete(null);
                            }
                        });
                } catch (Exception t) {
                    if (bb.refCnt() > 0) bb.release();
                    backpressure.release();
                    promise.completeExceptionally(t);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            promise.completeExceptionally(e);
        }

        return promise.thenAccept(res -> {
            long endTime = System.currentTimeMillis();
            long timeTaken = endTime - startTime;

            long bytesWritten = actualTotalRecords * recordSize;
            long mb = bytesWritten / (1024 * 1024);
            long throughput = timeTaken > 0 ? (mb * 1000) / timeTaken : 0;

            logger.info("--- Benchmark Results ---");
            logger.info("Status:      Success");
            logger.info("Time:        {} ms", timeTaken);
            logger.info("Total Size:  {} MB", mb);
            logger.info("Throughput:  {} MB/s", throughput);
            logger.info("-------------------------");
        })
        .thenAccept(ignored -> {});
    }
}