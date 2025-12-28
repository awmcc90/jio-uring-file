package example.runners;

import io.jiouring.file.IoUringFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.IoEventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionalJournal implements Runnable {

    private static final Logger logger = LogManager.getLogger(TransactionalJournal.class);

    @Override
    public void run() {
        runAsync().join();
    }

    private static CompletableFuture<Void> runAsync() {
        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
        IoEventLoop loop = group.next();

        ExecutorService worker = Executors.newSingleThreadExecutor(
            new DefaultThreadFactory("journal-benchmark-worker")
        );

        logger.info("Starting journal engine...");

        CompletableFuture<Void> promise = new CompletableFuture<>();

        IoUringFile
            .createTempFile(loop)
            .whenComplete((file, err) -> {
                if (err != null) promise.completeExceptionally(err);
                else {
                    worker.submit(() -> runInternal(file, promise));
                    promise
                        .whenComplete((ignored1, t) -> file
                            .closeAsync()
                            .whenComplete((ignored2, tt) -> {
                                Throwable finalErr = t != null ? t : tt;
                                shutdown(group, worker, promise, finalErr);
                            }));
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

    private static CompletableFuture<Void> runInternal(IoUringFile file, CompletableFuture<Void> promise) {
        long totalRecords = 100 * 1024 * 1024;
        long logBatch = 40 * 1024;
        int recordSize = 128;
        int recordsPerBatch = 128;
        int batchSize = recordSize * recordsPerBatch;

        long totalBatches = totalRecords / recordsPerBatch;
        long actualTotalRecords = totalBatches * recordsPerBatch;
        int maxInFlightBatches = 4096;

        AtomicLong completedBatches = new AtomicLong(0);
        Semaphore backpressure = new Semaphore(maxInFlightBatches);

        logger.info("Writing {} transactions in {} batches ({} bytes per write)...",
            actualTotalRecords, totalBatches, batchSize);

        long startTime = System.currentTimeMillis();

        try {
            for (long i = 0; i < totalBatches; i++) {
                backpressure.acquire();

                long fileOffset = i * batchSize;
                ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(batchSize);

                for (int r = 0; r < recordsPerBatch; r++) {
                    long recordId = (i * recordsPerBatch) + r;
                    buffer.writeLong(recordId);
                    buffer.writeDouble(99.99);
                    buffer.writeLong(System.nanoTime());
                    buffer.writeZero(recordSize - 24); // Padding
                }

                file
                    .writeAsync(buffer, fileOffset)
                    .handle((res, err) -> {
                        backpressure.release();

                        if (err != null) promise.completeExceptionally(err);
                        else {
                            long c = completedBatches.incrementAndGet();
                            if (c % logBatch == 0L) {
                                long progress = c * recordsPerBatch;
                                logger.info("Progress: {} / {} records", progress, actualTotalRecords);
                            }
                            if (c == totalBatches) promise.complete(null);
                        }
                        return null;
                    });

                buffer.release();
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
        .thenCompose(ignored -> file.fsync())
        .thenAccept(ignored -> {});
    }
}