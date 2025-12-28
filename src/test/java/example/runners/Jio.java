package example.runners;

import io.jiouring.file.Buffers;
import io.jiouring.file.IoUringFileIoHandle;
import io.netty.buffer.ByteBuf;
import io.netty.channel.IoEventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.util.concurrent.ScheduledFuture;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

public class Jio implements Runnable {

    private static final int BLOCK_SIZE = 4096;
    private static final long FILE_SIZE = (long) 1024 * 1024 * 1024; // 1GB
    private static final long MAX_BLOCKS = (FILE_SIZE / BLOCK_SIZE) - 1;
    private static final int IO_DEPTH = 64;
    private static final int NUM_JOBS = 4;
    private static final int RUNTIME_SECONDS = 30;

    @Override
    public void run() {
        try {
            runInternal();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void runInternal() throws Exception {
        System.out.println("Starting Java FIO Emulator...");
        System.out.printf("Jobs: %d, Depth: %d, Time: %ds, File: 1GB%n", NUM_JOBS, IO_DEPTH, RUNTIME_SECONDS);

        Path path = Paths.get("java_fio_test.dat");
        setupFile(path);

        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(NUM_JOBS, IoUringIoHandler.newFactory());

        LongAdder totalOps = new LongAdder();
        CountDownLatch latch = new CountDownLatch(NUM_JOBS);
        long endTime = System.currentTimeMillis() + (RUNTIME_SECONDS * 1000L);
        SplittableRandom random = new SplittableRandom(42);

        System.out.println("Ramping up...");

        for (int i = 0; i < NUM_JOBS; i++) {
            var loop = group.next();

            IoUringFileIoHandle.open(
                path,
                loop,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                com.sun.nio.file.ExtendedOpenOption.DIRECT
            ).whenComplete((h, err) -> {
                if (err == null) {
                    runJob(h, loop, endTime, totalOps, latch, random.split());
                } else {
                    err.printStackTrace();
                    latch.countDown();
                }
            });
        }

        latch.await();

        long ops = totalOps.sum();
        double iops = (double) ops / RUNTIME_SECONDS;
        double bw = (iops * BLOCK_SIZE) / (1024 * 1024); // MB/s

        System.out.println("------------------------------------------------");
        System.out.printf("Total IOPS: %.2f%n", iops);
        System.out.printf("Bandwidth:  %.2f MiB/s%n", bw);
        System.out.println("------------------------------------------------");

        group.shutdownGracefully().sync();
        Files.deleteIfExists(path);
    }

    private static void runJob(
        IoUringFileIoHandle handle,
        IoEventLoop context,
        long endTime,
        LongAdder totalOps,
        CountDownLatch latch,
        SplittableRandom random
    ) {
        ByteBuf[] pool = new ByteBuf[IO_DEPTH];
        for (int i = 0; i < IO_DEPTH; i++) {
            ByteBuf bb = pool[i] = Buffers.alignedDirect(BLOCK_SIZE, true);
            bb.writerIndex(BLOCK_SIZE);
        }

        long[] localOps = new long[1];

        class Loop {
            final int idx;

            Loop(int idx) {
                this.idx = idx;
            }

            void submit() {
                if (System.currentTimeMillis() >= endTime) {
                    pool[idx].release();
                    return;
                }

                long offset = Math.abs(random.nextLong() % MAX_BLOCKS) * BLOCK_SIZE;
                ByteBuf bb = pool[idx].retain();
                // readerIndex is never advanced
                // bb.readerIndex(0);

                handle.writeAsync(bb, offset, false)
                    .addListener((f) -> {
                        bb.release();
                        if (f.isSuccess()) {
                            localOps[0]++;
                            submit();
                        } else {
                            f.cause().printStackTrace();
                        }
                    });
            }
        }

        for (int i = 0; i < IO_DEPTH; i++) {
            new Loop(i).submit();
        }

        final ScheduledFuture[] shutdownFutures = new ScheduledFuture[1];
        shutdownFutures[0] = context.scheduleAtFixedRate(() -> {
            if (System.currentTimeMillis() > endTime) {
                handle.closeAsync();
                totalOps.add(localOps[0]);
                latch.countDown();
                if (shutdownFutures[0] != null) shutdownFutures[0].cancel(false);
            }
        }, 1, 1, java.util.concurrent.TimeUnit.SECONDS);
    }

    private static void setupFile(Path path) throws Exception {
        System.out.println("Pre-allocating 1GB file...");
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            ByteBuffer zeros = ByteBuffer.allocateDirect(1024 * 1024); // 1MB chunks
            for (int i = 0; i < 1024; i++) {
                zeros.clear();
                ch.write(zeros);
            }
            ch.force(true);
        }
    }
}
