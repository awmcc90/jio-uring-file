package example.runners;

import io.jiouring.file.IoUringFile;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.IoEventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.uring.IoUringIoHandler;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.UUID;

public class BasicExample {
    public static void run() throws Exception {
        MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
        IoEventLoop ioEventLoop = group.next();

        Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"));
        String fileName = "uring-" + UUID.randomUUID() + ".dat";
        Path path = tempDir.resolve(fileName);

        IoUringFile f = IoUringFile.open(
            path,
            ioEventLoop,
            new StandardOpenOption[] {
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
            }
        ).join();

        byte[] data = "hello-io-uring".getBytes(StandardCharsets.UTF_8);
        ByteBuf writeBuf = Unpooled.directBuffer(data.length).writeBytes(data);
        ByteBuf readBuf = Unpooled.directBuffer(data.length);

        f.writeAsync(writeBuf, 0, true).join();
        f.readAsync(readBuf, 0).join();

        byte[] out = new byte[data.length];
        readBuf.readBytes(out);

        assert Arrays.equals(out, data) : "out does not match data";
        System.out.println("Written: " + new String(data, StandardCharsets.UTF_8));
        System.out.println("Read:    " + new String(out, StandardCharsets.UTF_8));

        readBuf.release();
        writeBuf.release();

        f.delete().join();

        group.shutdownGracefully().get();
    }
}