package io.jiouring.file;

import com.sun.nio.file.ExtendedOpenOption;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;

import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

public final class OpenHelpers {

    private OpenHelpers() {}

    public static ByteBuf cStr(Path path) {
        String absPath = path.toAbsolutePath().toString();
        ByteBuf buf = Buffers.direct();
        ByteBufUtil.writeUtf8(buf, absPath);
        buf.writeByte(0);
        return buf;
    }

    public static int openFlags(OpenOption... options) {
        boolean read = false;
        boolean write = false;
        int flags = 0;

        for (OpenOption option : options) {
            if (option == StandardOpenOption.READ) {
                read = true;
            } else if (option == StandardOpenOption.WRITE) {
                write = true;
            } else if (option == StandardOpenOption.APPEND) {
                flags |= NativeConstants.OpenFlags.O_APPEND;
            } else if (option == StandardOpenOption.TRUNCATE_EXISTING) {
                flags |= NativeConstants.OpenFlags.O_TRUNC;
            } else if (option == StandardOpenOption.CREATE) {
                flags |= NativeConstants.OpenFlags.O_CREAT;
            } else if (option == StandardOpenOption.CREATE_NEW) {
                flags |= (NativeConstants.OpenFlags.O_CREAT | NativeConstants.OpenFlags.O_EXCL);
            } else if (option == StandardOpenOption.SYNC) {
                flags |= NativeConstants.OpenFlags.O_SYNC;
            } else if (option == StandardOpenOption.DSYNC) {
                flags |= NativeConstants.OpenFlags.O_DSYNC;
            } else if (option == ExtendedOpenOption.DIRECT) {
                flags |= NativeConstants.OpenFlags.O_DIRECT;
            } else if (option == LinkOption.NOFOLLOW_LINKS) {
                flags |= NativeConstants.OpenFlags.O_NOFOLLOW;
            } else {
                throw new UnsupportedOperationException(option + " not supported");
            }
        }

        int access;

        if (read && write) access = NativeConstants.OpenFlags.O_RDWR;
        else if (write) access = NativeConstants.OpenFlags.O_WRONLY;
        // Default to READ ONLY if nothing specified (standard POSIX/NIO behavior)
        else access = NativeConstants.OpenFlags.O_RDONLY;

        return flags | access;
    }

    public static int fileMode(FileAttribute<?>... attributes) {
        int mode = NativeConstants.FileMode.DEFAULT_FILE_PERMS;

        for (FileAttribute<?> attr : attributes) {
            if ("posix:permissions".equals(attr.name())) {
                @SuppressWarnings("unchecked")
                Set<PosixFilePermission> permissions = (Set<PosixFilePermission>) attr.value();

                int permMode = 0;
                for (PosixFilePermission p : permissions) {
                    switch (p) {
                        case OWNER_READ:     permMode |= NativeConstants.FileMode.S_IRUSR; break;
                        case OWNER_WRITE:    permMode |= NativeConstants.FileMode.S_IWUSR; break;
                        case OWNER_EXECUTE:  permMode |= NativeConstants.FileMode.S_IXUSR; break;

                        case GROUP_READ:     permMode |= NativeConstants.FileMode.S_IRGRP; break;
                        case GROUP_WRITE:    permMode |= NativeConstants.FileMode.S_IWGRP; break;
                        case GROUP_EXECUTE:  permMode |= NativeConstants.FileMode.S_IXGRP; break;

                        case OTHERS_READ:    permMode |= NativeConstants.FileMode.S_IROTH; break;
                        case OTHERS_WRITE:   permMode |= NativeConstants.FileMode.S_IWOTH; break;
                        case OTHERS_EXECUTE: permMode |= NativeConstants.FileMode.S_IXOTH; break;
                    }
                }
                mode = permMode;
            }
        }
        return mode;
    }
}