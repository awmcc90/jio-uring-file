package io.jiouring.file;

import io.netty.buffer.ByteBuf;

public class FileStats {
    public final long size;
    public final int mode;
    public final int nlink;
    public final int uid;
    public final int gid;

    public FileStats(ByteBuf buffer) {
        this.nlink = buffer.getInt(0x10);
        this.uid = buffer.getInt(0x14);
        this.gid = buffer.getInt(0x18);
        this.mode = buffer.getShort(0x1C) & 0xFFFF;
        this.size = buffer.getLong(0x40);
    }
}
