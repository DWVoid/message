package cn.newinfinideas.neworld.message

import java.nio.ByteBuffer

class Message(
    val app: Short,
    val mode: Int,
    val data: ByteBuffer
) {
    public var channelId: Int = 0
}