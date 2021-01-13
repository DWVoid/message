@file:Suppress("BlockingMethodInNonBlockingContext")

package cn.newinfinideas.neworld.message

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.min

interface DataLinkPerformanceListener {
    fun registerTx(length: Int)
    fun registerRx(length: Int)
    fun registerBrokenRx(length: Int)
}

interface DataChannel {
    fun send(message: Message)
    suspend fun receive(): Message
}

class DataLink(private val performance: DataLinkPerformanceListener) {
    private val mgTxCh = Channel<Message>(16, BufferOverflow.SUSPEND)
    private val mgRxCh = Channel<Message>(16, BufferOverflow.SUSPEND)
    private val dgTxCh = Channel<ByteBuffer>(16, BufferOverflow.SUSPEND)
    private val dgRxCh = Channel<ByteBuffer>(16, BufferOverflow.SUSPEND)
    private val dgSTxSg = Semaphore(0)
    private val dgSRxSg = Semaphore(0)
    private val dgSelector = Selector.open()
    private val closed = AtomicBoolean()

    @ExperimentalCoroutinesApi
    private suspend fun run(address: InetSocketAddress) {
        val scope = CoroutineScope(Dispatchers.IO)
        val channel = DatagramChannel.open()
        select(channel.connect(address))
        scope.launch { tx(channel) }.apply { rx(channel) }.join()
    }

    @ExperimentalCoroutinesApi
    private suspend fun tx(socket: DatagramChannel) {
        while (!dgTxCh.isClosedForReceive && socket.isOpen) {
            try {
                val buffer = dgTxCh.receive()
                val start = buffer.position()
                while (socket.send(buffer, null) == 0) dgSTxSg.acquire()
                performance.registerTx(buffer.position() - start)
            } catch (e: Throwable) {
            }
        }
    }

    @ExperimentalCoroutinesApi
    private suspend fun rx(socket: DatagramChannel) {
        while (!dgRxCh.isClosedForSend && socket.isOpen) {
            try {
                val buffer = getBuffer()
                while (true) {
                    val address = socket.receive(buffer)
                    if (address != null) break else dgSRxSg.acquire()
                }
                performance.registerRx(buffer.position())
                dgRxCh.send(buffer.flip())
            } catch (e: Throwable) {
            }
        }
    }

    private fun select(socket: DatagramChannel) {
        socket.register(dgSelector, SelectionKey.OP_READ or SelectionKey.OP_WRITE)
        dgSelector.select {
            when {
                it.isReadable -> dgSRxSg.release()
                it.isWritable -> dgSTxSg.release()
            }
        }
    }

    private fun getBuffer() = ByteBuffer.allocate(1400) // Allocate for Ethernet MTU

    @ExperimentalCoroutinesApi
    private suspend fun slicer() {
        var buffer = getBuffer()
        while (!mgTxCh.isClosedForReceive) {
            var message = mgTxCh.poll()
            if (message == null) {
                if (buffer.position() > 0) {
                    dgTxCh.send(buffer)
                    buffer = getBuffer()
                }
                message = mgRxCh.receive()
            }
            if (slicerNeedNewBuffer(buffer, message)) buffer = getBuffer()
            val space = buffer.remaining()
            val size = message.data.remaining()
            if (space - size >= 20) {
                buffer.putShort(size.toShort()).putShort(message.app).putInt(message.mode).putInt(sequence)
                buffer.put(message.data).putInt(0)
                ackRegister(message, sequence++)
            }
            else {
                for (msg in slice(message, space)) {
                    buffer.putShort(msg.data.remaining().toShort()).putShort(msg.app)
                    buffer.put(msg.data).putInt(msg.mode).putInt(sequence)
                    ackRegister(msg, sequence++)
                    dgTxCh.send(buffer)
                    buffer = getBuffer()
                }
            }
        }
    }

    private fun slice(message: Message, firstSpace: Int): List<Message> {
        val result = ArrayList<Message>()
        result.add(Message(
            ID_FRAG.toShort(), sequence,
            ByteBuffer.allocate(firstSpace - 20)
                .putShort(message.data.remaining().toShort()).putShort(message.app)
                .putInt(message.mode)
                .put(message.data.slice().limit(firstSpace - 28))
        ))
        return result
    }

    private fun ackRegister(msg: Message, seq: Int) {}

    private var sequence = 0

    private fun slicerNeedNewBuffer(buffer: ByteBuffer, msg: Message): Boolean {
        val space = buffer.remaining()
        val size = msg.data.remaining()
        // it cannot transmit at a short message or at least 64 bytes of payload
        return space - min(size, 64) < 20
    }

    class Channel {

    }

    companion object {
        // constants
        private const val ID_CONN = 0
        private const val ID_INFO = 1
        private const val ID_ACK = 2
        private const val ID_SEC = 3
        private const val ID_FRAG = 4
    }
}