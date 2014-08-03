package co.tinyqs.tinyredis;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Represents a blocking connection to a redis server
 * 
 * Commands may be sent using sendCommand() or pipelined using appendCommand()/getReply()
 * 
 * @author bwarminski
 *
 */
public class RedisConnection implements AutoCloseable
{
    private final SocketChannel channel;
    private final ProtocolReader reader;
    private final ProtocolWriter writer;
    private Deque<ByteBuffer> outputBuffs;
    private ByteBuffer input;
    private boolean errorState;
    private boolean exceptionOnError = false;
    
    /**
     * Open a connection to specified remote address.
     */
    public static RedisConnection connect(SocketAddress addr) throws IOException
    {
        return connect(addr, 0);
    }
    
    /**
     * Open a connection to a specified remote address, waiting a maximum of <strong>timeout</strong> ms
     */
    public static RedisConnection connect(SocketAddress addr, int timeout) throws IOException
    {
        Preconditions.checkNotNull(addr, "Address may not be null");
        SocketChannel channel = SocketChannel.open();
        Socket socket = channel.socket();
        socket.setReuseAddress(true);
        socket.setKeepAlive(true);
        socket.setTcpNoDelay(true);
        socket.setSoLinger(true, 0);
        socket.connect(addr, timeout);
        
        return new RedisConnection(channel, new ProtocolReader(), new ProtocolWriter());
    }
    
    private RedisConnection(SocketChannel channel, ProtocolReader reader, ProtocolWriter writer)
    {
        this.channel = channel;
        this.reader = reader;
        this.writer = writer;
        this.errorState = false;
        
        outputBuffs = new ArrayDeque<>();        
        input = ByteBuffer.allocate(1024*16);
    }   
    
    public RedisConnection registerSerializer(RedisSerializer serializer)
    {
        writer.registerSerializer(serializer);
        return this;
    }
    
    public RedisConnection exceptionOnError(boolean exceptionOnError)
    {
        this.exceptionOnError = exceptionOnError;
        return this;
    }
      
    /**
     * Send a command to the remote server and wait for a reply. %s and %b in the format string will
     * be interpolated with binary safe subsitutions
     *
     * @throws IOException - If an error occurs formatting, sending or receiving a command. The connection will
     * no longer be usable after this state.
     */
    public RedisReply sendCommand(String format, Object... args) throws IOException
    {
        Preconditions.checkState(!errorState, "Unable to send commands in an error state");
        try
        {
            ByteBuffer formatted = writer.formatCommand(format, args);
            appendCommand(formatted);        
            return blockForReply();
        }
        catch (RedisErrorException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            errorState = true;
            throw e;
        }
    }
    
    /**
     * Queues a command to be sent at the next call to getReply().
     */
    public void appendCommand(String format, Object... args) throws IOException
    {
        Preconditions.checkState(!errorState, "Unable to send commands in an error state");
        try
        {           
            ByteBuffer formatted = writer.formatCommand(format, args);
            appendCommand(formatted);
        }
        catch (Exception e)
        {
            errorState = true;
            throw e;
        }
    }
    
    private void appendCommand(ByteBuffer command)
    {
        outputBuffs.add(command);
    }
    
    private RedisReply blockForReply() throws IOException
    {
        if (!channel.isBlocking())
        {
            channel.configureBlocking(true);
        }
        return getReply();
    }
    
    protected boolean _isBlocking()
    {
        return channel.isBlocking();
    }
    
    protected int _write(ByteBuffer src) throws IOException
    {
        return channel.write(src);
    }
    
    protected int _read(ByteBuffer dest) throws IOException
    {
        return channel.read(dest);
    }
    
    /**
     * Sends all pending commands and blocks for a full reply to be returned 
     */
    public RedisReply getReply() throws IOException
    {
        RedisReply reply = null;
        Preconditions.checkState(!errorState, "Unable to retrieve commands in an error state");
        try
        {
            reply = reader.getReply();
            
            if (reply == null && _isBlocking())
            {
                boolean done = outputBuffs.isEmpty();
                while (!done)
                {
                    ByteBuffer out = outputBuffs.pop();
                    _write(out);
                    if (out.remaining() > 0)
                    {
                        outputBuffs.push(out);
                        done = true;
                    }
                    else
                    {
                        done = outputBuffs.isEmpty();
                    }
                }
                           
                while (reply == null)
                {
                    int nread = _read(input);
                    if (nread == -1)
                    {
                        throw new IOException("Input channel unexpectedly closed");
                    }
                    
                    Preconditions.checkState(nread > 0, "Blocking channel should read at least one byte before returning");
                    
                    input.flip();
                    reader.feed(input);
                    input.clear();
                    reply = reader.getReply();
                }
            }
        }
        catch (RedisErrorException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            errorState = true;
            throw e;
        }
        
        if (reply != null && this.exceptionOnError && reply.getType() == RedisReply.Type.ERROR)
        {
            throw new RedisErrorException(reply.getString());
        }
        return reply;
    }

    public void close() throws Exception
    {
        channel.close();
    }
}
