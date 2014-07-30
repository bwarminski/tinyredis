package co.tinyqs.tinyredis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Stateful redis protocol reader.
 * 
 * Buffers any received input via calls to feed() and returns replies via calls to getReply()
 * @author bwarminski
 *
 */
public class ProtocolReader
{
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final byte C_MINUS = "-".getBytes(CHARSET)[0];
    private static final byte C_PLUS = "+".getBytes(CHARSET)[0];
    private static final byte C_COLON = ":".getBytes(CHARSET)[0];
    private static final byte C_DOLLAR = "$".getBytes(CHARSET)[0];
    private static final byte C_STAR = "*".getBytes(CHARSET)[0];
    private static final byte C_CR = "\r".getBytes(CHARSET)[0];
    private static final byte C_LF = "\n".getBytes(CHARSET)[0];
    private static final byte C_0 = "0".getBytes(CHARSET)[0];
    private static final int MAX_INPUT_BUFF = 1024*16;
    
    private boolean errorState = false;
    private ByteBuffer buffer = BufferUtils.EMPTY;
    private Deque<ReadTask> stack = new ArrayDeque<>();
    private RedisReply reply = null;
    
    private enum State
    {
        READ_TYPE,
        READ_LEN,
        READ_NIL_ARRAY,
        READ_NIL_STRING,
        READ_INLINE,
        READ_INTEGER,
        READ_BULK,
        READ_ARRAY
    }
    
    private class ReadTask
    {
        State state = State.READ_TYPE;
        RedisReply.Type type = null;
        RedisReply[] elements = null;
        int len = -2;
        int idx = 0;
    }
    
    /**
     * Append zero or more bytes to the reader's internal buffer
     */
    public void feed(ByteBuffer input)
    {
        Preconditions.checkNotNull(input);
        Preconditions.checkState(!errorState, "Unable to feed data in error state");

        if (buffer.position() == 0 && buffer.capacity() > MAX_INPUT_BUFF)
        {
            buffer = BufferUtils.EMPTY;
        }
        buffer = BufferUtils.makeRoom(buffer, input.remaining());
        buffer.put(input);
    }
    
    /**
     * Attempts to read a reply off the reader's internal buffer, returning the reply
     * or null if not enough data is available.
     *
     * @throws IOException - In the case of errors parsing/decoding replies. An IOException will render the
     * reader unusable for future calls.
     */
    public RedisReply getReply() throws IOException
    {
        Preconditions.checkState(!errorState, "Can't return replies in error state");
        buffer.flip();
        if (stack.isEmpty())
        {
            stack.push(new ReadTask());
        }
        
        try
        {
            while (_readBuffer())
            {
                if (reply != null)
                {
                    RedisReply result = reply;
                    reply = null;
                    return result;
                }
            }
        }
        catch (Exception e)
        {
            errorState = true;
            throw e;
        }
        finally
        {
            buffer.compact();
        }
        
        return null;
    }
    
    /*
     * true - read more from the buffer or return reply
     * false - not enough data in buffer to proceed
     */
    private boolean _readBuffer() throws IOException
    {
        ReadTask task = stack.peek();
        switch (task.state)
        {
            case READ_TYPE:
            {
                if (buffer.remaining() == 0)
                {
                    return false;
                }
                byte p = buffer.get();
                if (p == C_MINUS)
                {
                    task.type = RedisReply.Type.ERROR;
                    task.state = State.READ_INLINE;
                }                
                else if (p == C_PLUS)
                {
                    task.type = RedisReply.Type.STATUS;
                    task.state = State.READ_INLINE;
                }
                else if (p == C_COLON)
                {
                    task.type = RedisReply.Type.INTEGER;
                    task.state = State.READ_INTEGER;
                }
                else if (p == C_DOLLAR)
                {
                    task.type = RedisReply.Type.STRING;
                    task.state = State.READ_LEN;
                }
                else if (p == C_STAR)
                {
                    task.type = RedisReply.Type.ARRAY;
                    task.state = State.READ_LEN;
                }
                else
                {
                    throw new IOException("Protocol error, got " + p + " as reply type byte");
                }
                return true;
            }
            case READ_LEN:
            {
                int newline = BufferUtils.seekNewLine(buffer);
                if (newline >= 0)
                {
                    task.len = readInt(buffer);
                    buffer.position(newline);
                    if (task.type == RedisReply.Type.STRING)
                    {
                        if (task.len == -1)
                        {
                            task.state = State.READ_NIL_STRING;
                            return true;
                        }
                        if (task.len >= 0)
                        {
                            task.state = State.READ_BULK;                            
                            return true;
                        }
                        throw new IOException("Protocol error, got " + task.len + " as a string length");
                    }
                    if (task.type == RedisReply.Type.ARRAY)
                    {
                        if (task.len == -1)
                        {
                            task.state = State.READ_NIL_ARRAY;
                            return true;
                        }
                        if (task.len >= 0)
                        {
                            task.state = State.READ_ARRAY;
                            task.elements = new RedisReply[task.len];
                            task.idx = 0;
                            return true;
                        }
                        throw new IOException("Protocol error, got " + task.len + " as an array length");
                    }
                    throw new IOException("State of reader was READ_LEN but was not a string or array: " + Preconditions.firstNonNull(task.state, "null"));
                }
                else
                {
                    return false;
                }
            }
            case READ_NIL_ARRAY:
            case READ_NIL_STRING:
            {
                if (stack.isEmpty())
                {
                    throw new IOException("Empty stack when in NIL state");
                }
                
                stack.pop();
                if (stack.isEmpty())
                {
                    reply = RedisReply.createNil();
                }
                else
                {
                    ReadTask arrayTask = stack.peek();
                    assert arrayTask.type == RedisReply.Type.ARRAY;
                    arrayTask.elements[arrayTask.idx] = RedisReply.createNil();
                    arrayTask.idx++;    
                }
                return true;
            }                       
            case READ_INLINE:
            {
                if (stack.isEmpty())
                {
                    throw new IOException("Empty stack when in INLINE state");
                }
                
                int newline = BufferUtils.seekNewLine(buffer);
                if (newline >= 0)
                {
                    int len = newline - buffer.position() - 2;
                    byte[] msg = new byte[len];
                    buffer.get(msg);
                    buffer.position(newline);
                    stack.pop();
                    if (stack.isEmpty())
                    {
                        reply = RedisReply.createString(task.type, msg);
                    }
                    else
                    {
                        ReadTask arrayTask = stack.peek();
                        assert arrayTask.type == RedisReply.Type.ARRAY;
                        arrayTask.elements[arrayTask.idx] = RedisReply.createString(task.type, msg);
                        arrayTask.idx++;                        
                    }
                    return true;
                }
                else
                {
                    return false;
                }
            }
            case READ_INTEGER:
            {
                if (stack.isEmpty())
                {
                    throw new IOException("Empty stack when in INTEGER state");
                }
                
                int newline = BufferUtils.seekNewLine(buffer);
                if (newline >= 0)
                {
                    long result = readLong(buffer);
                    buffer.position(newline);
                    stack.pop();
                    if (stack.isEmpty())
                    {
                        reply = RedisReply.createInteger(result);
                    }
                    else
                    {
                        ReadTask arrayTask = stack.peek();
                        assert arrayTask.type == RedisReply.Type.ARRAY;
                        arrayTask.elements[arrayTask.idx] = RedisReply.createInteger(result);
                        arrayTask.idx++;                        
                    }
                    return true;
                }
                else
                {
                    return false;
                }
            }
            case READ_BULK:
            {
                if (stack.isEmpty())
                {
                    throw new IOException("Empty stack when in BULK state");
                }
                
                if (task.len + 2 > buffer.remaining())
                {
                    return false;
                }
                
                byte[] msg = new byte[task.len];
                buffer.get(msg);
                int newline = BufferUtils.seekNewLine(buffer);
                if (newline != buffer.position() + 2)
                {
                    throw new IOException("Expected CRLF at end of bulk string reply");
                }
                buffer.position(newline);
                stack.pop();
                if (stack.isEmpty())
                {
                    reply = RedisReply.createBulkString(msg);
                }
                else
                {
                    ReadTask arrayTask = stack.peek();
                    assert arrayTask.type == RedisReply.Type.ARRAY;
                    arrayTask.elements[arrayTask.idx] = RedisReply.createBulkString(msg);
                    arrayTask.idx++;
                }
                
                return true;
            }
            case READ_ARRAY:
            {
                if (stack.isEmpty())
                {
                    throw new IOException("Empty stack when in ARRAY state");
                }
                
                assert task.len >= 0 && task.idx >= 0;
                
                if (task.len == task.idx)
                {
                    stack.pop();
                    if (stack.isEmpty())
                    {
                        reply = RedisReply.createArray(task.elements);
                    }
                    else
                    {
                        ReadTask arrayTask = stack.peek();
                        assert arrayTask.type == RedisReply.Type.ARRAY;
                        arrayTask.elements[arrayTask.idx] = RedisReply.createArray(task.elements);
                        arrayTask.idx++;
                    }
                }
                else
                {
                    stack.push(new ReadTask());
                }
                
                return true;
            }
            default:
                throw new IllegalStateException("Got in an illegal state");
        }
    }
    
    private static int readInt(ByteBuffer buff)
    {
        int v = 0;
        int dec, multi = 1;
        byte c = buff.get();
        
        if (c == C_MINUS)
        {
            multi = -1;
            c = buff.get();
        }
        else if (c == C_PLUS)
        {
            multi = 1;
            c = buff.get();
        }
         
        while (c != C_CR)
        {
            dec = c - C_0; // Tricky
            Preconditions.checkState(dec >= 0 && dec < 10, "Read decimal from input not between 0 and 10");
            v *= 10;
            v += dec;
            c = buff.get();
        }
        buff.get(); // Consume LF
        return v*multi;
    }
    
    private static long readLong(ByteBuffer buff)
    {
        long v = 0;
        int dec, multi = 1;
        byte c = buff.get();
        
        if (c == C_MINUS)
        {
            multi = -1;
            c = buff.get();
        }
        else if (c == C_PLUS)
        {
            multi = 1;
            c = buff.get();
        }
         
        while (c != C_CR)
        {
            dec = c - C_0; // Tricky
            Preconditions.checkState(dec >= 0 && dec < 10, "Read decimal from input not between 0 and 10");
            v *= 10;
            v += dec;
            c = buff.get();
        }
        buff.get(); // Consume LF
        return v*multi;
    }
}
