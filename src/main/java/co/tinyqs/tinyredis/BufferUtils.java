package co.tinyqs.tinyredis;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Static utilities for manipulating buffers
 * 
 * @author bwarminski
 *
 */
public class BufferUtils
{
    
    private static final Charset CHARSET = ProtocolReader.CHARSET;
    private static final byte C_CR = "\r".getBytes(CHARSET)[0];
    private static final byte C_LF = "\n".getBytes(CHARSET)[0];
    public static final ByteBuffer EMPTY = ByteBuffer.allocate(0);
    public static final int MAX_PREALLOC = (1024*1024);
    
    /**
     * Ensure that a given buffer contains at least the specified additional capacity in bytes
     * or reallocate and return a new expanded buffer with the correct capacity.
     * 
     * Precondition: the given buffer is no flipped. 
     * All bytes before buffer position will be written to the new buffer and the position set to the old
     * buffer's position.
     */
    public static ByteBuffer makeRoom(ByteBuffer buffer, int additionalCapacity)
    {
        // pre: buffer is not flipped (ie writable)
        buffer = Preconditions.firstNonNull(buffer, EMPTY);
 
        if (buffer.remaining() >= additionalCapacity)
        {
            return buffer;
        }
        
        Preconditions.checkState(buffer.limit() == buffer.capacity(), "Buffers passed to makeRoom should be in writable state");
        
        int newlen = buffer.capacity() + additionalCapacity - buffer.remaining();

        // Double the length up to the maximum prealloc size
        if (newlen < MAX_PREALLOC)
        {
            newlen *= 2;
        }
        else
        {
            newlen += MAX_PREALLOC;
        }
        
        ByteBuffer newBuff = ByteBuffer.allocate(newlen);
        buffer.flip();
        newBuff.put(buffer);
        
        return newBuff;
    }
    
    /**
     * Given a string, encode the string into a buffer suitable for network communication
     */
    public static ByteBuffer encode(String message)
    {
        Preconditions.checkNotNull(message);
        return ByteBuffer.wrap(message.getBytes(CHARSET)); 
    }
    
    /**
     * Given a byte array from the network, decode the bytes into a String
     */
    public static String decode(byte[] buffer)
    {
        Preconditions.checkNotNull(buffer);
        return decode(ByteBuffer.wrap(buffer));
    }
    
    /**
     * Given a bytebuffer, decode the bytes into a String
     */
    public static String decode(ByteBuffer buffer)
    {
        Preconditions.checkNotNull(buffer);
        int pos = buffer.position();
        String result = CHARSET.decode(buffer).toString();
        buffer.position(pos);
        return result;
    }
    
    /**
     * Return the position after a full CRLF in this buffer, or -1 if CRLF is not present
     */
    public static int seekNewLine(ByteBuffer buffer)
    {
        int pos = buffer.position();

        while (buffer.remaining() > 1)
        {
            byte c = buffer.get();
            while (c != C_CR && buffer.remaining() > 1)
            {
                c = buffer.get();
            }
            if (c != C_CR)
            {
                buffer.position(pos);
                return -1;
            }
            else
            {
                c = buffer.get();
                if (c == C_LF)
                {
                    int result = buffer.position();
                    buffer.position(pos);
                    return result;
                }
            }
        }
        
        return -1;
    }
}
