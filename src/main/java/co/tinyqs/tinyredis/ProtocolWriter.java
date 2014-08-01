package co.tinyqs.tinyredis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * redis protocol writer that supports a subset of printf-style string formats
 * 
 * Clients of this writer can plug in serializers that convert java objects to a binary format
 * 
 * @author bwarminski
 *
 */
public class ProtocolWriter
{
    private static final Charset CHARSET = ProtocolReader.CHARSET;
    private static final byte C_PERCENT = "%".getBytes(CHARSET)[0];
    private static final byte C_s = "s".getBytes(CHARSET)[0];
    private static final byte C_b = "b".getBytes(CHARSET)[0];
    private static final int PADDING = String.format("+%d\r\n", Integer.MAX_VALUE).getBytes(CHARSET).length;
    private static final byte[] CRLF = "\r\n".getBytes(CHARSET);
    
    private List<RedisSerializer> serializers;
    
    public ProtocolWriter()
    {
        serializers = new ArrayList<>();
    }
    
    private byte[] serialize(Object obj) throws IOException
    {
        for (RedisSerializer serializer : serializers)
        {
            if (serializer.canSerialize(obj))
            {
                return serializer.serialize(obj);
            }
        }
        
        throw new IOException("Unable to serialize object: " + Preconditions.firstNonNull(obj, "null").toString());
    }
    
    /**
     * Plug in a serializer for use in this writer. Serializers are called in the order that they are registered.
     */
    public void registerSerializer(RedisSerializer serializer)
    {
        Preconditions.checkNotNull(serializer, "Serializer may not be null");
        
        serializers.add(serializer);
    }
    
    /**
     * Convert a string-formatted command into a binary safe buffer for communication with a redis socket
     * 
     * Examples include:
     *   formatCommand("PING") - returns a simple "PING" message
     *   formatCommand("SET mykey %s", "hello world") - returns a _3_ element command "SET", "mykey", "hello world"
     *   formatCommand("SET mykey %b", myObject) - uses a registered serializer to convert myObject to a binary byte array
     *   
     * @throws IOException - If a given format can't be converted or a serializer is not available for a given object
     */
    public ByteBuffer formatCommand(String format, Object... args) throws IOException
    {
        Preconditions.checkNotNull(format);
        for (Object arg : args)
        {
            Preconditions.checkNotNull(arg);
        }
        
        int argidx = 0;
        ByteBuffer result = BufferUtils.EMPTY;
        
        String[] chunks = format.split("[ ]+");
        byte[] preamble = String.format("*%d\r\n", chunks.length).getBytes(CHARSET);
        result = BufferUtils.makeRoom(result, preamble.length).put(preamble);
        
        ByteBuffer temp = ByteBuffer.allocate(PADDING);
        temp.position(PADDING);
        for (String chunk : chunks)
        {
            
            byte[] chunkBuff = chunk.getBytes(CHARSET);
            temp = BufferUtils.makeRoom(temp, PADDING + 2 + chunkBuff.length);
            for (int i = 0; i < chunkBuff.length; i++)
            {
                byte c = chunkBuff[i];
                if (c == C_PERCENT && i+1 < chunkBuff.length)
                {
                    byte c2 = chunkBuff[i+1];
                    if (c2 == C_s)
                    {
                        Preconditions.checkState(argidx < args.length, "Not enough parameters given");
                        byte[] arg = args[argidx].toString().getBytes(CHARSET);
                        temp = BufferUtils.makeRoom(temp, arg.length);
                        temp.put(arg);
                        argidx++;
                    }                
                    else if (c2 == C_b)
                    {
                        Preconditions.checkState(argidx < args.length, "Not enough parameters given");
                        Object arg = args[argidx];
                        byte[] argBuff = serialize(arg);
                        temp = BufferUtils.makeRoom(temp, argBuff.length);
                        temp.put(argBuff);
                        argidx++;
                    }
                    else
                    {
                        temp = BufferUtils.makeRoom(temp, 2);
                        temp.put(c);
                        temp.put(c2);
                    }
                    i++;
                }
                else
                {
                    temp = BufferUtils.makeRoom(temp, 1);
                    temp.put(c);
                }
            }
            temp = BufferUtils.makeRoom(temp, 2);
            byte[] tempPreamble = String.format("$%d\r\n", temp.position() - PADDING).getBytes(CHARSET);
            temp.put(CRLF).flip().position(PADDING - tempPreamble.length).mark();
            temp.put(tempPreamble).reset();
            result = BufferUtils.makeRoom(result, temp.remaining()).put(temp);
            temp.clear().position(PADDING);
        }

        result.flip();
        return result;
    }
}
