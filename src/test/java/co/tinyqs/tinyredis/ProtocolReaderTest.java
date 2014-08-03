/**
 * 
 */
package co.tinyqs.tinyredis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;

/**
 * @author bwarminski
 *
 */
public class ProtocolReaderTest
{
    @Test
    public void testSimpleString() throws IOException
    {
        ProtocolReader reader = new ProtocolReader();
        reader.feed(BufferUtils.encode("+OK\r\n"));
        RedisReply reply = reader.getReply();
        assertNotNull(reply);
        assertEquals(reply.getType(), RedisReply.Type.STATUS);
        assertEquals(BufferUtils.decode(reply.getBytes()), "OK");
    }

    @Test
    public void testErrorString() throws IOException
    {
        ProtocolReader reader = new ProtocolReader();
        reader.feed(BufferUtils.encode("-Error message\r\n"));
        RedisReply reply = reader.getReply();
        assertNotNull(reply);
        assertEquals(reply.getType(), RedisReply.Type.ERROR);
        assertEquals(BufferUtils.decode(reply.getBytes()), "Error message");
    }
    
    @Test
    public void testInteger() throws IOException
    {
        ProtocolReader reader = new ProtocolReader();
        reader.feed(BufferUtils.encode(":0\r\n"));
        reader.feed(BufferUtils.encode(":10000\r\n"));
        reader.feed(BufferUtils.encode(":-1000\r\n"));
        reader.feed(BufferUtils.encode(":+10000\r\n"));
        long bigNumber = (long) Integer.MAX_VALUE + 10l;
        reader.feed(BufferUtils.encode(":"+bigNumber+"\r\n"));
        
        for (long val : new long[] {0l, 10000l, -1000l, 10000l, bigNumber})
        {
            RedisReply reply = reader.getReply();
            assertNotNull(reply);
            assertEquals(reply.getType(), RedisReply.Type.INTEGER);
            assertEquals(reply.getInteger(), val);
        }
        
    }
    
    @Test
    public void testBulkString() throws IOException
    {
        ProtocolReader reader = new ProtocolReader();
        reader.feed(BufferUtils.encode("$6\r\nfoobar\r\n"));
        reader.feed(BufferUtils.encode("$0\r\n\r\n"));
        RedisReply reply = reader.getReply();
        assertNotNull(reply);
        assertEquals(reply.getType(), RedisReply.Type.STRING);
        assertEquals(BufferUtils.decode(reply.getBytes()), "foobar");
        
        reply = reader.getReply();
        assertNotNull(reply);
        assertEquals(reply.getType(), RedisReply.Type.STRING);
        assertEquals(BufferUtils.decode(reply.getBytes()), "");
    }
    
    @Test
    public void testNilString() throws IOException
    {
        ProtocolReader reader = new ProtocolReader();
        reader.feed(BufferUtils.encode("$-1\r\n"));

        RedisReply reply = reader.getReply();
        assertNotNull(reply);
        assertEquals(reply.getType(), RedisReply.Type.NIL);
    }
    
    @Test
    public void testArrays() throws IOException
    {
        ProtocolReader reader = new ProtocolReader();
        reader.feed(BufferUtils.encode("*0\r\n"));
        reader.feed(BufferUtils.encode("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"));
        reader.feed(BufferUtils.encode("*3\r\n:1\r\n:2\r\n:3\r\n"));

        RedisReply reply = reader.getReply();
        assertNotNull(reply);
        assertEquals(reply.getType(), RedisReply.Type.ARRAY);
        assertTrue(reply.getElements().length == 0);
        
        reply = reader.getReply();
        assertNotNull(reply);
        assertEquals(reply.getType(), RedisReply.Type.ARRAY);
        assertTrue(reply.getElements().length == 2);
        RedisReply[] elements = reply.getElements();
        int i = 0;
        for (String val : new String[]{"foo", "bar"})
        {
            RedisReply innerReply = elements[i];
            assertEquals(innerReply.getType(), RedisReply.Type.STRING);
            assertEquals(BufferUtils.decode(innerReply.getBytes()), val);
            i++;
        }
        
        reply = reader.getReply();
        assertNotNull(reply);
        assertEquals(reply.getType(), RedisReply.Type.ARRAY);
        assertTrue(reply.getElements().length == 3);
        elements = reply.getElements();
        for (int j = 1; j < 4; j++)
        {
            RedisReply innerReply = elements[j-1];
            assertEquals(innerReply.getType(), RedisReply.Type.INTEGER);
            assertEquals(j,innerReply.getInteger());
        }
        
        reader.feed(BufferUtils.encode("*5\r\n"));
        assertNull(reader.getReply());
        reader.feed(BufferUtils.encode(":1\r\n"));
        assertNull(reader.getReply());
        reader.feed(BufferUtils.encode(":2\r\n"));
        assertNull(reader.getReply());
        reader.feed(BufferUtils.encode(":3\r\n"));
        assertNull(reader.getReply());
        reader.feed(BufferUtils.encode(":4\r\n"));
        assertNull(reader.getReply());
        reader.feed(BufferUtils.encode("$6\r\n"));
        assertNull(reader.getReply());
        reader.feed(BufferUtils.encode("foobar\r\n"));
        reply = reader.getReply();
        assertNotNull(reply);
        assertEquals(reply.getType(), RedisReply.Type.ARRAY);
        assertTrue(reply.getElements().length == 5);
        elements = reply.getElements();
        for (int j = 1; j < 4; j++)
        {
            RedisReply innerReply = elements[j-1];
            assertEquals(innerReply.getType(), RedisReply.Type.INTEGER);
            assertEquals(j,innerReply.getInteger());
        }
        assertEquals(elements[4].getType(), RedisReply.Type.STRING);
        assertEquals(BufferUtils.decode(elements[4].getBytes()), "foobar");
        
        reader.feed(BufferUtils.encode("*-1\r\n"));
        reply = reader.getReply();
        assertNotNull(reply);
        assertEquals(reply.getType(), RedisReply.Type.NIL);
        
        reader.feed(BufferUtils.encode("*2\r\n"));
        assertNull(reader.getReply());
        reader.feed(BufferUtils.encode("*3\r\n"));
        assertNull(reader.getReply());
        reader.feed(BufferUtils.encode(":1\r\n"));
        assertNull(reader.getReply());
        reader.feed(BufferUtils.encode(":2\r\n"));
        assertNull(reader.getReply());
        reader.feed(BufferUtils.encode(":3\r\n"));
        assertNull(reader.getReply());
        reader.feed(BufferUtils.encode("*2\r\n"));
        assertNull(reader.getReply());
        reader.feed(BufferUtils.encode("+Foo\r\n"));
        assertNull(reader.getReply());
        reader.feed(BufferUtils.encode("-Bar\r\n"));
        
        reply = reader.getReply();
        assertNotNull(reply);
        assertEquals(reply.getType(), RedisReply.Type.ARRAY);
        
        RedisReply nestedReply = reply.getElements()[0];
        assertNotNull(nestedReply);
        assertEquals(nestedReply.getType(), RedisReply.Type.ARRAY);
        elements = nestedReply.getElements();
        for (int j = 1; j < 4; j++)
        {
            RedisReply innerReply = elements[j-1];
            assertEquals(innerReply.getType(), RedisReply.Type.INTEGER);
            assertEquals(j,innerReply.getInteger());
        }
        
        nestedReply = reply.getElements()[1];
        assertNotNull(nestedReply);
        assertEquals(nestedReply.getType(), RedisReply.Type.ARRAY);
        elements = nestedReply.getElements();
        assertEquals(elements[0].getType(), RedisReply.Type.STATUS);
        assertEquals(elements[1].getType(), RedisReply.Type.ERROR);
        
        
        
        reader.feed(BufferUtils.encode("*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n"));
        reply = reader.getReply();
        assertNotNull(reply);
        assertEquals(reply.getType(), RedisReply.Type.ARRAY);
        assertEquals(reply.getElements()[1].getType(), RedisReply.Type.NIL);
        
        
    }
    
    @Test
    public void testErrorHandling() throws IOException
    {
        ProtocolReader reader = new ProtocolReader();
        try
        {           
            reader.feed(BufferUtils.encode("@foo\r\n"));
            RedisReply reply = reader.getReply();
            fail("Should have been an error");
        }
        catch (IOException e)
        {
            
        }
        
        try
        {
            reader.feed(BufferUtils.encode("+OK\r"));
            reader.getReply();
            fail("Shouldn't be able to read in error state");
        }
        catch (IllegalStateException e)
        {
            
        }
        
    }
    
    @Test
    public void testSeparatedNewlines() throws IOException
    {
        ProtocolReader reader = new ProtocolReader();
        reader.feed(BufferUtils.encode("+OK\r"));
        assertNull(reader.getReply());
        reader.feed(BufferUtils.encode("\n"));
        RedisReply reply = reader.getReply();
        assertNotNull(reply);
        assertEquals(BufferUtils.decode(reply.getBytes()), "OK");
    }
}
