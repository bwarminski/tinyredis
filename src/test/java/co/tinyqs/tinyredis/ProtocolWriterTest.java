package co.tinyqs.tinyredis;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.AfterClass;
import org.junit.Test;

/**
 * @author bwarminski
 *
 */
public class ProtocolWriterTest
{

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception
    {
    }

    /**
     * Test method for {@link co.tinyqs.tinyredis.ProtocolWriter#formatCommand(java.lang.String, java.lang.Object[])}.
     */
    @Test
    public void testFormatCommand() throws IOException
    {
        ProtocolWriter writer = new ProtocolWriter();
        ByteBuffer result = null;
        // Given a non formatted string, string is correct
        
        result = writer.formatCommand("OK");
        assertNotNull(result);
        assertEquals(BufferUtils.decode(result), "*1\r\n$2\r\nOK\r\n");
        
        // Given a formatted %s string, string is correct
        
        result = writer.formatCommand("OK   %s", "test");
        assertNotNull(result);
        assertEquals(BufferUtils.decode(result), "*2\r\n$2\r\nOK\r\n$4\r\ntest\r\n");
        
        // Given a formatted %b string with a serializer, string is correct
        writer.registerSerializer(new RedisSerializer(){

            @Override
            public boolean canSerialize(Object obj)
            {
                return true;
            }

            @Override
            public byte[] serialize(Object obj) throws IOException
            {
                return "OK".getBytes(StandardCharsets.UTF_8);
            }            
        });
        
        Object aRandomObject = System.out;
        result = writer.formatCommand("%b     %s", aRandomObject, "test");
        assertNotNull(result);
        assertEquals(BufferUtils.decode(result), "*2\r\n$2\r\nOK\r\n$4\r\ntest\r\n");
        
        result = writer.formatCommand("SET %s %s","foo","");
        assertNotNull(result);
        assertEquals(BufferUtils.decode(result), "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$0\r\n\r\n");
        
        result = writer.formatCommand("SET % %");
        assertNotNull(result);
        assertEquals(BufferUtils.decode(result), "*3\r\n$3\r\nSET\r\n$1\r\n%\r\n$1\r\n%\r\n");
    }

}
