package co.tinyqs.tinyredis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static co.tinyqs.tinyredis.Preconditions.checkState;

/**
 * Barebones integration test based entirely on hiredis's test.c
 *
 */
public class IntegrationTests
{

    public static void main(String[] args) throws Exception
    {
        checkState(args.length == 2, "Expecting host and port as args");
        SocketAddress addr = new InetSocketAddress(args[0], Integer.parseInt(args[1]));
        try (RedisConnection conn = RedisConnection.connect(addr))
        {
            System.out.println("Connected");
            
            conn.registerSerializer(new RedisSerializer(){

                @Override
                public boolean canSerialize(Object obj)
                {
                    return true;
                }

                @Override
                public byte[] serialize(Object obj) throws IOException
                {
                    String toSerialize = Preconditions.firstNonNull(obj, "null").toString();
                    return toSerialize.getBytes(StandardCharsets.UTF_16);
                }});
            
            System.out.println("Testing sending a command");
            RedisReply reply = conn.sendCommand("PING");
            checkState(reply.getType() == RedisReply.Type.STATUS, "Expecting status reply");
            checkState("pong".equalsIgnoreCase(reply.getString()), "Expected pong as a reply");
            
            System.out.println("Testing sending verbatim command");
            reply = conn.sendCommand("SET foo bar");
            checkState(reply.getType() == RedisReply.Type.STATUS, "Expecting status reply");
            checkState("ok".equalsIgnoreCase(reply.getString()), "Expected ok as a reply");
            
            System.out.println("Testing string interpolation");
            conn.sendCommand("SET %s %s","foo","hello world");
            reply = conn.sendCommand("GET foo");
            checkState(reply.getType() == RedisReply.Type.STRING, "Expecting string reply");
            checkState("hello world".equals(reply.getString()), "Expected hello world as a reply");
            
            System.out.println("Testing binary serialization");
            byte[] helloWorldBytes = "hello world".getBytes(StandardCharsets.UTF_16);
            conn.sendCommand("SET %s %b", "foo", "hello world");
            reply = conn.sendCommand("GET foo");
            checkState(reply.getType() == RedisReply.Type.STRING, "Expecting string reply");
            checkState(Arrays.equals(helloWorldBytes, reply.getBytes()), "Expecting equal byte length returned");
            
            System.out.println("Testing nil reply");
            reply = conn.sendCommand("GET nokey");
            checkState(reply.getType() == RedisReply.Type.NIL, "Expecting nil reply");
            
            System.out.println("Testing integer replies");
            conn.sendCommand("DEL mycounter");
            reply = conn.sendCommand("INCR mycounter");
            checkState(reply.getType() == RedisReply.Type.INTEGER, "Expecting integer reply");
            checkState(reply.getInteger() == 1, "Expecting count to be 1");
            
            System.out.println("Testing multi bulk replies");
            conn.sendCommand("DEL mylist");
            conn.sendCommand("LPUSH mylist foo");
            conn.sendCommand("LPUSH mylist bar");
            reply = conn.sendCommand("LRANGE mylist 0 -1");
            checkState(reply.getType() == RedisReply.Type.ARRAY, "Expecting array type");
            checkState(reply.getElements().length == 2, "Expecting two elements in the result");
            checkState("bar".equals(BufferUtils.decode(reply.getElements()[0].getBytes())), "Expecting bar as the first result");
            checkState("foo".equals(BufferUtils.decode(reply.getElements()[1].getBytes())), "Expecting foo as the second result");
            
            System.out.println("Testing nested multi bulk replies");
            conn.sendCommand("MULTI");
            conn.sendCommand("LRANGE mylist 0 -1");
            conn.sendCommand("PING");
            reply = conn.sendCommand("EXEC");
            
            checkState(reply.getType() == RedisReply.Type.ARRAY, "Expecting array type");
            checkState(reply.getElements().length == 2, "Expecting 2 elements");
            checkState(reply.getElements()[0].getType() == RedisReply.Type.ARRAY, "Expecting nested array type");
            checkState(reply.getElements()[0].getElements().length == 2, "Expecting 2 nested elements");
            checkState("bar".equals(BufferUtils.decode(reply.getElements()[0].getElements()[0].getBytes())), "Expecting bar as the first nested element");
            checkState("foo".equals(BufferUtils.decode(reply.getElements()[0].getElements()[1].getBytes())), "Expecting foo as the second nested element");
            checkState(reply.getElements()[1].getType() == RedisReply.Type.STATUS, "Expecting nested status type");
            checkState("pong".equalsIgnoreCase(BufferUtils.decode(reply.getElements()[1].getBytes())), "Expected pong");
            
            System.out.println("Testing exception on error");
            try
            {
                conn.exceptionOnError(true);
                reply = conn.sendCommand("GARBAGE");
                checkState(false, "Invalid command should have thrown an error");
            }
            catch (IOException e)
            {
                // Test passed
            }
            
            System.out.println("\r\n--Testing throughput --\r\n");
            
            for (int x = 0; x < 5; x++)
            {
                for (int i = 0; i < 500; i++)
                {
                    conn.sendCommand("LPUSH mylist foo");
                }
                RedisReply[] replies = new RedisReply[1000];
                long t1 = System.currentTimeMillis();
                for (int i = 0; i < 1000; i++)
                {
                    replies[i] = conn.sendCommand("PING");
                    checkState(replies[i].getType() == RedisReply.Type.STATUS, "Expecting status on all replies");
                }
                long t2 = System.currentTimeMillis();
                System.out.println("1000 PING: " + (t2 - t1) + " ms");
                t1 = System.currentTimeMillis();
                for (int i = 0; i < 1000; i++)
                {
                    replies[i] = conn.sendCommand("LRANGE mylist 0 499");
                    checkState(replies[i].getType() == RedisReply.Type.ARRAY, "Expecting array for all replies");
                    checkState(replies[i].getElements().length == 500, "Expecting 500 replies");
                }
                t2 = System.currentTimeMillis();
                System.out.println("1000 LRANGE with 500 elements: " + (t2 - t1) + " ms");
                replies = new RedisReply[10000];
                for (int i = 0; i < 10000; i++)
                {
                    conn.appendCommand("PING");
                }
                t1 = System.currentTimeMillis();
                for (int i = 0; i < 10000; i++)
                {
                    replies[i] = conn.getReply();
                    checkState(replies[i].getType() == RedisReply.Type.STATUS, "Expecting status on each reply");
                }
                t2 = System.currentTimeMillis();
                System.out.println("10,000 PING pipelined: " + (t2 - t1) + " ms");
                for (int i = 0; i < 10000; i++)
                {
                    conn.appendCommand("LRANGE mylist 0 499");
                }
                t1 = System.currentTimeMillis();
                for (int i = 0; i < 10000; i++)
                {
                    replies[i] = conn.getReply();
                    checkState(replies[i].getType() == RedisReply.Type.ARRAY, "Expecting array for each reply");
                    checkState(replies[i].getElements().length == 500, "Expecting 500 elements for each reply");
                }
                t2 = System.currentTimeMillis();
                System.out.println("10,000 LRANGE with 500 elements pipelined: " + (t2 - t1) + " ms");
            }
        }
        
        System.out.println("All test passed");
        System.exit(0);
    }

}
