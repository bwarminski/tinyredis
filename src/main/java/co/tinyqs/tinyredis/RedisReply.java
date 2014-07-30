package co.tinyqs.tinyredis;

import java.nio.ByteBuffer;

public class RedisReply
{
    public enum Type
    {
        STRING,
        ARRAY,
        INTEGER,
        NIL,
        STATUS,
        ERROR
    }
    private Type type = null;
    private long integer = 0;
    private byte[] buff = null;
    private RedisReply[] elements = null;
    
    private RedisReply(){};
    
    public static RedisReply createInteger(long integer)
    {
        RedisReply result = new RedisReply();
        result.type = Type.INTEGER;
        result.integer = integer;
        return result;
    }
    
    public static RedisReply createArray(RedisReply[] elements)
    {
        RedisReply result = new RedisReply();
        result.type = Type.ARRAY;
        result.elements = Preconditions.checkNotNull(elements);
        return result;
    }
    
    public static RedisReply createString(Type type, byte[] buff)
    {
        Preconditions.checkState(type == Type.ERROR || type == Type.STATUS, "Type must be error or status");
        RedisReply result = new RedisReply();
        result.type = type;
        result.buff = buff;
        return result;
    }
    
    public static RedisReply createNil()
    {
        RedisReply result = new RedisReply();
        result.type = Type.NIL;
        return result;
    }
    
    public static RedisReply createBulkString(byte[] buff)
    {
        RedisReply result = new RedisReply();
        result.type = Type.STRING;
        result.buff = Preconditions.checkNotNull(buff);
        return result;
    }
    
    public Type getType()
    {
        return type;
    }

    public byte[] getBuff()
    {
        Preconditions.checkState(type == Type.ERROR || type == Type.STATUS || type == Type.STRING, "getBuff() is only valid for string-type replies");
        return buff;
    }

    public RedisReply[] getElements()
    {
        Preconditions.checkState(type == Type.ARRAY, "getElements() is only valid for array replies");
        return elements;
    }

    public long getInteger()
    {
        Preconditions.checkState(type == Type.INTEGER, "getInteger() is only valid for integer replies");
        return integer;
    }
}
