package co.tinyqs.tinyredis;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface RedisSerializer
{
    public boolean canSerialize(Object obj);
    public byte[] serialize(Object obj) throws IOException;
}
