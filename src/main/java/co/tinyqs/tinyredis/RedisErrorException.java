package co.tinyqs.tinyredis;

import java.io.IOException;

/**
 * Convenience exception indicating that IO was successful, but the server returned an
 * error response unexpectedly.
 */
public class RedisErrorException extends IOException
{
    private static final long serialVersionUID = 6053073145724626493L;

    public RedisErrorException(String msg)
    {
        super(msg);
    }
}
