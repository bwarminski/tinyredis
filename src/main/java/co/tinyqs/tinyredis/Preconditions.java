package co.tinyqs.tinyredis;

/**
 * Static utility functions borrowed from Guava
 * @author bwarminski
 *
 */
public class Preconditions
{
    public static <T> T checkNotNull(T reference, String message)
    {
        if (reference == null)
        {
            throw new NullPointerException(message);
        }
        return reference;
    }
    
    public static <T> T checkNotNull(T reference)
    {
        if (reference == null)
        {
            throw new NullPointerException();
        }
        return reference;
    }
    
    public static <T> T firstNonNull(T first, T second)
    {
        return first == null ? second : first;
    }
    
    public static void checkState(boolean expression, String message)
    {
        if (!expression)
        {
            throw new IllegalStateException(message);
        }
    }
}
