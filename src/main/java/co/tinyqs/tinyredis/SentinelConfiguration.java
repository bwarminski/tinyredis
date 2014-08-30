package co.tinyqs.tinyredis;

import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class SentinelConfiguration
{
    public static int DEFAULT_SENTINEL_CONN_TIMEOUT = 200;
    private final Deque<SocketAddress> sentinelHosts;
    private final Deque<SocketAddress> hostsAttempted;
    private final int timeout;
    
    private final String serviceName;
    
    public SentinelConfiguration(String serviceName, List<SocketAddress> sentinelHosts)
    {
        this(serviceName, sentinelHosts, DEFAULT_SENTINEL_CONN_TIMEOUT);
    }
    
    public SentinelConfiguration(String serviceName, List<SocketAddress> sentinelHosts, int timeout)
    {
        Preconditions.checkArgument(!Preconditions.isNullOrEmpty(serviceName), "Service name may not be null or empty");
        Preconditions.checkArgument(!Preconditions.isNullOrEmpty(sentinelHosts), "Hosts list may not be null or empty");
        Preconditions.checkArgument(timeout > 0, "Timeout must be > 0");
        
        for (SocketAddress addr : sentinelHosts)
        {
            Preconditions.checkNotNull(addr, "Passed a null address");
        }
        
        this.serviceName = serviceName;
        this.sentinelHosts = new ArrayDeque<>(sentinelHosts);
        this.hostsAttempted = new ArrayDeque<>();
        this.timeout = timeout;
    }
    
    public String getServiceName()
    {
        return serviceName;
    }
    
    public int getTimeout()
    {
        return timeout;
    }
    
    public boolean moreHosts()
    {
        return !sentinelHosts.isEmpty();
    }
    
    public SocketAddress tryHost()
    {
        SocketAddress host = sentinelHosts.pop();
        hostsAttempted.push(host);
        return host;
    }
    
    public void successfulConnection()
    {
        sentinelHosts.push(hostsAttempted.pop());
        while (!hostsAttempted.isEmpty())
        {
            sentinelHosts.add(hostsAttempted.pollLast());
        }
    }
}
