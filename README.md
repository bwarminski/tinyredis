tinyredis
=========

Simple Java redis client based on hiredis

The advantage of tinyredis over other Java clients is the flexibility that its simplicity provides. The client is agnostic
to the version of redis being used and will not require upgrading as new commands and functionality are implemented on the
server. 

After getting the hang of basic client usage, you won't find yourself needing to refer to javadocs or source code just to
understand how to send a command to the server.

All credit for the concepts behind this implementation belongs to the original authors of hiredis. This is Java port of hiredis' 
blocking functionality with a few bells and whistles to deal with character encoding and serialization. 

Installation
------------

Assuming you have maven installed, simply clone the repository and execute `mvn package`. The jar will be in the target/ directory.
The only dependencies are JUnit for the unit tests.

Basic Usage
-----------

```java
SocketAddress addr = new InetSocketAddress("localhost", 6379);
try (RedisConnection conn = RedisConnection.connect(addr)) // Client implements autocloseable
{
    RedisReply reply = conn.sendCommand("PING"); // Sends a command and waits for a response from the server
    System.out.println(reply.getString()); // Prints "PONG" to stdout
}
```

Each connection can be optionally configured to throw exceptions when a reply is returned of type ERROR. This can help
prevent constant error checking if you except all commands sent to the server to be generally successful.

```java
SocketAddress addr = new InetSocketAddress("localhost", 6379);
try (RedisConnection conn = RedisConnection.connect(addr).exceptionOnError(true) // Client implements autocloseable
{
    RedisReply reply = conn.sendCommand("GARBAGE"); // Sends a command and waits for a response from the server
    System.out.println(reply.getString()); // Won't happen
}
catch (IOException e)
{
    System.err.println("Garbage in / garbage out");
}
```

String Interpolation
--------------------
The initial string is split on space characters and any %s is substituted inline as a single word of a command

```java
conn.sendCommand("SET %s %s", "message", "Hello World!"); // Sends 3 part command [SET] [message] [Hello World!]
conn.sendCommand("SET key%s %s", 1, "value"); // Sends [SET] [key1] [value]
```

Binary Serialization
--------------------
RedisConnections can also be configured to serialize objects directly into binary-safe command strings. Simply implement one or more instances of
*RedisSerializer* and register them with the connection. The registered serializers will be used to interpolate %b placeholders in the
format string.

```java
// Trivial serializer that just serializes toString() as UTF-16
conn.registerSerializer(new RedisSerializer(){ // registerSerializer also uses the builder pattern for try-with-resources

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
        }
});

conn.sendCommand("SET %s %b", "foo", "hello world");
RedisReply reply = conn.sendCommand("GET foo");
byte[] helloWorldBytes = "hello world".getBytes(StandardCharsets.UTF_16);
System.out.println(Arrays.equals(helloWorldBytes, rely.getBytes())); // Prints "true"

```

Pipelining
----------

Multiple commands can be pipelined using appendCommand(). The full batch of commands will be sent with the next call to getReply().

```java
for (int i = 0; i < 10000; i++)
{
    conn.appendCommand("PING");
}

for (int i = 0; i < 10000; i++)
{
    System.out.println(conn.geyReply().getString()); // Prints 10,000 PONGS
}                
```
