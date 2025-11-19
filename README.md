# No Asyncio NATS

A Python wrapper for NATS that provides an API without requiring asyncio, built with Rust and PyO3.

Rant: This is the real pain of asyncio - libraries need to be implemented twice.
WHY? We have gevent, which does not force to reimplement everything. 

## Internals

This driver uses [Rust async_nats](https://docs.rs/async-nats/latest/async_nats/) under the hood.
The Tokio runtime is executed in a seperate (not python aware) thread. Syncronization/Notification between python and Tokio is done via [eventfd](https://man7.org/linux/man-pages/man2/eventfd.2.html)

Even with this overhead performance is still decent.

By using eventfd this driver can also be used with **gevent**. Simply use the `gevent_connect` function, which provides a gevent friendly NATS connection.

## Installation

```bash
pip install no-asyncio-nats
```

## Quick Start

```python
import no_asyncio_nats
import datetime

# Connect to NATS
nc = no_asyncio_nats.connect("nats://localhost:4222")

# Subscribe to a subject
sub = nc.subscribe('my.subject')

# Publish a message
nc.publish('my.subject', b'Hello, World!')

# Receive a message
message = sub.recv_msg()
print(f"Received: {message['payload']}")

# Close the connection
sub.unsubscribe()
```

## API Reference

### Connection

#### `connect(address, options=None)`

Connects to a NATS server and returns a `Client` instance.

**Parameters:**
- `address` (str): The NATS server address (e.g., "nats://localhost:4222")
- `options` (dict, optional): Connection configuration options

**Returns:**
- `Client`: A client instance for interacting with NATS

**Connection Options:**

The `options` parameter is a dictionary that can contain the following keys:

**Authentication Options:**
- `token` (str): Authentication token
- `user_and_password` (tuple): Username and password as `(username, password)`
- `nkey` (str): Nkey seed for authentication
- `credentials_file` (str): Path to credentials file
- `credentials` (str): Credentials as a string

**TLS Options:**
- `tls_root_certificates` (str): Path to root certificate file
- `tls_client_cert` (str): Path to client certificate file
- `tls_client_key` (str): Path to client private key file
- `require_tls` (bool): Whether TLS is required
- `tls_first` (bool): Whether to establish TLS connection before handshake

**Timeout and Performance Options:**
- `ping_interval` (datetime.timedelta): Ping interval
- `connection_timeout` (datetime.timedelta): Connection timeout
- `request_timeout` (datetime.timedelta): Request timeout
- `subscription_capacity` (int): Maximum number of subscriptions
- `read_buffer_capacity` (int): Read buffer size

**Connection Behavior Options:**
- `no_echo` (bool): Disable message echo
- `custom_inbox_prefix` (str): Custom inbox prefix
- `name` (str): Client name
- `retry_on_initial_connect` (bool): Retry on initial connection failure
- `max_reconnects` (int): Maximum number of reconnection attempts
- `ignore_discovered_servers` (bool): Ignore server discovery
- `retain_servers_order` (bool): Retain server connection order

### Client Class

The main client class for interacting with NATS.

#### Methods

##### `publish(subject, data, reply=None, headers=None)`

Publishes a message to a subject.

**Parameters:**
- `subject` (str): The subject to publish to
- `data` (bytes): The message payload as bytes
- `reply` (str, optional): Reply subject for responses
- `headers` (dict, optional): Message headers as key-value pairs

**Returns:**
- None

##### `flush()`

Flushes all pending messages to the server.

**Parameters:**
- None

**Returns:**
- None

##### `subscribe(subject)`

Subscribes to a subject and returns a `Subscriber` instance.

**Parameters:**
- `subject` (str): The subject to subscribe to

**Returns:**
- `Subscriber`: A subscriber instance for receiving messages

##### `queue_subscribe(subject, queue_group)`

Subscribes to a subject with queue group semantics.

**Parameters:**
- `subject` (str): The subject to subscribe to
- `queue_group` (str): The queue group name

**Returns:**
- `Subscriber`: A subscriber instance for receiving messages

##### `request(subject, data, headers=None)`

Performs a request-response pattern.

**Parameters:**
- `subject` (str): The subject to send the request to
- `data` (bytes): The request payload as bytes
- `headers` (dict, optional): Request headers as key-value pairs

**Returns:**
- dict: Response message containing:
  - `subject` (str): Response subject
  - `payload` (bytes): Response payload
  - `reply` (str, optional): Reply subject
  - `headers` (dict, optional): Response headers

##### `new_inbox()`

Creates a new unique inbox name.

**Parameters:**
- None

**Returns:**
- str: A unique inbox name

##### `jetstream()`

Creates a JetStream context for advanced messaging features.

**Parameters:**
- None

**Returns:**
- `JetStream`: A JetStream instance

### Subscriber Class

Represents a subscription to a NATS subject.

#### Methods

##### `drain()`

Drains the subscription, allowing in-flight messages to be processed.

**Parameters:**
- None

**Returns:**
- None

##### `unsubscribe()`

Unsubscribes from the subject.

**Parameters:**
- None

**Returns:**
- None

##### `unsubscribe_after(count)`

Unsubscribes after receiving a specific number of messages.

**Parameters:**
- `count` (int): Number of messages to receive before unsubscribing

**Returns:**
- None

##### `recv_msg(timeout=None)`

Receives a message from the subscription.

**Parameters:**
- `timeout` (datetime.timedelta, optional): Timeout. If None, blocks indefinitely.

**Returns:**
- dict or None: Message dictionary or None if timeout occurs. Message contains:
  - `subject` (str): Message subject
  - `payload` (bytes): Message payload
  - `reply` (str, optional): Reply subject
  - `headers` (dict, optional): Message headers

### JetStream Class

Provides JetStream functionality for persistent messaging and streams.

#### Methods

##### `set_timeout(timeout)`

Sets the timeout for JetStream operations.

**Parameters:**
- `timeout` (datetime.timedelta): Timeout

**Returns:**
- None

##### `publish(subject, data, headers=None)`

Publishes a message to a JetStream stream.

**Parameters:**
- `subject` (str): The subject to publish to
- `data` (bytes): The message payload as bytes
- `headers` (dict, optional): Message headers as key-value pairs

**Returns:**
- `PublishAckFuture`: A future that will contain the publish acknowledgment

##### `get_or_create_stream(stream_config)`

Gets or creates a JetStream stream.

**Parameters:**
- `stream_config` (dict): Stream configuration dictionary

**Returns:**
- `JetStreamStream`: A stream instance

##### `delete_stream(stream)`

Deletes a JetStream stream.

**Parameters:**
- `stream` (str): Stream name

**Returns:**
- bool: True if deletion was successful

### JetStreamStream Class

Represents a JetStream stream.

#### Methods

##### `get_consumer(name, typename, ordered)`

Gets a consumer from the stream.

**Parameters:**
- `name` (str): Consumer name
- `typename` (str): Consumer type ("pull" for pull consumers)
- `ordered` (bool): Whether consumer should maintain message order

**Returns:**
- `JetStreamPullConsumer`: A pull consumer instance

##### `get_or_create_consumer(name, typename, ordered, config)`

Gets or creates a consumer from the stream.

**Parameters:**
- `name` (str): Consumer name
- `typename` (str): Consumer type ("pull" for pull consumers)
- `ordered` (bool): Whether consumer should maintain message order
- `config` (dict): Consumer configuration dictionary

**Returns:**
- `JetStreamPullConsumer`: A pull consumer instance

### JetStreamPullConsumer Class

Represents a pull-based consumer in JetStream.

#### Methods

##### `make_receiver()`

Creates a message receiver for the consumer.

**Parameters:**
- None

**Returns:**
- `JetStreamPullConsumerMessages`: A message receiver instance

### JetStreamPullConsumerMessages Class

Provides methods to receive messages from a pull consumer.

#### Methods

##### `recv_msg()`

Receives a message from the consumer.

**Parameters:**
- None

**Returns:**
- `Message` or None: A message instance or None if no messages available

### PublishAckFuture Class

Represents a future publish acknowledgment in JetStream.

#### Methods

##### `wait()`

Waits for the publish acknowledgment.

**Parameters:**
- None

**Returns:**
- dict: Acknowledgment containing:
  - `stream` (str): Stream name
  - `sequence` (int): Message sequence number
  - `domain` (str): Domain name
  - `duplicate` (bool): Whether message is a duplicate
  - `value` (str): Acknowledgment value

### Message Structure

Messages returned by the API have the following structure:

```python
{
    "subject": "message_subject",
    "payload": b"message_payload",
    "reply": "reply_subject",  # optional
    "headers": {"key": "value"},  # optional
    "status": 200,  # optional
    "description": "status_description"  # optional
}
```

### HeaderMap Structure

Headers are represented as dictionaries where keys and values are strings:

```python
{
    "Content-Type": "application/json",
    "X-Custom-Header": "value",
    "X-Request-ID": "12345"
}
```

## JetStream Configuration

### Stream Configuration

The `stream_config` parameter for `get_or_create_stream` accepts the following keys:

**Required Fields:**
- `name` (str): Stream name

**Subject Matching:**
- `subjects` (list[str]): List of subjects the stream should match. Supports wildcards like "events.>" to match "events.user.created", "events.order.updated", etc.

**Basic Configuration:**
- `retention` (str): Retention policy ("limits", "interest", "workqueue")
- `storage` (str): Storage type ("file", "memory")
- `discard` (str): Discard policy ("new", "old")
- `num_replicas` (int): Number of replicas
- `max_messages` (int): Maximum number of messages
- `max_bytes` (int): Maximum size in bytes
- `max_age` (datetime.timedelta): Maximum message age
- `max_message_size` (int): Maximum individual message size

**Advanced Configuration:**
- `no_ack` (bool): Disable acknowledgments
- `allow_direct` (bool): Allow direct access
- `compression` (str): Compression policy ("none", "s2")
- `max_messages_per_subject` (int): Maximum messages per subject
- `duplicate_window` (datetime.timedelta): Duplicate detection window
- `sealed` (bool): Whether stream is sealed
- `mirror_direct` (bool): Direct mirror access
- `allow_rollup` (bool): Allow rollup messages
- `deny_delete` (bool): Deny stream deletion
- `deny_purge` (bool): Deny stream purging
- `allow_message_ttl` (bool): Allow message TTL
- `allow_atomic_publish` (bool): Allow atomic publish
- `allow_message_schedules` (bool): Allow message scheduling
- `allow_message_counter` (bool): Allow message counters
- `description` (str): Stream description
- `template_owner` (str): Template owner
- `first_sequence` (int): First sequence number
- `metadata` (dict): Additional metadata

**Replication and Mirroring:**
- `republish` (dict): Republish configuration
- `mirror` (dict): Mirror configuration
- `sources` (list[dict]): List of source configurations
- `subject_transform` (dict): Subject transformation
- `consumer_limits` (dict): Consumer limits
- `placement` (dict): Placement configuration
- `persist_mode` (str): Persistence mode ("default", "async")
- `pause_until` (str): Pause until timestamp
- `subject_delete_marker_ttl` (datetime.timedelta): Subject delete marker TTL

### Consumer Configuration

The `config` parameter for `get_or_create_consumer` accepts the following keys:

**Basic Configuration:**
- `durable_name` (str): Durable consumer name
- `name` (str): Consumer name
- `description` (str): Consumer description
- `filter_subject` (str): Subject filter
- `filter_subjects` (list[str]): List of subject filters
- `deliver_policy` (str): Delivery policy ("all", "last", "new", "last_per_subject")
- `ack_policy` (str): Acknowledgment policy ("explicit", "none", "all")
- `replay_policy` (str): Replay policy ("instant", "original")
- `priority_policy` (str): Priority policy ("overflow", "pinned_client", "prioritized", "none")

**Timing Configuration:**
- `ack_wait` (datetime.timedelta): Acknowledgment wait time
- `max_expires` (datetime.timedelta): Maximum expiration time
- `inactive_threshold` (datetime.timedelta): Inactive threshold

**Rate and Size Limits:**
- `max_deliver` (int): Maximum delivery attempts
- `rate_limit` (int): Rate limit in bytes per second
- `sample_frequency` (int): Sample frequency (0-100)
- `max_waiting` (int): Maximum waiting messages
- `max_ack_pending` (int): Maximum acknowledgments pending
- `max_batch` (int): Maximum batch size
- `max_bytes` (int): Maximum bytes
- `num_replicas` (int): Number of replicas

**Behavior Configuration:**
- `headers_only` (bool): Headers only mode
- `memory_storage` (bool): Memory storage mode
- `metadata` (dict): Additional metadata
- `backoff` (list[datetime.timedelta]): Backoff intervals
- `priority_groups` (list[str]): Priority groups
- `pause_until` (str): Pause until timestamp

### Complex Configuration Types

#### Republish Configuration

The `republish` configuration accepts the following keys:

```python
{
    "source": "events.>",           # str: Source subject pattern with wildcards
    "destination": "processed.>",   # str: Destination subject pattern with wildcards
    "headers_only": False           # bool: Whether to republish only headers
}
```

#### Mirror Configuration

The `mirror` configuration accepts the following keys:

```python
{
    "name": "source_stream",        # str: Name of the source stream to mirror (required)
    "start_sequence": 1000,         # int, optional: Starting sequence number
    "start_time": "2023-01-01T00:00:00Z",  # str, optional: Starting timestamp in ISO format
    "filter_subject": "events.>",   # str, optional: Subject filter with wildcards
    "external": {                   # dict, optional: External stream configuration
        "api_prefix": "JS.external.api",
        "delivery_prefix": "external.delivery"
    },
    "domain": "example.com",        # str, optional: Domain name
    "subject_transforms": [         # list[dict], optional: List of subject transformations
        {
            "source": "old.>",
            "destination": "new.>"
        }
    ]
}
```

#### Source Configuration

Each source in the `sources` list accepts the following keys:

```python
[
    {
        "name": "source_stream",    # str: Name of the source stream (required)
        "start_sequence": 1000,     # int, optional: Starting sequence number
        "start_time": "2023-01-01T00:00:00Z",  # str, optional: Starting timestamp in ISO format
        "filter_subject": "events.>",  # str, optional: Subject filter with wildcards
        "external": {               # dict, optional: External stream configuration
            "api_prefix": "JS.external.api",
            "delivery_prefix": "external.delivery"
        },
        "domain": "example.com",    # str, optional: Domain name
        "subject_transforms": [     # list[dict], optional: List of subject transformations
            {
                "source": "old.>",
                "destination": "new.>"
            }
        ]
    }
]
```

#### Subject Transformation

The `subject_transform` configuration accepts the following keys:

```python
{
    "source": "old.>",        # str: Source subject pattern with wildcards
    "destination": "new.>"    # str: Destination subject pattern with wildcards
}
```

#### Consumer Limits

The `consumer_limits` configuration accepts the following keys:

```python
{
    "inactive_threshold": datetime.timedelta(hours=1),  # datetime.timedelta: Inactive threshold
    "max_ack_pending": 1000    # int: Maximum acknowledgments pending
}
```

#### Placement Configuration

The `placement` configuration accepts the following keys:

```python
{
    "cluster": "nats-cluster",  # str, optional: Cluster name
    "tags": ["fast", "ssd"]     # list[str]: List of placement tags
}
```

#### External Configuration

The `external` configuration (used in mirror/source configs) accepts the following keys:

```python
{
    "api_prefix": "JS.external.api",        # str: API prefix for external access
    "delivery_prefix": "external.delivery"  # str, optional: Delivery prefix for external access
}
```

#### Backoff Configuration

The `backoff` configuration (used in consumer configs) accepts a list of `datetime.timedelta` objects:

```python
{
    "backoff": [
        datetime.timedelta(milliseconds=100),
        datetime.timedelta(seconds=1),
        datetime.timedelta(seconds=5)
    ]
}
```

## Examples

### Basic Publishing and Subscribing

```python
import no_asyncio_nats

# Connect to NATS
nc = no_asyncio_nats.connect("nats://localhost:4222")

# Subscribe to a subject
sub = nc.subscribe('updates')

# Publish a message
nc.publish('updates', b'Hello, World!')

# Receive a message
message = sub.recv_msg()
print(f"Received: {message['payload']}")

# Clean up
sub.unsubscribe()
```

### Request-Response Pattern

```python
import no_asyncio_nats

nc = no_asyncio_nats.connect("nats://localhost:4222")

# Send a request
response = nc.request('service.hello', b'Hello, Service!')
print(f"Response: {response['payload']}")

# Handle multiple requests
def handle_request(message):
    print(f"Received request: {message['payload']}")
    response_data = b"Hello, Client!"
    nc.publish(message['reply'], response_data)

# Set up request handler
sub = nc.subscribe('service.hello')
while True:
    msg = sub.recv_msg()
    handle_request(msg)
```

### Queue Groups

```python
import no_asyncio_nats
import threading

def worker(worker_id):
    nc = no_asyncio_nats.connect("nats://localhost:4222")
    sub = nc.queue_subscribe('tasks', 'worker-group')
    
    while True:
        msg = sub.recv_msg()
        print(f"Worker {worker_id} processing: {msg['payload']}")

# Start multiple workers
for i in range(3):
    threading.Thread(target=worker, args=(i,)).start()
```

### JetStream Publishing

```python
import no_asyncio_nats
import datetime

nc = no_asyncio_nats.connect("nats://localhost:4222")

# Get JetStream context
js = nc.jetstream()

# Create a stream
stream_config = {
    "name": "mystream",
    "subjects": ["events.>", "notifications.>"],  # List of subject patterns with wildcards
    "retention": "limits",
    "max_messages": 1000,
    "max_age": datetime.timedelta(days=7),  # Use timedelta for time durations
    "consumer_limits": {
        "inactive_threshold": datetime.timedelta(hours=1),
        "max_ack_pending": 1000
    }
}
stream = js.get_or_create_stream(stream_config)

# Publish a message
ack_future = js.publish('events.user.created', b'User created data')
ack = ack_future.wait()
print(f"Published to sequence: {ack['sequence']}")

# Create a pull consumer
consumer_config = {
    "durable_name": "myconsumer",
    "deliver_policy": "all",
    "ack_policy": "explicit",
    "max_deliver": 3
}
consumer = stream.get_or_create_consumer("myconsumer", "pull", False, consumer_config)

# Create a message receiver
receiver = consumer.make_receiver()

# Receive messages
while True:
    msg = receiver.recv_msg()
    if msg:
        print(f"Received: {msg['payload']}")
        # Acknowledge the message
        msg.ack()
```

### Advanced Connection Options

```python
import no_asyncio_nats
import datetime

# Connect with authentication and TLS
options = {
    "name": "my-client",
    "user_and_password": ("username", "password"),
    "tls_root_certificates": "/path/to/ca.crt",
    "tls_client_cert": "/path/to/client.crt",
    "tls_client_key": "/path/to/client.key",
    "connection_timeout": datetime.timedelta(seconds=10),  # Use timedelta for time durations
    "ping_interval": datetime.timedelta(seconds=30),      # Use timedelta for time durations
    "max_reconnects": 5
}

nc = no_asyncio_nats.connect("nats://localhost:4222", options)
```

### Advanced Stream Configuration Example

```python
import no_asyncio_nats
import datetime

nc = no_asyncio_nats.connect("nats://localhost:4222")
js = nc.jetstream()

# Complex stream configuration with all features
stream_config = {
    "name": "advanced_stream",
    "subjects": [
        "events.user.>",      # Matches events.user.created, events.user.updated, etc.
        "events.order.>",     # Matches events.order.created, events.order.cancelled, etc.
        "system.metrics"      # Exact match
    ],
    "retention": "limits",
    "storage": "file",
    "discard": "old",
    "num_replicas": 3,
    "max_messages": 1000000,
    "max_bytes": 1073741824,  # 1GB
    "max_age": datetime.timedelta(days=30),
    "max_message_size": 1048576,  # 1MB
    "max_consumers": 100,
    "duplicate_window": datetime.timedelta(minutes=2),
    "description": "Advanced stream with mirroring and replication",
    
    # Replication
    "republish": {
        "source": "internal.>",
        "destination": "external.>",
        "headers_only": False
    },
    
    # Mirroring
    "mirror": {
        "name": "source_stream",
        "filter_subject": "critical.>",
        "external": {
            "api_prefix": "JS.mirror.api",
            "delivery_prefix": "mirror.delivery"
        }
    },
    
    # Multiple sources
    "sources": [
        {
            "name": "backup_stream",
            "filter_subject": "backup.>",
            "subject_transforms": [
                {
                    "source": "backup.>",
                    "destination": "archived.>"
                }
            ]
        }
    ],
    
    # Consumer limits
    "consumer_limits": {
        "inactive_threshold": datetime.timedelta(hours=2),
        "max_ack_pending": 5000
    },
    
    # Placement
    "placement": {
        "cluster": "primary-cluster",
        "tags": ["fast", "ssd", "eu-west"]
    },
    
    # Additional features
    "allow_direct": True,
    "allow_rollup": True,
    "allow_message_ttl": True,
    "allow_atomic_publish": True,
    "metadata": {
        "environment": "production",
        "owner": "data-team"
    }
}

stream = js.get_or_create_stream(stream_config)
```

## Error Handling

The API uses exceptions to handle errors. Common exceptions include:

- `ConnectionError`: When connection to NATS fails
- `TimeoutError`: When operations timeout
- `ValueError`: When invalid parameters are provided

Always handle exceptions appropriately:

```python
try:
    nc = no_asyncio_nats.connect("nats://localhost:4222")
    response = nc.request('service.hello', b'Hello')
except ConnectionError as e:
    print(f"Connection failed: {e}")
except TimeoutError as e:
    print(f"Request timed out: {e}")
```
