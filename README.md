# tacview-consumer
Tacview Events/Objects -> SQLite

## Overview
Tacview-consumer provides a high performance, real-time, event processor for Tacview streams.
The client can efficiently determine, based on proximity, the parent of all new objects
published to the stream.  It can also determine, using the same proximity algorithm,
the source of an impact event.  All processed data is written to a local sqlite database from
shared memory, ensuring that the stream processor is not blocked.

Tacview-consumer reliably processes ~70,000 messages/second, easily
exceeding the publication rate of even high volume Tacview server instances,
leaving considerable headroom for future extensions.

