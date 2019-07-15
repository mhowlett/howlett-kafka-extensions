## Kafka as a System-Of-Record

### A Progression of Kafka Use Cases

In increasing order of sophistication:

1. Buffering
2. ETL
3. Stream Processing
4. System-Of-Record (?)

### A Problem

Unique / foreign key constraint capability for materialized data does not come for free out-of-the-box.

How easy is it to make a higher level abstraction for this purpose? Not trivial.

As a concrete example, consider enforcing unique constraints on users table.

```
{
    "Name": "users",
    "NumPartitions": 2,
    "Columns": {
      "id":        { "Type": "long",   "Unique": true },
      "username":  { "Type": "string", "Unique": true },
      "email":     { "Type": "string", "Unique": true },
      "firstname": { "Type": "string", "Unique": false },
      "lastname":  { "Type": "string", "Unique": false },
      "quota":     { "Type": "long",   "Unique": false }
    }
}
```

### Justification

**Why**

- The fewer disparate systems you need to manage, the better.
- Specific to me: I know more about Kafka than anything other storage system.

**Why Not**

- Other systems will provide similar functionality better (but which ones can scale to the degree that this strategy can?). If you're not operating at scale, this approach is not worth considering. If you are, I think it is.
- Highish write amplification (in initial design at least).
- Highish latency (need to synchonously wait on sequence of events to propagate).


### Implementation

To implement this, we fundamentally need to duplicate the change log data in topics partitioned by each of the unique keys.

Use compacted topic to back a materialized view of each.

But also introduce a 'command' topic for each unique key. All changes go via the command topics in order to validate.

```
      id             username           email
[ change_log ]    [ change_log ]    [ change-log ]
[  command   ]    [  command   ]    [  command   ]
```

All topics (change log + command) will have an equal number of partitions.

There will be one process per partition number. This process is responsible for that partition number for all topics. i.e.:

```
     process for                 process for
     partition 0                 partition 1
  ------------------           -----------------
  id changelog                 id changelog
  username changelog           username changelog
  email changelog              email changelog
  id command                   id command
  username command             username command
  email command                email command
```

Say we want to update the username of a particular user and we have their id:

Example 

TODO.

### API

Ultimately code generation should be used to make an easy to use API, but initially identify columns by string names and use object type for values.

Serialization format JSON.

### Consumer Groups and Replicas

- Use a static partition assignment - no consumer groups.
- Don't ever want partitions moving about. 
- Ultimately, want to have more than one materialized view processor operating side-by-side per partition. One will be the 'leader' (which does the work required re: the command topic). 
- The others will be used to implement zero-wait failover + scale read throughput.

### Kafka Topic replication requirements

- For HA, need three replicas for all topics.
- That's expensive.
- If all we care about is not loosing data, data requirements are just triple replication of change log data. with three unique columns, have this via re-partitioning automatically.
    - It would be possible to make this work, but it'd be substantially more complex to implement.
