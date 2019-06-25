## Kafka as a System-Of-Record

### A Progression of Kafka Use Cases

1. Buffering (e.g. ingest into Elastic Search)
2. ETL
3. Stream Processing
4. System-Of-Record (?)

### The Problem

Don't get unique / foreign key constraints capability for materialized data out-of-the-box. Not trivial to implement. 

What about making a higher level abstraction to make this easy(er)?

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
- It's not that far-fetched - a distributed log is a very solid building block.

**Why Not**

- Other systems will provide this functionality better (though many of them not as able to scale as Kafka). If you're not operating at scale, this is not worth considering. If you are, I think it is.
- Highish write amplification (in initial design at least).
- Highish latency (need to synchonously wait on sequence of events to propagate).


### Implementation

To implement this, we fundamentally need to duplicate the change log data in topics partitioned by each of the unique keys.

Use compacted topic to back materialized view of each.

But also introduce a 'command' topic for each unique key. All changes go via the command topics to validate.

```
      id             username           email
[ change_log ]    [ change_log ]    [ change-log ]
[  command   ]    [  command   ]    [  command   ]
```

All topics (change log + command) will have an equal number of partitions.

There is one process per partition number. This process is responsible for that partition number for all topics. i.e.:

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

1. determine the partition # associated with the id. this points us to the process that will coordinate execution of the command. Actually, the requirement is just that we need to be on a process that has a relevant command topic partition corresponding to any unique key.
2. write a *change command* to each command topic for each unique key. for other keys, these will generally be different partitions than the one of the coordinator process. The command will be given a unique guid token.
3. the process associated with the partition for each unique key reads in the commands, determines if the command clashes with the current state of the materialized view.
    1. if not, writes a *verify command* to each of the other command topics (citing guid), an
    2. if yes, writes a *abort command* to each of the other command topics (citing guid).
4. When verify or abort commands are consumed in by the controller process for all unique keys, the change can be applied (or not in the case of abort) to the change-log compacted topic. 
5. Transactions are not required for the above.
    1. duplicate change, verify or abort commands do not matter. duplicate write to changelog topic does not matter.
    2. commit offsets after change-log topics have been written to.
6. The controlling process completes a task associated with the change after the change for it's managed key has been written to the changelog table, waking up the code that made the request.
7. Eventually consistent across keys.

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
