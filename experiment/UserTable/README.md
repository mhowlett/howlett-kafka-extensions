## Kafka as a System-Of-Record

### A Progression of Kafka Use Cases

In increasing order of sophistication:

1. Buffering
2. ETL
3. Stream Processing
4. System of Record

### System of Record - Limitations

Unique / foreign key constraint capability for materialized change log data does not come out-of-the-box.

How easy is it to build an abstraction for this purpose? Not trivial.

As a concrete example, consider enforcing more than one unique constraint on a users table.

```
{
    "Name": "users",
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

- The fewer disparate systems you need to manage, the better. If you are building an application on top of Kafka, perhaps this will allow you can avoid adding another system to your stack.
- [specific to me] I know more about Kafka than any database.
- Perhaps Kafka enables constraints (maybe involving time / ordering) that are useful that are difficult / not possible to achieve with other systems.

**Why Not**

- Other systems will provide similar functionality, better. Horizontally scalable systems that allow for scalable constraints OOTB include:
    - [Cockroach Db](https://www.cockroachlabs.com/)
    - [Vitess](https://vitess.io/)
    - [YugaByte](http://yugabyte.com/)
    - [Fauna](https://fauna.com/)
    - TODO: more?
- If you don't need horizontal scalabiliy, there are many other highly flexible alternatives: postgres etc.
- Not hugely performant:
    - High write amplification (in initial design at least).
    - High latency (need to synchonously wait on sequence of events to propagate).
    - But nothing will be - fundamentally difficult problem.


### Implementation

To implement this, change log data fundamentally needs to be duplicated in topics partitioned by each of the unique keys.

Use compacted topic to back a materialized view of each.

But also introduce a 'command' topic for each unique key. All changes go via the command topics in order to validate / provide a fine-grained locking mechanism.

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

TODO (refer to the code!)

### API

TODO

### Consumer Groups and Replicas

- Use a static partition assignment - no consumer groups.
    - Don't ever want partitions moving about. 
- Ultimately, want to have more than one materialized view processor operating side-by-side each partition. One will be the 'leader' (which does the work required re: the command topic). 
- The others will be used to implement zero-wait failover + scale read throughput.

### Kafka Topic replication requirements

- For HA, need three replicas for all topics.
- That's expensive.
- If all we care about is not loosing data, data requirements are just triple replication of change log data in some form. with three unique columns, have this via re-partitioning automatically.
    - It would be possible to make this work, but it'd be substantially more complex to implement.



no unique column is special - which is 'primary' depends on request.