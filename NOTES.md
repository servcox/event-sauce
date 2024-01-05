## Events
* One blob folder per topic, contains a file for each of 50k events
    - {topic}\00000000000000001.tsv
    - {topic}\00000000000000002.tsv
    - .. 
* New row for each event
    - {streamid}\t{eventtype}\t{body}\t{createdby}\t{createdat}\n
    - JSAFZC32S00100636\tSERVCO.SOLUTIONS.EVENTS.INSTANCES.COMPLIANCEDATEDETERMINED\t{"Id":"6279036883959808"}\t2024-01-03T22:54:59.9786093Z\7baad952-3fba-4628-9895-7a530a729fa4

## Projections
* One blob per projection
    - {projection}/{version}/{hash}.json
* Client loads remote cache into memory at startup
* Local cache is updated on each read
* Remote cache is written every hour

Advantages:
* Super fast reads
* Super low cost

## Interface
EventStore(topic, connectionString)
 * Write(event)
 * Write(events[])
 * Read(offset) : Event[]

ProjectionStore<TProjection>(version, connectionString)
 * TryRead(streamid): TProjection?
 * Read(streamId) : TProjection
 * Query(Eq(key,value)) : TProjection[]


CONFIGURATION:
 * Projection definition
 * Index fields
 * How often to update cache

NOTES:
 * Block read until cache has been read