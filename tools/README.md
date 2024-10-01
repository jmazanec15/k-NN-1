# Map Document IDs to Segments

## Usage

First, obtain a list of id's with field "id" and put them in a file. At the end of the file add -1. Call it docIdsForShard. This assumes id is an int.

Then, on the node, run:
```json
./gradlew -q  -p tools run --args " <OS_DATA_PATH>/nodes/0/indices/<INDEX_ID>/<SHARD_ID>/index/ /tmp/segments.txt" < docIdsForShard.txt
```

This will output the segment the given id is in:
```json
3367797 2
178318 8
827163 8
3286686 2
3134518 14
4319202 8
605152 8
2542747 2
1293931 8
2733146 8
476012 8
2174617 8
4628646 2
4507457 8
4589724 2
```
