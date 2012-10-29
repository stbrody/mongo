var s = new ShardingTest("recvChunkCommitFailed", 2, 0, 1, {shardOptions : {enableFaultInjection : ""}});

s.adminCommand({enablesharding : "test"});
s.adminCommand({shardcollection : "test.foo" , key : {num : 1}});

var db = s.getDB("test");
var str = new Array(1024*64).toString();
for (var i = 0; i < 100; i++) {
    db.foo.insert({num : i, str : str});
}
assert.eq(100, db.foo.find().itcount());

// Make sure there are constant queries to trigger setShardVersion commands.
startParallelShell("var i = 0; while(true) { var res = db.foo.update({num : i%100}, {$set : {val : i}});" +
                   "if (i++%1000 == 0) { print('inserting');}}");

s.adminCommand({moveChunk : "test.foo", find : {num : 1}, to : "shard0000"});

s.shard0.adminCommand({configureFailPoint : 'recvChunkCommitFailpoint', mode : 'alwaysOn'});
s.shard1.adminCommand({configureFailPoint : 'recvChunkCommitFailpoint', mode : 'alwaysOn'});

assert.throws(function() {s.adminCommand({moveChunk : "test.foo", find : {num : 1}, to : "shard0001"});});

// Make sure queries still work - version information was reset properly.
assert.eq(100, db.foo.find().itcount());

s.shard0.adminCommand({configureFailPoint : 'recvChunkCommitFailpoint', mode : 'off'});
s.shard1.adminCommand({configureFailPoint : 'recvChunkCommitFailpoint', mode : 'off'});

s.adminCommand({moveChunk : "test.foo", find : {num : 1}, to : "shard0001"}).ok;
assert.eq(100, db.foo.find().itcount());

s.stop();
