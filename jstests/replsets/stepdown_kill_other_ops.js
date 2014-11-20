// Ensure that stepDown kills all other running operations TODO FINISH THIS
// 1. Start up a 3 node set (1 arbiter).
// 2. Isolate the SECONDARY.
// 3. Do one write and then spin up a second shell which asks the PRIMARY to StepDown.
// 4. Once StepDown has begun, spin up a third shell which will attempt to do writes, which should
//    block waiting for stepDown to release its lock, which it never will do because no secondaries
//    are caught up.
// 5. Once a write is blocked, kill the stepDown operation
// 6. Writes should become unblocked and the primary should stay primary

(function () {
     "use strict";
     var name = "stepdownKillOps";
     var replSet = new ReplSetTest({name: name, nodes: 3});
     var nodes = replSet.nodeList();
     replSet.startSet();
     replSet.initiate({"_id" : name,
                       "members" : [
                           {"_id" : 0, "host" : nodes[0], "priority" : 3},
                           {"_id" : 1, "host" : nodes[1]},
                           {"_id" : 2, "host" : nodes[2], "arbiterOnly" : true}]});

     var primary = replSet.getPrimary();
     assert.eq(primary.host, nodes[0], "primary assumed to be node 0");
     assert.writeOK(primary.getDB(name).foo.insert({x: 1}, {w: 2, wtimeout:10000}));
     replSet.awaitReplication();

     jsTestLog("Sleeping 30 seconds so the SECONDARY will be considered electable");
     sleep(30000);

     // Run eval() in a separate thread to take the global write lock which would prevent stepdown
     // from completing if it failed to kill all running operations.
     jsTestLog("Running eval() to grab global write lock");
     var evalCmd = function() {
         db.eval(function() { sleep(60000); });
     }
     var evalRunner = startParallelShell(evalCmd, primary.port);

     jsTestLog("Confirming that eval() is running and has the global lock");
     assert.soon(function() {
                     var res = primary.getDB('admin').currentOp();
                     for (var index in res.inprog) {
                         var entry = res.inprog[index];
                         if (entry["query"] && entry["query"]["$eval"]) {
                             assert.eq("W", entry["locks"]["Global"]);
                             return true;
                         }
                     }
                     printjson(res);
                     return false;
                 }, "$eval never ran and grabbed the global write lock");

     jsTestLog("Stepping down");
     try {
         assert.commandWorked(primary.getDB('admin').runCommand({replSetStepDown: 30}));
     } catch (x) {
         // expected
     }

     jsTestLog("Waiting for former PRIMARY to become SECONDARY");
     replSet.waitForState(primary, replSet.SECONDARY, 30000);

     var newPrimary = replSet.getPrimary();
     assert.neq(primary, newPrimary, "SECONDARY did not become PRIMARY");

     var code = evalRunner();
     assert.neq(0, code); // Eval should have exited with an error due to being interrupted
 })();
