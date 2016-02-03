/**
*
*/

load("jstests/replsets/rslib.js");

// todo move jsTestLogs to callers?
var CSRSUpgradeController = function() {
"use strict";

var testDBName = jsTestName();
var dataCollectionName = testDBName + ".data";
var csrsName = jsTestName() + "-csrs";
var numCsrsMembers;
var st;
var shardConfigs;
var csrsConfig;
var csrs;
var csrs0Opts

this.getTestDBName = function() {
    return testDBName;
}

this.getDataCollectionName = function() {
    return dataCollectionName;
}

this.getCSRSNodes = function() {
    return csrs;
}

this.getCSRSName = function() {
    return csrsName;
}

this.getMongosConfig = function() {
    var sconfig = Object.extend({}, st.s0.fullOptions, /* deep */ true);
    delete sconfig.port;
    return sconfig;
}

this.getMongos = function(n) {
    return st._mongos[n];
}

this.getShardName = function(n) {
    return shardConfigs[n]._id;
}

var _waitUntilMaster = function (dnode) {
    var isMasterReply;
    assert.soon(function () {
        isMasterReply = dnode.adminCommand({ismaster: 1});
        return isMasterReply.ismaster;
    }, function () {
        return "Expected " + dnode.name + " to respond ismaster:true, but got " +
            tojson(isMasterReply);
    });
};

this.setupSCCCCluster = function() {
    if (TestData.storageEngine == "wiredTiger" || TestData.storageEngine == "") {
        // TODO(schwerin): SERVER-19739 Support testing CSRS with storage engines other than wired
        // tiger, when such other storage engines support majority read concern.
        numCsrsMembers = 3;
    } else {
        numCsrsMembers = 4;
    }

    jsTest.log("Setting up SCCC sharded cluster")

    st = new ShardingTest({name: "csrsUpgrade",
                           mongos: 2,
                           rs: { nodes: 3 },
                           shards: 2,
                           nopreallocj: true,
                           other: {
                               sync: true,
                               enableBalancer: false,
                               useHostname: true
                           }});

    shardConfigs = st.s0.getCollection("config.shards").find().toArray();
    assert.eq(2, shardConfigs.length);

    jsTest.log("Enabling sharding on " + testDBName + " and making " + this.getShardName(0) +
               " the primary shard");
    assert.commandWorked(st.s0.adminCommand({enablesharding: testDBName}));
    st.ensurePrimaryShard(testDBName, this.getShardName(0));

    jsTest.log("Creating a sharded collection " + dataCollectionName);
    assert.commandWorked(st.s0.adminCommand({shardcollection: dataCollectionName,
                                             key: { _id: 1 }
                                            }));
}

this.restartFirstConfigAsReplSet = function() {
    jsTest.log("Restarting " + st.c0.name + " as a standalone replica set");
    csrsConfig = {
        _id: csrsName,
        version: 1,
        configsvr: true,
        members: [ { _id: 0, host: st.c0.name }]
    };
    assert.commandWorked(st.c0.adminCommand({replSetInitiate: csrsConfig}));
    csrs = [];
    csrs0Opts = Object.extend({}, st.c0.fullOptions, /* deep */ true);
    csrs0Opts.restart = true;  // Don't clean the data files from the old c0.
    csrs0Opts.replSet = csrsName;
    csrs0Opts.configsvrMode = "sccc";
    MongoRunner.stopMongod(st.c0);
    csrs.push(MongoRunner.runMongod(csrs0Opts));
    _waitUntilMaster(csrs[0]);
}

this.startNewCSRSNodes = function() {
    jsTest.log("Starting new CSRS nodes");
    for (var i = 1; i < numCsrsMembers; ++i) {
        csrs.push(MongoRunner.runMongod({replSet: csrsName,
                                         configsvr: "",
                                         storageEngine: "wiredTiger"
                                        }));
        csrsConfig.members.push({ _id: i, host: csrs[i].name, votes: 0, priority: 0 });
    }
    csrsConfig.version = 2;
    jsTest.log("Adding non-voting members to csrs set: " + tojson(csrsConfig));
    assert.commandWorked(csrs[0].adminCommand({replSetReconfig: csrsConfig}));
}

this.waitUntilConfigsCaughtUp = function() {
    waitUntilAllNodesCaughtUp(csrs);
}

this.shutdownOneSCCCNode = function() {
    // Only shut down one of the SCCC config servers to avoid any period without any config servers
    // online.
    jsTest.log("Shutting down third SCCC config server node");
    MongoRunner.stopMongod(st.c2);
}

this.allowAllCSRSNodesToVote = function() {
    csrsConfig.members.forEach(function (member) { member.votes = 1; member.priority = 1});
    csrsConfig.version = 3;
    jsTest.log("Allowing all csrs members to vote: " + tojson(csrsConfig));
    assert.commandWorked(csrs[0].adminCommand({replSetReconfig: csrsConfig}));
}

this.switchToCSRSMode = function() {
    jsTest.log("Restarting " + csrs[0].name + " in csrs mode");
    delete csrs0Opts.configsvrMode;
    try {
        csrs[0].adminCommand({replSetStepDown: 60});
    } catch (e) {} // Expected
    MongoRunner.stopMongod(csrs[0]);
    csrs[0] = MongoRunner.runMongod(csrs0Opts);
    var csrsStatus;
    assert.soon(function () {
        csrsStatus = csrs[0].adminCommand({replSetGetStatus: 1});
        if (csrsStatus.members[0].stateStr == "STARTUP" ||
            csrsStatus.members[0].stateStr == "STARTUP2" ||
            csrsStatus.members[0].stateStr == "RECOVERING") {
            // Make sure first node is fully online or else mongoses still in SCCC mode might not
            // find any node online to talk to.
            return false;
        }

        var i;
        for (i = 0; i < csrsStatus.members.length; ++i) {
            if (csrsStatus.members[i].name == csrs[0].name) {
                var supportsCommitted =
                    csrs[0].getDB("admin").serverStatus().storageEngine.supportsCommittedReads;
                var stateIsRemoved = csrsStatus.members[i].stateStr == "REMOVED";
                // If the storage engine supports committed reads, it shouldn't go into REMOVED
                // state, but if it does not then it should.
                if (supportsCommitted) {
                    assert(!stateIsRemoved);
                } else if (!stateIsRemoved) {
                    return false;
                }
            }
            if (csrsStatus.members[i].stateStr == "PRIMARY") {
                return csrs[i].adminCommand({ismaster: 1}).ismaster;
            }
        }
        return false;
    }, function() {
        return "No primary or non-WT engine not removed in " + tojson(csrsStatus);
    });
}

};