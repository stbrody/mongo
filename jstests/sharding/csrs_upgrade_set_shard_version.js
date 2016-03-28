/**
 * This test performs an upgrade from SCCC config servers to CSRS config servers.
 *
 * Along the way, it confirms that the config servers always offer the
 * ability to read metadata, and checks that metadata is writable or
 * unwritable as appropriate at certain steps in the process.
 *
 * During the setup phase, a new sharded cluster is created with SCCC
 * config servers and a single sharded collection with documents on
 * each of two shards.
 *
 * During the upgrade phase, chunks are split to confirm the
 * availability or unavailability of metadata writes, and
 * config.version is read to confirm the availability of metadata
 * reads.
 *
 * This test restarts nodes and expects the data to still be present.
 * @tags: [requires_persistence]
 */

load("jstests/libs/csrs_upgrade_util.js");

var st;
(function() {
    "use strict";

    var coordinator = new CSRSUpgradeCoordinator();
    coordinator.setupSCCCCluster();

    assert.commandWorked(coordinator.getMongos(0).adminCommand(
        {split: coordinator.getShardedCollectionName(), middle: {_id: 0}}));

    assert.commandWorked(coordinator.getMongos(0).adminCommand({
        moveChunk: coordinator.getShardedCollectionName(),
        find: {_id: 0},
        to: coordinator.getShardName(1)
    }));

    jsTest.log("Inserting data into " + coordinator.getShardedCollectionName());
    coordinator.getMongos(1)
        .getCollection(coordinator.getShardedCollectionName())
        .insert((function() {
            var result = [];
            var i;
            for (i = -20; i < 20; ++i) {
                result.push({_id: i});
            }
            return result;
        }()));

    coordinator.restartFirstConfigAsReplSet();
    coordinator.startNewCSRSNodes();
    coordinator.waitUntilConfigsCaughtUp();
    coordinator.shutdownOneSCCCNode();
    coordinator.allowAllCSRSNodesToVote();
    coordinator.switchToCSRSMode();

    print("!!!0: " + coordinator.getShard(0).adminCommand('serverStatus').sharding.configsvrConnectionString);
    print("!!!1: " + coordinator.getShard(1).adminCommand('serverStatus').sharding.configsvrConnectionString);

    assert.commandWorked(coordinator.getMongos(0).adminCommand('flushRouterConfig'));

    assert.eq(40, coordinator.getMongos(0).getCollection(coordinator.getShardedCollectionName()).find().itcount());

    print("!!!2: " + coordinator.getShard(0).adminCommand('serverStatus').sharding.configsvrConnectionString);
    print("!!!3: " + coordinator.getShard(1).adminCommand('serverStatus').sharding.configsvrConnectionString);

}());
