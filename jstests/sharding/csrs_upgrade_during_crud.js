/**
 * This test performs an upgrade from SCCC config servers to CSRS config servers while various CRUD
 * operations are taking place, and verifies that the CRUD operations continue to work.
 *
 * This test restarts nodes and expects the data to still be present.
 * @tags: [requires_persistence]
 */

load("jstests/libs/csrs_upgrade_util.js");

(function() {
    "use strict";

    var controller = new CSRSUpgradeController();


     jsTestLog("#####" + controller.getTestDBName());
//    controller.setupSCCCCluster();

    

    // insert initial data
}());
