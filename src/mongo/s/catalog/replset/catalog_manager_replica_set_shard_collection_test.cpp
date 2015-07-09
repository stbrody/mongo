/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include <string>
#include <vector>

#include "mongo/client/remote_command_targeter_mock.h"
#include "mongo/db/commands.h"
#include "mongo/executor/network_interface_mock.h"
#include "mongo/executor/task_executor.h"
#include "mongo/s/catalog/dist_lock_manager_mock.h"
#include "mongo/s/catalog/replset/catalog_manager_replica_set.h"
#include "mongo/s/catalog/replset/catalog_manager_replica_set_test_fixture.h"
#include "mongo/s/catalog/type_changelog.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/catalog/type_database.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/chunk.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/s/write_ops/batched_command_request.h"
#include "mongo/s/write_ops/batched_command_response.h"
#include "mongo/stdx/chrono.h"
#include "mongo/stdx/future.h"
#include "mongo/util/log.h"

namespace mongo {
namespace {

using executor::NetworkInterfaceMock;
using executor::TaskExecutor;
using std::string;
using std::vector;
using stdx::chrono::milliseconds;
using unittest::assertGet;

class ShardCollectionTest : public CatalogManagerReplSetTestFixture {
public:
    void setUp() override {
        CatalogManagerReplSetTestFixture::setUp();
        configTargeter()->setFindHostReturnValue(configHost);
        getMessagingPort()->setRemote(clientHost);
    }

    void expectGetDatabase(const DatabaseType& expectedDb) {
        onFindCommand([&](const RemoteCommandRequest& request) {
            ASSERT_EQUALS(configHost, request.target);
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(DatabaseType::ConfigNS, nss.ns());

            auto query =
                assertGet(LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));

            ASSERT_EQ(DatabaseType::ConfigNS, query->ns());
            ASSERT_EQ(BSON(DatabaseType::name(expectedDb.getName())), query->getFilter());
            ASSERT_EQ(BSONObj(), query->getSort());
            ASSERT_EQ(1, query->getLimit().get());

            return vector<BSONObj>{expectedDb.toBSON()};
        });
    }

    // Intercepts network request to upsert a new chunk definition to the config.chunks collection.
    // Since the catalog manager cannot predict the epoch that will be assigned the new chunk,
    // returns the chunk version that is sent in the upsert.
    ChunkVersion expectCreateChunk(const ChunkType& expectedChunk) {
        ChunkVersion actualVersion;

        onCommand([&](const RemoteCommandRequest& request) {
            ASSERT_EQUALS(configHost, request.target);
            ASSERT_EQUALS("config", request.dbname);

            BatchedUpdateRequest actualBatchedUpdate;
            std::string errmsg;
            ASSERT_TRUE(actualBatchedUpdate.parseBSON(request.dbname, request.cmdObj, &errmsg));
            ASSERT_EQUALS(ChunkType::ConfigNS, actualBatchedUpdate.getNS().ns());
            auto updates = actualBatchedUpdate.getUpdates();
            ASSERT_EQUALS(1U, updates.size());
            auto update = updates.front();

            ASSERT_TRUE(update->getUpsert());
            ASSERT_FALSE(update->getMulti());
            ASSERT_EQUALS(BSON(ChunkType::name(expectedChunk.getName())), update->getQuery());

            BSONObj chunkObj = update->getUpdateExpr();
            ASSERT_EQUALS(expectedChunk.getName(), chunkObj["_id"].String());
            ASSERT_EQUALS(Timestamp(expectedChunk.getVersion().toLong()),
                          chunkObj[ChunkType::DEPRECATED_lastmod()].timestamp());
            // Can't check the chunk version's epoch b/c they won't match since it's a randomly
            // generated OID so just check that the field exists and is *a* OID.
            ASSERT_EQUALS(jstOID, chunkObj[ChunkType::DEPRECATED_epoch()].type());
            ASSERT_EQUALS(expectedChunk.getNS(), chunkObj[ChunkType::ns()].String());
            ASSERT_EQUALS(expectedChunk.getMin(), chunkObj[ChunkType::min()].Obj());
            ASSERT_EQUALS(expectedChunk.getMax(), chunkObj[ChunkType::max()].Obj());
            ASSERT_EQUALS(expectedChunk.getShard(), chunkObj[ChunkType::shard()].String());

            actualVersion = ChunkVersion::fromBSON(chunkObj);

            BatchedCommandResponse response;
            response.setOk(true);
            response.setNModified(1);

            return response.toBSON();
        });

        return actualVersion;
    }

    void expectReloadChunks(const std::string& ns, const vector<BSONObj>& chunks) {
        onFindCommand([&](const RemoteCommandRequest& request) {
            ASSERT_EQUALS(configHost, request.target);
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(nss.ns(), ChunkType::ConfigNS);

            auto query =
                assertGet(LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));
            BSONObj expectedQuery =
                BSON(ChunkType::ns(ns) << ChunkType::DEPRECATED_lastmod << GTE << Timestamp());
            BSONObj expectedSort = BSON(ChunkType::DEPRECATED_lastmod() << 1);

            ASSERT_EQ(ChunkType::ConfigNS, query->ns());
            ASSERT_EQ(expectedQuery, query->getFilter());
            ASSERT_EQ(expectedSort, query->getSort());
            ASSERT_FALSE(query->getLimit().is_initialized());

            return chunks;
        });
    }

    void expectUpdateCollection(const CollectionType& expectedCollection) {
        onCommand([&](const RemoteCommandRequest& request) {
            ASSERT_EQUALS(configHost, request.target);
            ASSERT_EQUALS("config", request.dbname);

            BatchedUpdateRequest actualBatchedUpdate;
            std::string errmsg;
            ASSERT_TRUE(actualBatchedUpdate.parseBSON(request.dbname, request.cmdObj, &errmsg));
            ASSERT_EQUALS(CollectionType::ConfigNS, actualBatchedUpdate.getNS().ns());
            auto updates = actualBatchedUpdate.getUpdates();
            ASSERT_EQUALS(1U, updates.size());
            auto update = updates.front();

            ASSERT_TRUE(update->getUpsert());
            ASSERT_FALSE(update->getMulti());
            ASSERT_EQUALS(BSON(CollectionType::fullNs(expectedCollection.getNs().toString())),
                          update->getQuery());
            ASSERT_EQUALS(expectedCollection.toBSON(), update->getUpdateExpr());

            BatchedCommandResponse response;
            response.setOk(true);
            response.setNModified(1);

            return response.toBSON();
        });
    }

    void expectReloadCollection(const CollectionType& collection) {
        onFindCommand([&](const RemoteCommandRequest& request) {
            ASSERT_EQUALS(configHost, request.target);
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(nss.ns(), CollectionType::ConfigNS);

            auto query =
                assertGet(LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));

            ASSERT_EQ(CollectionType::ConfigNS, query->ns());
            {
                BSONObjBuilder b;
                b.appendRegex(CollectionType::fullNs(),
                              string(str::stream() << "^" << collection.getNs().db() << "\\."));
                ASSERT_EQ(b.obj(), query->getFilter());
            }
            ASSERT_EQ(BSONObj(), query->getSort());

            return vector<BSONObj>{collection.toBSON()};
        });
    }

    void expectLoadNewestChunk(const string& ns, const ChunkType& chunk) {
        onFindCommand([&](const RemoteCommandRequest& request) {
            ASSERT_EQUALS(configHost, request.target);
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(nss.ns(), ChunkType::ConfigNS);

            auto query =
                assertGet(LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));
            BSONObj expectedQuery = BSON(ChunkType::ns(ns));
            BSONObj expectedSort = BSON(ChunkType::DEPRECATED_lastmod() << -1);

            ASSERT_EQ(ChunkType::ConfigNS, query->ns());
            ASSERT_EQ(expectedQuery, query->getFilter());
            ASSERT_EQ(expectedSort, query->getSort());
            ASSERT_EQ(1, query->getLimit().get());

            return vector<BSONObj>{chunk.toBSON()};
        });
    }

protected:
    const HostAndPort configHost{"TestHost1"};
    const HostAndPort clientHost{"client1:12345"};
};

TEST_F(ShardCollectionTest, shardCollectionDistLockFails) {
    distLock()->expectLock(
        [](StringData name,
           StringData whyMessage,
           milliseconds waitFor,
           milliseconds lockTryInterval) {
            ASSERT_EQUALS("test.foo", name);
            ASSERT_EQUALS("shardCollection", whyMessage);
        },
        Status(ErrorCodes::LockBusy, "lock already held"));

    ShardKeyPattern keyPattern(BSON("_id" << 1));
    ASSERT_EQUALS(
        ErrorCodes::LockBusy,
        catalogManager()->shardCollection(
            operationContext(), "test.foo", keyPattern, false, vector<BSONObj>{}, nullptr));
}

TEST_F(ShardCollectionTest, shardCollectionAnotherMongosSharding) {
    ShardType shard;
    shard.setName("shard0");
    shard.setHost("shardHost");

    setupShards(vector<ShardType>{shard});

    DatabaseType db;
    db.setName("db1");
    db.setPrimary(shard.getName());
    db.setSharded(true);

    string ns = "db1.foo";

    ShardKeyPattern keyPattern(BSON("_id" << 1));
    auto future = launchAsync([&] {
        ASSERT_EQUALS(ErrorCodes::AlreadyInitialized,
                      catalogManager()->shardCollection(
                          operationContext(), ns, keyPattern, false, vector<BSONObj>{}, nullptr));
    });

    distLock()->expectLock(
        [&](StringData name,
            StringData whyMessage,
            milliseconds waitFor,
            milliseconds lockTryInterval) {
            ASSERT_EQUALS(ns, name);
            ASSERT_EQUALS("shardCollection", whyMessage);
        },
        Status::OK());

    expectGetDatabase(db);

    // Report that chunks exist for the given collection, indicating that another mongos must have
    // already started sharding the collection.
    expectCount(configHost, NamespaceString(ChunkType::ConfigNS), BSON(ChunkType::ns(ns)), 1);

    future.timed_get(kFutureTimeout);
}

TEST_F(ShardCollectionTest, shardCollectionNoInitialChunksOrData) {
    // Initial setup
    const HostAndPort shardHost{"shardHost"};
    ShardType shard;
    shard.setName("shard0");
    shard.setHost(shardHost.toString());

    setupShards(vector<ShardType>{shard});

    RemoteCommandTargeterMock::get(grid.shardRegistry()->getShard(shard.getName())->getTargeter())
        ->setFindHostReturnValue(shardHost);

    string ns = "db1.foo";

    DatabaseType db;
    db.setName("db1");
    db.setPrimary(shard.getName());
    db.setSharded(true);

    ShardKeyPattern keyPattern(BSON("_id" << 1));

    ChunkType expectedChunk;
    expectedChunk.setName(Chunk::genID(ns, keyPattern.getKeyPattern().globalMin()));
    expectedChunk.setNS(ns);
    expectedChunk.setMin(keyPattern.getKeyPattern().globalMin());
    expectedChunk.setMax(keyPattern.getKeyPattern().globalMax());
    expectedChunk.setShard(shard.getName());
    {
        ChunkVersion expectedVersion;
        expectedVersion.incEpoch();
        expectedVersion.incMajor();
        expectedChunk.setVersion(expectedVersion);
    }

    // Now start actually sharding the collection.
    auto future = launchAsync([&] {
        ASSERT_OK(catalogManager()->shardCollection(
            operationContext(), ns, keyPattern, false, vector<BSONObj>{}, nullptr));
    });

    distLock()->expectLock(
        [&](StringData name,
            StringData whyMessage,
            milliseconds waitFor,
            milliseconds lockTryInterval) {
            ASSERT_EQUALS(ns, name);
            ASSERT_EQUALS("shardCollection", whyMessage);
        },
        Status::OK());

    expectGetDatabase(db);

    // Report that no chunks exist for the given collection
    expectCount(configHost, NamespaceString(ChunkType::ConfigNS), BSON(ChunkType::ns(ns)), 0);

    // Respond to write to change log
    {
        BSONObj logChangeDetail =
            BSON("shardKey" << keyPattern.toBSON() << "collection" << ns << "primary"
                            << shard.getName() + ":" + shard.getHost() << "initShards"
                            << BSONArray() << "numChunks" << 1);
        expectChangeLogCreate(configHost, BSON("ok" << 1));
        expectChangeLogInsert(
            configHost, clientHost.toString(), "shardCollection.start", ns, logChangeDetail);
    }

    // Report that no documents exist for the given collection on the target shard
    expectCount(shardHost, NamespaceString(ns), BSONObj(), 0);

    // Handle the write to create the initial chunk.
    ChunkVersion actualVersion = expectCreateChunk(expectedChunk);

    // Since the generated epoch OID will not match the one we initialized expectedChunk with,
    // update the stored version in expectedChunk so that it matches what was actually
    // written, to avoid problems relating to non-matching epochs down the road.
    expectedChunk.setVersion(actualVersion);

    // Handle the query to load the newly created chunk
    expectReloadChunks(ns, {expectedChunk.toBSON()});

    CollectionType expectedCollection;
    expectedCollection.setNs(NamespaceString(ns));
    expectedCollection.setEpoch(expectedChunk.getVersion().epoch());
    expectedCollection.setUpdatedAt(
        Date_t::fromMillisSinceEpoch(expectedChunk.getVersion().toLong()));
    expectedCollection.setKeyPattern(keyPattern.toBSON());
    expectedCollection.setUnique(false);

    // Handle the update to the collection entry in config.collectinos.
    expectUpdateCollection(expectedCollection);

    // Respond to various requests for reloading parts of the chunk/collection metadata.
    expectGetDatabase(db);
    expectGetDatabase(db);
    expectReloadCollection(expectedCollection);
    expectReloadChunks(ns, {expectedChunk.toBSON()});
    expectLoadNewestChunk(ns, expectedChunk);

    // Respond to request to write final changelog entry indicating success.
    expectChangeLogInsert(configHost,
                          clientHost.toString(),
                          "shardCollection",
                          ns,
                          BSON("version"
                               << ""));

    future.timed_get(kFutureTimeout);
}

}  // namespace
}  // namespace mongo
