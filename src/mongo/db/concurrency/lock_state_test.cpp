/**
 *    Copyright (C) 2014 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include <algorithm>
#include <string>
#include <vector>

#include "mongo/config.h"
#include "mongo/db/concurrency/lock_manager_test_help.h"
#include "mongo/db/concurrency/locker.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/log.h"
#include "mongo/util/timer.h"

namespace mongo {

TEST(LockerImpl, LockNoConflict) {
    const ResourceId resId(RESOURCE_COLLECTION, "TestDB.collection"_sd);

    LockerImpl locker;
    locker.lockGlobal(MODE_IX);

    ASSERT(LOCK_OK == locker.lock(resId, MODE_X));

    ASSERT(locker.isLockHeldForMode(resId, MODE_X));
    ASSERT(locker.isLockHeldForMode(resId, MODE_S));

    ASSERT(locker.unlock(resId));

    ASSERT(locker.isLockHeldForMode(resId, MODE_NONE));

    locker.unlockGlobal();
}

TEST(LockerImpl, ReLockNoConflict) {
    const ResourceId resId(RESOURCE_COLLECTION, "TestDB.collection"_sd);

    LockerImpl locker;
    locker.lockGlobal(MODE_IX);

    ASSERT(LOCK_OK == locker.lock(resId, MODE_S));
    ASSERT(LOCK_OK == locker.lock(resId, MODE_X));

    ASSERT(!locker.unlock(resId));
    ASSERT(locker.isLockHeldForMode(resId, MODE_X));

    ASSERT(locker.unlock(resId));
    ASSERT(locker.isLockHeldForMode(resId, MODE_NONE));

    ASSERT(locker.unlockGlobal());
}

TEST(LockerImpl, ConflictWithTimeout) {
    const ResourceId resId(RESOURCE_COLLECTION, "TestDB.collection"_sd);

    LockerImpl locker1;
    ASSERT(LOCK_OK == locker1.lockGlobal(MODE_IX));
    ASSERT(LOCK_OK == locker1.lock(resId, MODE_X));

    LockerImpl locker2;
    ASSERT(LOCK_OK == locker2.lockGlobal(MODE_IX));
    ASSERT(LOCK_TIMEOUT == locker2.lock(resId, MODE_S, Date_t::now()));

    ASSERT(locker2.getLockMode(resId) == MODE_NONE);

    ASSERT(locker1.unlock(resId));

    ASSERT(locker1.unlockGlobal());
    ASSERT(locker2.unlockGlobal());
}

TEST(LockerImpl, ConflictUpgradeWithTimeout) {
    const ResourceId resId(RESOURCE_COLLECTION, "TestDB.collection"_sd);

    LockerImpl locker1;
    ASSERT(LOCK_OK == locker1.lockGlobal(MODE_IS));
    ASSERT(LOCK_OK == locker1.lock(resId, MODE_S));

    LockerImpl locker2;
    ASSERT(LOCK_OK == locker2.lockGlobal(MODE_IS));
    ASSERT(LOCK_OK == locker2.lock(resId, MODE_S));

    // Try upgrading locker 1, which should block and timeout
    ASSERT(LOCK_TIMEOUT == locker1.lock(resId, MODE_X, Date_t::now() + Milliseconds(1)));

    locker1.unlockGlobal();
    locker2.unlockGlobal();
}


TEST(LockerImpl, ReadTransaction) {
    LockerImpl locker;

    locker.lockGlobal(MODE_IS);
    locker.unlockGlobal();

    locker.lockGlobal(MODE_IX);
    locker.unlockGlobal();

    locker.lockGlobal(MODE_IX);
    locker.lockGlobal(MODE_IS);
    locker.unlockGlobal();
    locker.unlockGlobal();
}

/**
 * Test that saveLockerImpl works by examining the output.
 */
TEST(LockerImpl, saveAndRestoreGlobal) {
    Locker::LockSnapshot lockInfo;

    LockerImpl locker;

    // No lock requests made, no locks held.
    locker.saveLockStateAndUnlock(&lockInfo);
    ASSERT_EQUALS(0U, lockInfo.locks.size());

    // Lock the global lock, but just once.
    locker.lockGlobal(MODE_IX);

    // We've locked the global lock.  This should be reflected in the lockInfo.
    locker.saveLockStateAndUnlock(&lockInfo);
    ASSERT(!locker.isLocked());
    ASSERT_EQUALS(MODE_IX, lockInfo.globalMode);

    // Restore the lock(s) we had.
    locker.restoreLockState(lockInfo);

    ASSERT(locker.isLocked());
    ASSERT(locker.unlockGlobal());
}

/**
 * Test that we don't unlock when we have the global lock more than once.
 */
TEST(LockerImpl, saveAndRestoreGlobalAcquiredTwice) {
    Locker::LockSnapshot lockInfo;

    LockerImpl locker;

    // No lock requests made, no locks held.
    locker.saveLockStateAndUnlock(&lockInfo);
    ASSERT_EQUALS(0U, lockInfo.locks.size());

    // Lock the global lock.
    locker.lockGlobal(MODE_IX);
    locker.lockGlobal(MODE_IX);

    // This shouldn't actually unlock as we're in a nested scope.
    ASSERT(!locker.saveLockStateAndUnlock(&lockInfo));

    ASSERT(locker.isLocked());

    // We must unlockGlobal twice.
    ASSERT(!locker.unlockGlobal());
    ASSERT(locker.unlockGlobal());
}

/**
 * Tests that restoreLockerImpl works by locking a db and collection and saving + restoring.
 */
TEST(LockerImpl, saveAndRestoreDBAndCollection) {
    Locker::LockSnapshot lockInfo;

    LockerImpl locker;

    const ResourceId resIdDatabase(RESOURCE_DATABASE, "TestDB"_sd);
    const ResourceId resIdCollection(RESOURCE_COLLECTION, "TestDB.collection"_sd);

    // Lock some stuff.
    locker.lockGlobal(MODE_IX);
    ASSERT_EQUALS(LOCK_OK, locker.lock(resIdDatabase, MODE_IX));
    ASSERT_EQUALS(LOCK_OK, locker.lock(resIdCollection, MODE_X));
    locker.saveLockStateAndUnlock(&lockInfo);

    // Things shouldn't be locked anymore.
    ASSERT_EQUALS(MODE_NONE, locker.getLockMode(resIdDatabase));
    ASSERT_EQUALS(MODE_NONE, locker.getLockMode(resIdCollection));

    // Restore lock state.
    locker.restoreLockState(lockInfo);

    // Make sure things were re-locked.
    ASSERT_EQUALS(MODE_IX, locker.getLockMode(resIdDatabase));
    ASSERT_EQUALS(MODE_X, locker.getLockMode(resIdCollection));

    ASSERT(locker.unlockGlobal());
}

TEST(LockerImpl, DefaultLocker) {
    const ResourceId resId(RESOURCE_DATABASE, "TestDB"_sd);

    LockerImpl locker;
    ASSERT_EQUALS(LOCK_OK, locker.lockGlobal(MODE_IX));
    ASSERT_EQUALS(LOCK_OK, locker.lock(resId, MODE_X));

    // Make sure the flush lock IS NOT held
    Locker::LockerInfo info;
    locker.getLockerInfo(&info);
    ASSERT(!info.waitingResource.isValid());
    ASSERT_EQUALS(2U, info.locks.size());
    ASSERT_EQUALS(RESOURCE_GLOBAL, info.locks[0].resourceId.getType());
    ASSERT_EQUALS(resId, info.locks[1].resourceId);

    ASSERT(locker.unlockGlobal());
}

TEST(LockerImpl, CanceledDeadlockUnblocks) {
    const ResourceId db1(RESOURCE_DATABASE, "db1"_sd);
    const ResourceId db2(RESOURCE_DATABASE, "db2"_sd);

    LockerImpl locker1;
    LockerImpl locker2;
    LockerImpl locker3;

    ASSERT(LOCK_OK == locker1.lockGlobal(MODE_IX));
    ASSERT(LOCK_OK == locker1.lock(db1, MODE_S));

    ASSERT(LOCK_OK == locker2.lockGlobal(MODE_IX));
    ASSERT(LOCK_OK == locker2.lock(db2, MODE_X));

    // Set up locker1 and locker2 for deadlock
    ASSERT(LOCK_WAITING == locker1.lockBegin(nullptr, db2, MODE_X));
    ASSERT(LOCK_WAITING == locker2.lockBegin(nullptr, db1, MODE_X));

    // Locker3 blocks behind locker 2
    ASSERT(LOCK_OK == locker3.lockGlobal(MODE_IX));
    ASSERT(LOCK_WAITING == locker3.lockBegin(nullptr, db1, MODE_S));

    // Detect deadlock, canceling our request
    ASSERT(
        LOCK_DEADLOCK ==
        locker2.lockComplete(db1, MODE_X, Date_t::now() + Milliseconds(1), /*checkDeadlock*/ true));

    // Now locker3 must be able to complete its request
    ASSERT(LOCK_OK ==
           locker3.lockComplete(
               db1, MODE_S, Date_t::now() + Milliseconds(1), /*checkDeadlock*/ false));

    // Locker1 still can't complete its request
    ASSERT(LOCK_TIMEOUT ==
           locker1.lockComplete(db2, MODE_X, Date_t::now() + Milliseconds(1), false));

    // Check ownership for db1
    ASSERT(locker1.getLockMode(db1) == MODE_S);
    ASSERT(locker2.getLockMode(db1) == MODE_NONE);
    ASSERT(locker3.getLockMode(db1) == MODE_S);

    // Check ownership for db2
    ASSERT(locker1.getLockMode(db2) == MODE_NONE);
    ASSERT(locker2.getLockMode(db2) == MODE_X);
    ASSERT(locker3.getLockMode(db2) == MODE_NONE);

    ASSERT(locker1.unlockGlobal());
    ASSERT(locker2.unlockGlobal());
    ASSERT(locker3.unlockGlobal());
}

TEST(LockerImpl, SharedLocksShouldTwoPhaseLockIsTrue) {
    // Test that when setSharedLocksShouldTwoPhaseLock is true and we are in a WUOW, unlock on IS
    // and S locks are postponed until endWriteUnitOfWork() is called. Mode IX and X locks always
    // participate in two-phased locking, regardless of the setting.

    const ResourceId globalResId(RESOURCE_GLOBAL, ResourceId::SINGLETON_GLOBAL);
    const ResourceId resId1(RESOURCE_DATABASE, "TestDB1"_sd);
    const ResourceId resId2(RESOURCE_DATABASE, "TestDB2"_sd);
    const ResourceId resId3(RESOURCE_COLLECTION, "TestDB.collection3"_sd);
    const ResourceId resId4(RESOURCE_COLLECTION, "TestDB.collection4"_sd);

    LockerImpl locker;
    locker.setSharedLocksShouldTwoPhaseLock(true);

    ASSERT_EQ(LOCK_OK, locker.lockGlobal(MODE_IS));
    ASSERT_EQ(locker.getLockMode(globalResId), MODE_IS);

    ASSERT_EQ(LOCK_OK, locker.lock(resId1, MODE_IS));
    ASSERT_EQ(LOCK_OK, locker.lock(resId2, MODE_IX));
    ASSERT_EQ(LOCK_OK, locker.lock(resId3, MODE_S));
    ASSERT_EQ(LOCK_OK, locker.lock(resId4, MODE_X));
    ASSERT_EQ(locker.getLockMode(resId1), MODE_IS);
    ASSERT_EQ(locker.getLockMode(resId2), MODE_IX);
    ASSERT_EQ(locker.getLockMode(resId3), MODE_S);
    ASSERT_EQ(locker.getLockMode(resId4), MODE_X);

    locker.beginWriteUnitOfWork();

    ASSERT_FALSE(locker.unlock(resId1));
    ASSERT_FALSE(locker.unlock(resId2));
    ASSERT_FALSE(locker.unlock(resId3));
    ASSERT_FALSE(locker.unlock(resId4));
    ASSERT_EQ(locker.getLockMode(resId1), MODE_IS);
    ASSERT_EQ(locker.getLockMode(resId2), MODE_IX);
    ASSERT_EQ(locker.getLockMode(resId3), MODE_S);
    ASSERT_EQ(locker.getLockMode(resId4), MODE_X);

    ASSERT_FALSE(locker.unlockGlobal());
    ASSERT_EQ(locker.getLockMode(globalResId), MODE_IS);

    locker.endWriteUnitOfWork();

    ASSERT_EQ(locker.getLockMode(resId1), MODE_NONE);
    ASSERT_EQ(locker.getLockMode(resId2), MODE_NONE);
    ASSERT_EQ(locker.getLockMode(resId3), MODE_NONE);
    ASSERT_EQ(locker.getLockMode(resId4), MODE_NONE);
    ASSERT_EQ(locker.getLockMode(globalResId), MODE_NONE);
}

TEST(LockerImpl, ModeIXAndXLockParticipatesInTwoPhaseLocking) {
    // Unlock on mode IX and X locks during a WUOW should always be postponed until
    // endWriteUnitOfWork() is called. Mode IS and S locks should unlock immediately.

    const ResourceId globalResId(RESOURCE_GLOBAL, ResourceId::SINGLETON_GLOBAL);
    const ResourceId resId1(RESOURCE_DATABASE, "TestDB1"_sd);
    const ResourceId resId2(RESOURCE_DATABASE, "TestDB2"_sd);
    const ResourceId resId3(RESOURCE_COLLECTION, "TestDB.collection3"_sd);
    const ResourceId resId4(RESOURCE_COLLECTION, "TestDB.collection4"_sd);

    LockerImpl locker;

    ASSERT_EQ(LOCK_OK, locker.lockGlobal(MODE_IX));
    ASSERT_EQ(locker.getLockMode(globalResId), MODE_IX);

    ASSERT_EQ(LOCK_OK, locker.lock(resId1, MODE_IS));
    ASSERT_EQ(LOCK_OK, locker.lock(resId2, MODE_IX));
    ASSERT_EQ(LOCK_OK, locker.lock(resId3, MODE_S));
    ASSERT_EQ(LOCK_OK, locker.lock(resId4, MODE_X));
    ASSERT_EQ(locker.getLockMode(resId1), MODE_IS);
    ASSERT_EQ(locker.getLockMode(resId2), MODE_IX);
    ASSERT_EQ(locker.getLockMode(resId3), MODE_S);
    ASSERT_EQ(locker.getLockMode(resId4), MODE_X);

    locker.beginWriteUnitOfWork();

    ASSERT_TRUE(locker.unlock(resId1));
    ASSERT_FALSE(locker.unlock(resId2));
    ASSERT_TRUE(locker.unlock(resId3));
    ASSERT_FALSE(locker.unlock(resId4));
    ASSERT_EQ(locker.getLockMode(resId1), MODE_NONE);
    ASSERT_EQ(locker.getLockMode(resId2), MODE_IX);
    ASSERT_EQ(locker.getLockMode(resId3), MODE_NONE);
    ASSERT_EQ(locker.getLockMode(resId4), MODE_X);

    ASSERT_FALSE(locker.unlockGlobal());
    ASSERT_EQ(locker.getLockMode(globalResId), MODE_IX);

    locker.endWriteUnitOfWork();

    ASSERT_EQ(locker.getLockMode(resId2), MODE_NONE);
    ASSERT_EQ(locker.getLockMode(resId4), MODE_NONE);
    ASSERT_EQ(locker.getLockMode(globalResId), MODE_NONE);
}

TEST(LockerImpl, OverrideLockRequestTimeout) {
    const ResourceId resIdFirstDB(RESOURCE_DATABASE, "FirstDB"_sd);
    const ResourceId resIdSecondDB(RESOURCE_DATABASE, "SecondDB"_sd);

    LockerImpl locker1;
    LockerImpl locker2;

    // Set up locker2 to override lock requests' provided timeout if greater than 1000 milliseconds.
    locker2.setMaxLockTimeout(Milliseconds(1000));

    ASSERT_EQ(LOCK_OK, locker1.lockGlobal(MODE_IX));
    ASSERT_EQ(LOCK_OK, locker2.lockGlobal(MODE_IX));

    // locker1 acquires FirstDB under an exclusive lock.
    ASSERT_EQ(LOCK_OK, locker1.lock(resIdFirstDB, MODE_X));
    ASSERT_TRUE(locker1.isLockHeldForMode(resIdFirstDB, MODE_X));

    // locker2's attempt to acquire FirstDB with unlimited wait time should timeout after 1000
    // milliseconds and throw because _maxLockRequestTimeout is set to 1000 milliseconds.
    ASSERT_THROWS_CODE(locker2.lock(resIdFirstDB, MODE_X, Date_t::max()),
                       AssertionException,
                       ErrorCodes::LockTimeout);

    // locker2's attempt to acquire an uncontested lock should still succeed normally.
    ASSERT_EQ(LOCK_OK, locker2.lock(resIdSecondDB, MODE_X));

    ASSERT_TRUE(locker1.unlock(resIdFirstDB));
    ASSERT_TRUE(locker1.isLockHeldForMode(resIdFirstDB, MODE_NONE));
    ASSERT_TRUE(locker2.unlock(resIdSecondDB));
    ASSERT_TRUE(locker2.isLockHeldForMode(resIdSecondDB, MODE_NONE));

    ASSERT(locker1.unlockGlobal());
    ASSERT(locker2.unlockGlobal());
}

TEST(LockerImpl, DoNotWaitForLockAcquisition) {
    const ResourceId resIdFirstDB(RESOURCE_DATABASE, "FirstDB"_sd);
    const ResourceId resIdSecondDB(RESOURCE_DATABASE, "SecondDB"_sd);

    LockerImpl locker1;
    LockerImpl locker2;

    // Set up locker2 to immediately return if a lock is unavailable, regardless of supplied
    // deadlines in the lock request.
    locker2.setMaxLockTimeout(Milliseconds(0));

    ASSERT_EQ(LOCK_OK, locker1.lockGlobal(MODE_IX));
    ASSERT_EQ(LOCK_OK, locker2.lockGlobal(MODE_IX));

    // locker1 acquires FirstDB under an exclusive lock.
    ASSERT_EQ(LOCK_OK, locker1.lock(resIdFirstDB, MODE_X));
    ASSERT_TRUE(locker1.isLockHeldForMode(resIdFirstDB, MODE_X));

    // locker2's attempt to acquire FirstDB with unlimited wait time should fail immediately and
    // throw because _maxLockRequestTimeout was set to 0.
    ASSERT_THROWS_CODE(locker2.lock(resIdFirstDB, MODE_X, Date_t::max()),
                       AssertionException,
                       ErrorCodes::LockTimeout);

    // locker2's attempt to acquire an uncontested lock should still succeed normally.
    ASSERT_EQ(LOCK_OK, locker2.lock(resIdSecondDB, MODE_X));

    ASSERT_TRUE(locker1.unlock(resIdFirstDB));
    ASSERT_TRUE(locker1.isLockHeldForMode(resIdFirstDB, MODE_NONE));
    ASSERT_TRUE(locker2.unlock(resIdSecondDB));
    ASSERT_TRUE(locker2.isLockHeldForMode(resIdSecondDB, MODE_NONE));

    ASSERT(locker1.unlockGlobal());
    ASSERT(locker2.unlockGlobal());
}

namespace {
/**
 * Helper function to determine if 'lockerInfo' contains a lock with ResourceId 'resourceId' and
 * lock mode 'mode' within 'lockerInfo.locks'.
 */
bool lockerInfoContainsLock(const Locker::LockerInfo& lockerInfo,
                            const ResourceId& resourceId,
                            const LockMode& mode) {
    return (1U == std::count_if(lockerInfo.locks.begin(),
                                lockerInfo.locks.end(),
                                [&resourceId, &mode](const Locker::OneLock& lock) {
                                    return lock.resourceId == resourceId && lock.mode == mode;
                                }));
}
}  // namespace

TEST(LockerImpl, GetLockerInfoShouldReportHeldLocks) {
    const ResourceId globalId(RESOURCE_GLOBAL, ResourceId::SINGLETON_GLOBAL);
    const ResourceId dbId(RESOURCE_DATABASE, "TestDB"_sd);
    const ResourceId collectionId(RESOURCE_COLLECTION, "TestDB.collection"_sd);

    // Take an exclusive lock on the collection.
    LockerImpl locker;
    ASSERT_EQ(LOCK_OK, locker.lockGlobal(MODE_IX));
    ASSERT_EQ(LOCK_OK, locker.lock(dbId, MODE_IX));
    ASSERT_EQ(LOCK_OK, locker.lock(collectionId, MODE_X));

    // Assert it shows up in the output of getLockerInfo().
    Locker::LockerInfo lockerInfo;
    locker.getLockerInfo(&lockerInfo);

    ASSERT(lockerInfoContainsLock(lockerInfo, globalId, MODE_IX));
    ASSERT(lockerInfoContainsLock(lockerInfo, dbId, MODE_IX));
    ASSERT(lockerInfoContainsLock(lockerInfo, collectionId, MODE_X));
    ASSERT_EQ(3U, lockerInfo.locks.size());

    ASSERT(locker.unlock(collectionId));
    ASSERT(locker.unlock(dbId));
    ASSERT(locker.unlockGlobal());
}

TEST(LockerImpl, GetLockerInfoShouldReportPendingLocks) {
    const ResourceId globalId(RESOURCE_GLOBAL, ResourceId::SINGLETON_GLOBAL);
    const ResourceId dbId(RESOURCE_DATABASE, "TestDB"_sd);
    const ResourceId collectionId(RESOURCE_COLLECTION, "TestDB.collection"_sd);

    // Take an exclusive lock on the collection.
    LockerImpl successfulLocker;
    ASSERT_EQ(LOCK_OK, successfulLocker.lockGlobal(MODE_IX));
    ASSERT_EQ(LOCK_OK, successfulLocker.lock(dbId, MODE_IX));
    ASSERT_EQ(LOCK_OK, successfulLocker.lock(collectionId, MODE_X));

    // Now attempt to get conflicting locks.
    LockerImpl conflictingLocker;
    ASSERT_EQ(LOCK_OK, conflictingLocker.lockGlobal(MODE_IS));
    ASSERT_EQ(LOCK_OK, conflictingLocker.lock(dbId, MODE_IS));
    ASSERT_EQ(LOCK_WAITING, conflictingLocker.lockBegin(nullptr, collectionId, MODE_IS));

    // Assert the held locks show up in the output of getLockerInfo().
    Locker::LockerInfo lockerInfo;
    conflictingLocker.getLockerInfo(&lockerInfo);
    ASSERT(lockerInfoContainsLock(lockerInfo, globalId, MODE_IS));
    ASSERT(lockerInfoContainsLock(lockerInfo, dbId, MODE_IS));
    ASSERT(lockerInfoContainsLock(lockerInfo, collectionId, MODE_IS));
    ASSERT_EQ(3U, lockerInfo.locks.size());

    // Assert it reports that it is waiting for the collection lock.
    ASSERT_EQ(collectionId, lockerInfo.waitingResource);

    // Make sure it no longer reports waiting once unlocked.
    ASSERT(successfulLocker.unlock(collectionId));
    ASSERT(successfulLocker.unlock(dbId));
    ASSERT(successfulLocker.unlockGlobal());

    const bool checkDeadlock = false;
    ASSERT_EQ(LOCK_OK,
              conflictingLocker.lockComplete(collectionId, MODE_IS, Date_t::now(), checkDeadlock));

    conflictingLocker.getLockerInfo(&lockerInfo);
    ASSERT_FALSE(lockerInfo.waitingResource.isValid());

    ASSERT(conflictingLocker.unlock(collectionId));
    ASSERT(conflictingLocker.unlock(dbId));
    ASSERT(conflictingLocker.unlockGlobal());
}

TEST(LockerImpl, ReaquireLockPendingUnlock) {
    const ResourceId resId(RESOURCE_COLLECTION, "TestDB.collection"_sd);

    LockerImpl locker;
    locker.lockGlobal(MODE_IS);

    ASSERT_EQ(LOCK_OK, locker.lock(resId, MODE_X));
    ASSERT_TRUE(locker.isLockHeldForMode(resId, MODE_X));

    locker.beginWriteUnitOfWork();

    ASSERT_FALSE(locker.unlock(resId));
    ASSERT_TRUE(locker.isLockHeldForMode(resId, MODE_X));
    ASSERT(locker.numResourcesToUnlockAtEndUnitOfWorkForTest() == 1);
    ASSERT(locker.getRequestsForTest().find(resId).objAddr()->unlockPending == 1);

    // Reacquire lock pending unlock.
    ASSERT_EQ(LOCK_OK, locker.lock(resId, MODE_X));
    ASSERT(locker.numResourcesToUnlockAtEndUnitOfWorkForTest() == 0);
    ASSERT(locker.getRequestsForTest().find(resId).objAddr()->unlockPending == 0);

    locker.endWriteUnitOfWork();

    ASSERT_TRUE(locker.isLockHeldForMode(resId, MODE_X));

    locker.unlockGlobal();
}

TEST(LockerImpl, AcquireLockPendingUnlockWithCoveredMode) {
    const ResourceId resId(RESOURCE_COLLECTION, "TestDB.collection"_sd);

    LockerImpl locker;
    locker.lockGlobal(MODE_IS);

    ASSERT_EQ(LOCK_OK, locker.lock(resId, MODE_X));
    ASSERT_TRUE(locker.isLockHeldForMode(resId, MODE_X));

    locker.beginWriteUnitOfWork();

    ASSERT_FALSE(locker.unlock(resId));
    ASSERT_TRUE(locker.isLockHeldForMode(resId, MODE_X));
    ASSERT(locker.numResourcesToUnlockAtEndUnitOfWorkForTest() == 1);
    ASSERT(locker.getRequestsForTest().find(resId).objAddr()->unlockPending == 1);

    // Attempt to lock the resource with a mode that is covered by the existing mode.
    ASSERT_EQ(LOCK_OK, locker.lock(resId, MODE_IX));
    ASSERT(locker.numResourcesToUnlockAtEndUnitOfWorkForTest() == 0);
    ASSERT(locker.getRequestsForTest().find(resId).objAddr()->unlockPending == 0);

    locker.endWriteUnitOfWork();

    ASSERT_TRUE(locker.isLockHeldForMode(resId, MODE_IX));

    locker.unlockGlobal();
}

TEST(LockerImpl, ConvertLockPendingUnlock) {
    const ResourceId resId(RESOURCE_COLLECTION, "TestDB.collection"_sd);

    LockerImpl locker;
    locker.lockGlobal(MODE_IS);

    ASSERT_EQ(LOCK_OK, locker.lock(resId, MODE_IX));
    ASSERT_TRUE(locker.isLockHeldForMode(resId, MODE_IX));

    locker.beginWriteUnitOfWork();

    ASSERT_FALSE(locker.unlock(resId));
    ASSERT_TRUE(locker.isLockHeldForMode(resId, MODE_IX));
    ASSERT(locker.numResourcesToUnlockAtEndUnitOfWorkForTest() == 1);
    ASSERT(locker.getRequestsForTest().find(resId).objAddr()->unlockPending == 1);

    // Convert lock pending unlock.
    ASSERT_EQ(LOCK_OK, locker.lock(resId, MODE_X));
    ASSERT(locker.numResourcesToUnlockAtEndUnitOfWorkForTest() == 1);
    ASSERT(locker.getRequestsForTest().find(resId).objAddr()->unlockPending == 1);

    locker.endWriteUnitOfWork();

    ASSERT(locker.numResourcesToUnlockAtEndUnitOfWorkForTest() == 0);
    ASSERT(locker.getRequestsForTest().find(resId).objAddr()->unlockPending == 0);
    ASSERT_TRUE(locker.isLockHeldForMode(resId, MODE_X));

    locker.unlockGlobal();
}

TEST(LockerImpl, ConvertLockPendingUnlockAndUnlock) {
    const ResourceId resId(RESOURCE_COLLECTION, "TestDB.collection"_sd);

    LockerImpl locker;
    locker.lockGlobal(MODE_IS);

    ASSERT_EQ(LOCK_OK, locker.lock(resId, MODE_IX));
    ASSERT_TRUE(locker.isLockHeldForMode(resId, MODE_IX));

    locker.beginWriteUnitOfWork();

    ASSERT_FALSE(locker.unlock(resId));
    ASSERT_TRUE(locker.isLockHeldForMode(resId, MODE_IX));
    ASSERT(locker.numResourcesToUnlockAtEndUnitOfWorkForTest() == 1);
    ASSERT(locker.getRequestsForTest().find(resId).objAddr()->unlockPending == 1);

    // Convert lock pending unlock.
    ASSERT_EQ(LOCK_OK, locker.lock(resId, MODE_X));
    ASSERT(locker.numResourcesToUnlockAtEndUnitOfWorkForTest() == 1);
    ASSERT(locker.getRequestsForTest().find(resId).objAddr()->unlockPending == 1);

    // Unlock the lock conversion.
    ASSERT_FALSE(locker.unlock(resId));
    ASSERT(locker.numResourcesToUnlockAtEndUnitOfWorkForTest() == 1);
    ASSERT(locker.getRequestsForTest().find(resId).objAddr()->unlockPending == 2);

    locker.endWriteUnitOfWork();

    ASSERT(locker.numResourcesToUnlockAtEndUnitOfWorkForTest() == 0);
    ASSERT(locker.getRequestsForTest().find(resId).finished());
    ASSERT_TRUE(locker.isLockHeldForMode(resId, MODE_NONE));

    locker.unlockGlobal();
}

TEST(LockerImpl, TempName) {  // todo test name
    const ResourceId globalResId(RESOURCE_GLOBAL, ResourceId::SINGLETON_GLOBAL);
    const ResourceId resIdDatabase(RESOURCE_DATABASE, "TestDB"_sd);
    const ResourceId resIdCollection1(RESOURCE_COLLECTION, "TestDB.collection1"_sd);
    const ResourceId resIdCollection2(RESOURCE_COLLECTION, "TestDB.collection2"_sd);
    const ResourceId resIdCollection3(RESOURCE_COLLECTION, "TestDB.collection2"_sd);

    Locker::LockSnapshot lockInfo1, lockInfo2, lockInfo3;
    // TODO // LockerImpl stepUpLocker, txnLocker1, txnLocker2, txnLocker3, randomOpLocker1,
    // randomOpLocker2;
    LockerImpl stepUpLocker;
    LockerImpl txnLocker1;
    LockerImpl txnLocker2;
    LockerImpl txnLocker3;
    LockerImpl randomOpLocker1;
    LockerImpl randomOpLocker2;
    OperationContextNoop opCtx1, opCtx2, opCtx3;
    ON_BLOCK_EXIT([&] {
        // clean up locks on test completion.
        stepUpLocker.unlockGlobal();
        txnLocker1.unlockGlobal();
        txnLocker2.unlockGlobal();
        txnLocker3.unlockGlobal();
        randomOpLocker1.unlockGlobal();
        randomOpLocker2.unlockGlobal();
    });

    // Take some locks
    txnLocker1.lockGlobal(&opCtx1, MODE_IX);
    ASSERT_EQUALS(LOCK_OK, txnLocker1.lock(&opCtx1, resIdDatabase, MODE_IX));
    ASSERT_EQUALS(LOCK_OK, txnLocker1.lock(&opCtx1, resIdCollection1, MODE_IX));
    ASSERT_EQUALS(LOCK_OK, txnLocker1.lock(&opCtx1, resIdCollection2, MODE_IX));

    txnLocker2.lockGlobal(&opCtx2, MODE_IX);
    ASSERT_EQUALS(LOCK_OK, txnLocker2.lock(&opCtx2, resIdDatabase, MODE_IX));
    ASSERT_EQUALS(LOCK_OK, txnLocker2.lock(&opCtx2, resIdCollection2, MODE_IX));

    txnLocker3.lockGlobal(&opCtx3, MODE_IX);
    ASSERT_EQUALS(LOCK_OK, txnLocker3.lock(&opCtx3, resIdDatabase, MODE_IX));
    ASSERT_EQUALS(LOCK_OK, txnLocker3.lock(&opCtx3, resIdCollection3, MODE_IX));

    // Enqueue request for global X lock in stepUpLocker.
    ASSERT_EQUALS(LockResult::LOCK_WAITING, stepUpLocker.lockGlobalBegin(MODE_X, Date_t::max()));

    // Enqueue a lock request behind the pending global X request to ensure that it gets granted
    // later when the global X lock is released.
    ASSERT_EQUALS(LockResult::LOCK_WAITING,
                  randomOpLocker1.lockGlobalBegin(MODE_IS, Date_t::max()));

    // Yield locks on all txn threads.
    txnLocker1.saveLockStateAndUnlock(&lockInfo1);
    txnLocker2.saveLockStateAndUnlock(&lockInfo2);
    txnLocker3.saveLockStateAndUnlock(&lockInfo3);

    // Ensure that stepUpLocker is now able to acquire the global X lock.
    ASSERT_EQUALS(LockResult::LOCK_OK, stepUpLocker.lockGlobalComplete(Date_t::max()));

    // Enqueue a lock request behind the global X lock to ensure that it gets granted
    // later when the global X lock is released.
    ASSERT_EQUALS(LockResult::LOCK_WAITING,
                  randomOpLocker2.lockGlobalBegin(MODE_IS, Date_t::max()));

    // Atomically release the global X lock from stepUpLocker and restore the lock state for the txn
    // threads. TODO update comment
    LockHead tempLockHead;
    tempLockHead.initNew(globalResId);

    txnLocker1.restoreLockStateWithTemporaryGlobalLockHead(&opCtx1, lockInfo1, &tempLockHead);
    txnLocker2.restoreLockStateWithTemporaryGlobalLockHead(&opCtx2, lockInfo2, &tempLockHead);
    txnLocker3.restoreLockStateWithTemporaryGlobalLockHead(&opCtx3, lockInfo3, &tempLockHead);

    // Check that the global lock state was restored successfully into the tempLockHead.
    invariant(!tempLockHead.grantedList.empty());
    invariant(tempLockHead.conflictList.empty());
    invariant(tempLockHead.grantedCounts[MODE_IX] == 3);
    invariant(tempLockHead.grantedCounts[MODE_X] == 0);

    stepUpLocker.replaceGlobalLockStateWithTemporaryGlobalLockHead(&tempLockHead);

    // Make sure things were re-locked.
    ASSERT_EQUALS(MODE_NONE, stepUpLocker.getLockMode(globalResId));

    ASSERT_EQUALS(MODE_IX, txnLocker1.getLockMode(globalResId));
    ASSERT_EQUALS(MODE_IX, txnLocker1.getLockMode(resIdDatabase));
    ASSERT_EQUALS(MODE_IX, txnLocker1.getLockMode(resIdCollection1));
    ASSERT_EQUALS(MODE_IX, txnLocker1.getLockMode(resIdCollection2));

    ASSERT_EQUALS(MODE_IX, txnLocker2.getLockMode(globalResId));
    ASSERT_EQUALS(MODE_IX, txnLocker2.getLockMode(resIdDatabase));
    ASSERT_EQUALS(MODE_IX, txnLocker2.getLockMode(resIdCollection2));

    ASSERT_EQUALS(MODE_IX, txnLocker3.getLockMode(globalResId));
    ASSERT_EQUALS(MODE_IX, txnLocker3.getLockMode(resIdDatabase));
    ASSERT_EQUALS(MODE_IX, txnLocker3.getLockMode(resIdCollection3));

    // Make sure the pending global lock requests got granted when the global X lock was released.
    ASSERT_EQUALS(LockResult::LOCK_OK, randomOpLocker1.lockGlobalComplete(Date_t::now()));
    ASSERT_EQUALS(LockResult::LOCK_OK, randomOpLocker2.lockGlobalComplete(Date_t::now()));


    std::cout << "AAAA!!!!! TEST "
                 "PASSED!!!!@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
                 "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
              << std::endl;
}
}  // namespace mongo
