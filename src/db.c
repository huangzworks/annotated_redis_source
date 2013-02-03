/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis.h"

#include <signal.h>
#include <ctype.h>

void SlotToKeyAdd(robj *key);
void SlotToKeyDel(robj *key);

/*-----------------------------------------------------------------------------
 * C-level DB API
 *----------------------------------------------------------------------------*/

/*
 * 在数据库 db 中查找给定 key 
 *
 * T = O(1)
 */
robj *lookupKey(redisDb *db, robj *key) {

    // 查找 key 对象
    dictEntry *de = dictFind(db->dict,key->ptr);

    // 存在？
    if (de) {
        // 取出 key 对应的值对象
        robj *val = dictGetVal(de);

        /* Update the access time for the aging algorithm.
         * Don't do it if we have a saving child, as this will trigger
         * a copy on write madness. */
        // 如果条件允许，那么更新 lru 时间
        if (server.rdb_child_pid == -1 && server.aof_child_pid == -1)
            val->lru = server.lruclock;

        return val;
    } else {
        // 不存在
        return NULL;
    }
}

/* 
 * 为进行读操作而读取数据库
 */
robj *lookupKeyRead(redisDb *db, robj *key) {

    robj *val;

    // 检查 key 是否过期，如果是的话，将它删除
    expireIfNeeded(db,key);

    // 查找 key ，并根据查找结果更新命中/不命中数
    val = lookupKey(db,key);
    if (val == NULL)
        server.stat_keyspace_misses++;
    else
        server.stat_keyspace_hits++;

    // 返回 key 的值
    return val;
}

/*
 * 为进行写操作而读取数据库
 *
 * 这个函数和 lookupKeyRead 的区别是
 * 这个函数不更新命中/不命中计数
 */
robj *lookupKeyWrite(redisDb *db, robj *key) {
    expireIfNeeded(db,key);
    return lookupKey(db,key);
}

/*
 * 为执行读取操作而从数据库中取出给定 key 的值。
 * 如果 key 不存在，向客户端发送信息 reply 。
 */
robj *lookupKeyReadOrReply(redisClient *c, robj *key, robj *reply) {
    robj *o = lookupKeyRead(c->db, key);
    if (!o) addReply(c,reply);
    return o;
}

/*
 * 为执行写入操作而从数据库中取出给定 key 的值。
 * 如果 key 不存在，向客户端发送信息 reply 。
 */
robj *lookupKeyWriteOrReply(redisClient *c, robj *key, robj *reply) {
    robj *o = lookupKeyWrite(c->db, key);
    if (!o) addReply(c,reply);
    return o;
}

/* Add the key to the DB. It's up to the caller to increment the reference
 * counte of the value if needed.
 *
 * 添加给定 key - value 对到数据库
 * 对 value 的引用计数处理由调用者决定
 *
 * The program is aborted if the key already exists. 
 *
 * 添加只在 key 不存在的情况下进行
 */
void dbAdd(redisDb *db, robj *key, robj *val) {
    // 键（字符串）
    sds copy = sdsdup(key->ptr);
    // 保存 键-值 对
    int retval = dictAdd(db->dict, copy, val);

    redisAssertWithInfo(NULL,key,retval == REDIS_OK);

    if (server.cluster_enabled) SlotToKeyAdd(key);
 }

/* Overwrite an existing key with a new value. Incrementing the reference
 * count of the new value is up to the caller.
 * This function does not modify the expire time of the existing key.
 *
 * 使用新值 value 覆盖原本 key 的旧值
 * 对 value 的引用计数处理由调用者决定
 *
 * The program is aborted if the key was not already present.
 *
 * 添加只在 key 存在的情况下进行
 */
void dbOverwrite(redisDb *db, robj *key, robj *val) {
    // 取出节点
    struct dictEntry *de = dictFind(db->dict,key->ptr);
    
    redisAssertWithInfo(NULL,key,de != NULL);

    // 用新值覆盖旧值
    dictReplace(db->dict, key->ptr, val);
}

/* High level Set operation. This function can be used in order to set
 * a key, whatever it was existing or not, to a new object.
 *
 * 高阶 set 操作。
 * 可以给一个 key 设置 value ，不管 key 是否存在。
 *
 * 1) The ref count of the value object is incremented.
 *    value 对象的引用计数已增加
 * 2) clients WATCHing for the destination key notified.
 *    如果有 key 正在被 WATCH ，那么告知客户端这个 key 已被修改
 * 3) The expire time of the key is reset (the key is made persistent).
 *    key 的过期时间（如果有的话）会被重置，将 key 变为持久化的
 */
void setKey(redisDb *db, robj *key, robj *val) {
    // 根据 key 的存在情况，进行 key 的写入或覆盖操作
    if (lookupKeyWrite(db,key) == NULL) {
        dbAdd(db,key,val);
    } else {
        dbOverwrite(db,key,val);
    }

    // 增加值的引用计数
    incrRefCount(val);

    // 移除旧 key 原有的过期时间（如果有的话）
    removeExpire(db,key);

    // 告知所有正在 WATCH 这个键的客户端，键已经被修改
    signalModifiedKey(db,key);
}

/*
 * 检查 key 是否存在于 DB
 *
 * 是的话返回 1 ，否则返回 0 
 */
int dbExists(redisDb *db, robj *key) {
    return dictFind(db->dict,key->ptr) != NULL;
}

/* Return a random key, in form of a Redis object.
 * If there are no keys, NULL is returned.
 *
 * 以 Redis Object 的形式随机返回数据库中的一个 key
 * 如果数据库为空，那么返回 NULL
 *
 * The function makes sure to return keys not already expired.
 *
 * 函数只返回未过期的 key
 */
robj *dbRandomKey(redisDb *db) {
    struct dictEntry *de;

    while(1) {
        sds key;
        robj *keyobj;

        // 从字典中返回随机值， O(N)
        de = dictGetRandomKey(db->dict);
        // 数据库为空
        if (de == NULL) return NULL;

        // 取出值对象
        key = dictGetKey(de);
        keyobj = createStringObject(key,sdslen(key));
        // 检查 key 是否已过期
        if (dictFind(db->expires,key)) {
            if (expireIfNeeded(db,keyobj)) {
                decrRefCount(keyobj);
                // 这个 key 已过期，继续寻找下个 key
                continue; /* search for another key. This expired. */
            }
        }

        return keyobj;
    }
}

/* Delete a key, value, and associated expiration entry if any, from the DB */
/*
 * 从数据库中删除 key ，key 对应的值，以及对应的过期时间（如果有的话）
 */
int dbDelete(redisDb *db, robj *key) {
    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary. */
    // 先删除过期时间
    if (dictSize(db->expires) > 0) dictDelete(db->expires,key->ptr);

    // 删除 key 和 value
    if (dictDelete(db->dict,key->ptr) == DICT_OK) {
        if (server.cluster_enabled) SlotToKeyDel(key);
        return 1;
    } else {
        return 0;
    }
}

/*
 * 清空所有数据库
 *
 * T = O(N^2)
 */
long long emptyDb() {
    int j;
    long long removed = 0;

    // 清空所有数据库, O(N^2)
    for (j = 0; j < server.dbnum; j++) {
        removed += dictSize(server.db[j].dict);
        // O(N)
        dictEmpty(server.db[j].dict);
        // O(N)
        dictEmpty(server.db[j].expires);
    }
    
    // 返回清除的 key 数量
    return removed;
}

/*
 * 选择数据库
 * 
 * T = O(1)
 */
int selectDb(redisClient *c, int id) {

    if (id < 0 || id >= server.dbnum)
        return REDIS_ERR;

    c->db = &server.db[id];

    return REDIS_OK;
}

/*-----------------------------------------------------------------------------
 * Hooks for key space changes.
 *
 * Every time a key in the database is modified the function
 * signalModifiedKey() is called.
 *
 * Every time a DB is flushed the function signalFlushDb() is called.
 *----------------------------------------------------------------------------*/

/*
 * 通知所有监视 key 的客户端，key 已被修改。
 *
 * touchWatchedKey 定义在 multi.c
 */
void signalModifiedKey(redisDb *db, robj *key) {
    touchWatchedKey(db,key);
}

/*
 * FLUSHDB/FLUSHALL 命令调用之后的通知函数
 *
 * touchWatchedKeysOnFlush 定义在 multi.c
 */
void signalFlushedDb(int dbid) {
    touchWatchedKeysOnFlush(dbid);
}

/*-----------------------------------------------------------------------------
 * Type agnostic commands operating on the key space
 *----------------------------------------------------------------------------*/

/*
 * 清空客户端当前所使用的数据库
 */
void flushdbCommand(redisClient *c) {
    server.dirty += dictSize(c->db->dict);
    signalFlushedDb(c->db->id);
    dictEmpty(c->db->dict);
    dictEmpty(c->db->expires);
    addReply(c,shared.ok);
}

/*
 * 清空所有数据库
 */
void flushallCommand(redisClient *c) {

    signalFlushedDb(-1);

    // 清空所有数据库
    server.dirty += emptyDb();

    addReply(c,shared.ok);

    // 如果正在执行数据库的保存工作，那么强制中断它
    if (server.rdb_child_pid != -1) {
        kill(server.rdb_child_pid,SIGKILL);
        rdbRemoveTempFile(server.rdb_child_pid);
    }

    if (server.saveparamslen > 0) {
        /* Normally rdbSave() will reset dirty, but we don't want this here
         * as otherwise FLUSHALL will not be replicated nor put into the AOF. */
        int saved_dirty = server.dirty;
        rdbSave(server.rdb_filename);
        server.dirty = saved_dirty;
    }
    server.dirty++;
}

/*
 * 从数据库中删除所有给定 key
 */
void delCommand(redisClient *c) {
    int deleted = 0, j;

    for (j = 1; j < c->argc; j++) {
        if (dbDelete(c->db,c->argv[j])) {
            signalModifiedKey(c->db,c->argv[j]);
            server.dirty++;
            deleted++;
        }
    }

    addReplyLongLong(c,deleted);
}

/*
 * 检查给定 key 是否存在
 */
void existsCommand(redisClient *c) {
    expireIfNeeded(c->db,c->argv[1]);
    if (dbExists(c->db,c->argv[1])) {
        addReply(c, shared.cone);
    } else {
        addReply(c, shared.czero);
    }
}

/*
 * 切换数据库
 */
void selectCommand(redisClient *c) {
    long id;

    // id 号必须是整数
    if (getLongFromObjectOrReply(c, c->argv[1], &id,
        "invalid DB index") != REDIS_OK)
        return;

    // 不允许在集群模式下似乎用 SELECT
    if (server.cluster_enabled && id != 0) {
        addReplyError(c,"SELECT is not allowed in cluster mode");
        return;
    }

    // 切换数据库
    if (selectDb(c,id) == REDIS_ERR) {
        addReplyError(c,"invalid DB index");
    } else {
        addReply(c,shared.ok);
    }
}

/*
 * RANDOMKEY 命令的实现
 *
 * 随机从数据库中返回一个键
 */
void randomkeyCommand(redisClient *c) {
    robj *key;

    if ((key = dbRandomKey(c->db)) == NULL) {
        addReply(c,shared.nullbulk);
        return;
    }

    addReplyBulk(c,key);
    decrRefCount(key);
}

/*
 * KEYS 命令的实现
 *
 * 查找和给定模式匹配的 key
 */
void keysCommand(redisClient *c) {
    dictIterator *di;
    dictEntry *de;

    sds pattern = c->argv[1]->ptr;

    int plen = sdslen(pattern),
        allkeys;
    unsigned long numkeys = 0;
    void *replylen = addDeferredMultiBulkLength(c);

    // 指向当前数据库的 key space
    di = dictGetSafeIterator(c->db->dict);
    // key 的匹配模式
    allkeys = (pattern[0] == '*' && pattern[1] == '\0');
    while((de = dictNext(di)) != NULL) {
        sds key = dictGetKey(de);
        robj *keyobj;

        // 检查当前迭代到的 key 是否匹配，如果是的话，将它返回
        if (allkeys || stringmatchlen(pattern,plen,key,sdslen(key),0)) {
            keyobj = createStringObject(key,sdslen(key));
            // 只返回不过期的 key
            if (expireIfNeeded(c->db,keyobj) == 0) {
                addReplyBulk(c,keyobj);
                numkeys++;
            }
            decrRefCount(keyobj);
        }
    }
    dictReleaseIterator(di);

    setDeferredMultiBulkLength(c,replylen,numkeys);
}

/*
 * DBSIZE 命令的实现
 *
 * 返回数据库键值对数量
 */
void dbsizeCommand(redisClient *c) {
    addReplyLongLong(c,dictSize(c->db->dict));
}

/*
 * LASTSAVE 命令的实现
 *
 * 返回数据库的最后保存时间
 */
void lastsaveCommand(redisClient *c) {
    addReplyLongLong(c,server.lastsave);
}

/*
 * TYPE 命令的实现
 * 
 * 返回 key 对象类型的字符串形式
 */
void typeCommand(redisClient *c) {
    robj *o;
    char *type;

    o = lookupKeyRead(c->db,c->argv[1]);
    if (o == NULL) {
        type = "none";
    } else {
        switch(o->type) {
        case REDIS_STRING: type = "string"; break;
        case REDIS_LIST: type = "list"; break;
        case REDIS_SET: type = "set"; break;
        case REDIS_ZSET: type = "zset"; break;
        case REDIS_HASH: type = "hash"; break;
        default: type = "unknown"; break;
        }
    }

    addReplyStatus(c,type);
}

/*
 * 关闭服务器
 */
void shutdownCommand(redisClient *c) {
    int flags = 0;

    // 选择关闭的模式
    if (c->argc > 2) {
        addReply(c,shared.syntaxerr);
        return;
    } else if (c->argc == 2) {
        if (!strcasecmp(c->argv[1]->ptr,"nosave")) {
            flags |= REDIS_SHUTDOWN_NOSAVE;
        } else if (!strcasecmp(c->argv[1]->ptr,"save")) {
            flags |= REDIS_SHUTDOWN_SAVE;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }

    // 关闭
    if (prepareForShutdown(flags) == REDIS_OK) exit(0);

    addReplyError(c,"Errors trying to SHUTDOWN. Check logs.");
}

/*
 * 对 key 进行改名
 */
void renameGenericCommand(redisClient *c, int nx) {
    robj *o;
    long long expire;

    /* To use the same key as src and dst is probably an error */
    if (sdscmp(c->argv[1]->ptr,c->argv[2]->ptr) == 0) {
        addReply(c,shared.sameobjecterr);
        return;
    }

    // 取出源 key
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr)) == NULL)
        return;

    incrRefCount(o);
    expire = getExpire(c->db,c->argv[1]);
    // 取出目标 key
    if (lookupKeyWrite(c->db,c->argv[2]) != NULL) {
        // 如果目标 key 存在，且 nx FLAG 打开，那么设置失败，直接返回
        if (nx) {
            decrRefCount(o);
            addReply(c,shared.czero);
            return;
        }
        /* Overwrite: delete the old key before creating the new one with the same name. */
        // 否则，将目标 key 删除
        dbDelete(c->db,c->argv[2]);
    }
    // 将源对象以目标 key 的名字添加到数据库
    dbAdd(c->db,c->argv[2],o);
    // 如果源 key 有超时时间，那么设置新 key 的超时时间
    if (expire != -1) setExpire(c->db,c->argv[2],expire);
    // 删除旧的源 key
    dbDelete(c->db,c->argv[1]);

    signalModifiedKey(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[2]);

    server.dirty++;

    addReply(c,nx ? shared.cone : shared.ok);
}

void renameCommand(redisClient *c) {
    renameGenericCommand(c,0);
}

void renamenxCommand(redisClient *c) {
    renameGenericCommand(c,1);
}

/*
 * 将 key 从一个数据库移动到另一个数据库
 */
void moveCommand(redisClient *c) {
    robj *o;
    redisDb *src, *dst;
    int srcid;

    // 不允许在集群情况下使用
    if (server.cluster_enabled) {
        addReplyError(c,"MOVE is not allowed in cluster mode");
        return;
    }

    /* Obtain source and target DB pointers */
    // 记录源数据库
    src = c->db;
    srcid = c->db->id;
    // 通过切换数据库来测试目标数据库是否存在
    if (selectDb(c,atoi(c->argv[2]->ptr)) == REDIS_ERR) {
        addReply(c,shared.outofrangeerr);
        return;
    }
    // 记录目标数据库
    dst = c->db;
    // 切换回源数据库
    selectDb(c,srcid); /* Back to the source DB */

    /* If the user is moving using as target the same
     * DB as the source DB it is probably an error. */
    // 源数据库和目标数据库相同，直接返回
    if (src == dst) {
        addReply(c,shared.sameobjecterr);
        return;
    }

    /* Check if the element exists and get a reference */
    // 检查源 key 的存在性
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (!o) {
        addReply(c,shared.czero);
        return;
    }

    /* Return zero if the key already exists in the target DB */
    // 如果 key 已经存在于目标数据库，那么返回
    if (lookupKeyWrite(dst,c->argv[1]) != NULL) {
        addReply(c,shared.czero);
        return;
    }

    // 将 key 添加到目标数据库
    dbAdd(dst,c->argv[1],o);
    incrRefCount(o);

    /* OK! key moved, free the entry in the source DB */
    // 删除源数据库中的 key
    dbDelete(src,c->argv[1]);

    server.dirty++;
    addReply(c,shared.cone);
}

/*-----------------------------------------------------------------------------
 * Expires API
 *----------------------------------------------------------------------------*/

/*
 * 移除 key 的过期时间
 */
int removeExpire(redisDb *db, robj *key) {
    /* An expire may only be removed if there is a corresponding entry in the
     * main dict. Otherwise, the key will never be freed. */
    redisAssertWithInfo(NULL,key,dictFind(db->dict,key->ptr) != NULL);
    return dictDelete(db->expires,key->ptr) == DICT_OK;
}

/*
 * 为 key 设置过期时间
 */
void setExpire(redisDb *db, robj *key, long long when) {
    dictEntry *kde, *de;

    /* Reuse the sds from the main dict in the expire dict */
    kde = dictFind(db->dict,key->ptr);
    redisAssertWithInfo(NULL,key,kde != NULL);
    de = dictReplaceRaw(db->expires,dictGetKey(kde));
    dictSetSignedIntegerVal(de,when);
}

/* Return the expire time of the specified key, or -1 if no expire
 * is associated with this key (i.e. the key is non volatile) */
/*
 * 返回给定 key 的过期时间
 * 
 * 如果给定 key 没有和某个过期时间关联（它是一个非易失 key ）
 * 那么返回 -1
 */
long long getExpire(redisDb *db, robj *key) {
    dictEntry *de;

    /* No expire? return ASAP */
    // 数据库的过期记录中没有任何数据
    // 或者，过期记录中没有和 key 关联的时间
    // 那么直接返回
    if (dictSize(db->expires) == 0 ||
       (de = dictFind(db->expires,key->ptr)) == NULL) return -1;

    /* The entry was found in the expire dict, this means it should also
     * be present in the main dict (safety check). */
    // 确保 key 在数据库中必定存在（安全性检查）
    redisAssertWithInfo(NULL,key,dictFind(db->dict,key->ptr) != NULL);

    // 取出字典值中保存的整数值
    return dictGetSignedIntegerVal(de);
}

/* Propagate expires into slaves and the AOF file.
 * When a key expires in the master, a DEL operation for this key is sent
 * to all the slaves and the AOF file if enabled.
 *
 * This way the key expiry is centralized in one place, and since both
 * AOF and the master->slave link guarantee operation ordering, everything
 * will be consistent even if we allow write operations against expiring
 * keys. */
/*
 * 向附属节点和 AOF 文件传播过期命令
 *
 * 当一个键在主节点中过期时，传播一个 DEL 命令到所有附属节点和 AOF 文件
 *
 * 通过将删除过期键的工作集中在主节点中，可以维持数据库的一致性。
 */
void propagateExpire(redisDb *db, robj *key) {
    robj *argv[2];

    // DEL 命名
    argv[0] = shared.del;
    // 目标键
    argv[1] = key;

    incrRefCount(argv[0]);
    incrRefCount(argv[1]);

    if (server.aof_state != REDIS_AOF_OFF)
        feedAppendOnlyFile(server.delCommand,db->id,argv,2);

    if (listLength(server.slaves))
        replicationFeedSlaves(server.slaves,db->id,argv,2);

    decrRefCount(argv[0]);
    decrRefCount(argv[1]);
}

/*
 * 如果 key 已经过期，那么将它删除，否则，不做动作。
 *
 * key 没有过期时间、服务器正在载入或 key 未过期时，返回 0 
 * key 已过期，那么返回正数值
 */
int expireIfNeeded(redisDb *db, robj *key) {
    // 取出 key 的过期时间
    long long when = getExpire(db,key);

    // key 没有过期时间，直接返回
    if (when < 0) return 0; /* No expire for this key */

    /* Don't expire anything while loading. It will be done later. */
    // 不要在服务器载入数据时执行过期
    if (server.loading) return 0;

    /* If we are running in the context of a slave, return ASAP:
     * the slave key expiration is controlled by the master that will
     * send us synthesized DEL operations for expired keys.
     *
     * Still we try to return the right information to the caller, 
     * that is, 0 if we think the key should be still valid, 1 if
     * we think the key is expired at this time. */
    // 如果服务器作为附属节点运行，那么直接返回
    // 因为附属节点的过期是由主节点通过发送 DEL 命令来删除的
    // 不必自主删除
    if (server.masterhost != NULL) {
        // 返回一个理论上正确的值，但不执行实际的删除操作
        return mstime() > when;
    }

    /* Return when this key has not expired */
    // 未过期
    if (mstime() <= when) return 0;

    /* Delete the key */
    server.stat_expiredkeys++;

    // 传播过期命令
    propagateExpire(db,key);

    // 从数据库中删除 key
    return dbDelete(db,key);
}

/*-----------------------------------------------------------------------------
 * Expires Commands
 *----------------------------------------------------------------------------*/

/* This is the generic command implementation for EXPIRE, PEXPIRE, EXPIREAT
 * and PEXPIREAT. Because the commad second argument may be relative or absolute
 * the "basetime" argument is used to signal what the base time is (either 0
 * for *AT variants of the command, or the current time for relative expires).
 *
 * 这个命令是 EXPIRE 、 PEXPIRE 、 EXPIREAT 和 PEXPIREAT 命令的实现。
 *
 * 命令的第二个参数可能是绝对值，也可能是相对值。
 * 当执行 *AT 命令时， basetime 为 0 ，在其他情况下，它保存的就是当前的绝对时间。
 *
 * unit is either UNIT_SECONDS or UNIT_MILLISECONDS, and is only used for
 * the argv[2] parameter. The basetime is always specified in milliesconds. 
 *
 * unit 用于指定第二个参数的格式，它可以是 UNIT_SECONDS 或 UNIT_MILLISECONDS ，
 * basetime 参数总是毫秒格式的。
 */
void expireGenericCommand(redisClient *c, long long basetime, int unit) {

    dictEntry *de;

    robj *key = c->argv[1], 
         *param = c->argv[2];

    long long when; /* unix time in milliseconds when the key will expire. */

    // 取出 when 参数
    if (getLongLongFromObjectOrReply(c, param, &when, NULL) != REDIS_OK)
        return;

    // 如果 when 参数以秒计算，那么将它转换成毫秒
    if (unit == UNIT_SECONDS) when *= 1000;
    // 将时间设置为绝对时间
    when += basetime;

    // 取出键
    de = dictFind(c->db->dict,key->ptr);
    if (de == NULL) {
        // 键不存在，返回 0
        addReply(c,shared.czero);
        return;
    }
    /* EXPIRE with negative TTL, or EXPIREAT with a timestamp into the past
     * should never be executed as a DEL when load the AOF or in the context
     * of a slave instance.
     *
     * Instead we take the other branch of the IF statement setting an expire
     * (possibly in the past) and wait for an explicit DEL from the master. */
    // 如果当前节点为主节点
    // 并且在附属节点或者 AOF 文件中解释到了负数 TTL ，或者已经过期的绝对时间
    // 那么删除 key ，并向附属节点和 AOF 发送 DEL 命令
    if (when <= mstime() && !server.loading && !server.masterhost) {
        robj *aux;

        redisAssertWithInfo(c,key,dbDelete(c->db,key));
        server.dirty++;

        /* Replicate/AOF this as an explicit DEL. */
        aux = createStringObject("DEL",3);
        rewriteClientCommandVector(c,2,aux,key);
        decrRefCount(aux);
        signalModifiedKey(c->db,key);
        addReply(c, shared.cone);
        return;

    // 否则，设置 key 的过期时间
    } else {

        setExpire(c->db,key,when);

        addReply(c,shared.cone);
        signalModifiedKey(c->db,key);
        server.dirty++;
        return;
    }
}

void expireCommand(redisClient *c) {
    expireGenericCommand(c,mstime(),UNIT_SECONDS);
}

void expireatCommand(redisClient *c) {
    expireGenericCommand(c,0,UNIT_SECONDS);
}

void pexpireCommand(redisClient *c) {
    expireGenericCommand(c,mstime(),UNIT_MILLISECONDS);
}

void pexpireatCommand(redisClient *c) {
    expireGenericCommand(c,0,UNIT_MILLISECONDS);
}

void ttlGenericCommand(redisClient *c, int output_ms) {
    long long expire, ttl = -1;

    expire = getExpire(c->db,c->argv[1]);
    /* If the key does not exist at all, return -2 */
    if (expire == -1 && lookupKeyRead(c->db,c->argv[1]) == NULL) {
        addReplyLongLong(c,-2);
        return;
    }
    /* The key exists. Return -1 if it has no expire, or the actual
     * TTL value otherwise. */
    if (expire != -1) {
        ttl = expire-mstime();
        if (ttl < 0) ttl = -1;
    }
    if (ttl == -1) {
        addReplyLongLong(c,-1);
    } else {
        addReplyLongLong(c,output_ms ? ttl : ((ttl+500)/1000));
    }
}

void ttlCommand(redisClient *c) {
    ttlGenericCommand(c, 0);
}

void pttlCommand(redisClient *c) {
    ttlGenericCommand(c, 1);
}

void persistCommand(redisClient *c) {
    dictEntry *de;

    de = dictFind(c->db->dict,c->argv[1]->ptr);
    if (de == NULL) {
        addReply(c,shared.czero);
    } else {
        if (removeExpire(c->db,c->argv[1])) {
            addReply(c,shared.cone);
            server.dirty++;
        } else {
            addReply(c,shared.czero);
        }
    }
}

/* -----------------------------------------------------------------------------
 * API to get key arguments from commands
 * ---------------------------------------------------------------------------*/

int *getKeysUsingCommandTable(struct redisCommand *cmd,robj **argv, int argc, int *numkeys) {
    int j, i = 0, last, *keys;
    REDIS_NOTUSED(argv);

    if (cmd->firstkey == 0) {
        *numkeys = 0;
        return NULL;
    }
    last = cmd->lastkey;
    if (last < 0) last = argc+last;
    keys = zmalloc(sizeof(int)*((last - cmd->firstkey)+1));
    for (j = cmd->firstkey; j <= last; j += cmd->keystep) {
        redisAssert(j < argc);
        keys[i++] = j;
    }
    *numkeys = i;
    return keys;
}

int *getKeysFromCommand(struct redisCommand *cmd,robj **argv, int argc, int *numkeys, int flags) {
    if (cmd->getkeys_proc) {
        return cmd->getkeys_proc(cmd,argv,argc,numkeys,flags);
    } else {
        return getKeysUsingCommandTable(cmd,argv,argc,numkeys);
    }
}

void getKeysFreeResult(int *result) {
    zfree(result);
}

int *noPreloadGetKeys(struct redisCommand *cmd,robj **argv, int argc, int *numkeys, int flags) {
    if (flags & REDIS_GETKEYS_PRELOAD) {
        *numkeys = 0;
        return NULL;
    } else {
        return getKeysUsingCommandTable(cmd,argv,argc,numkeys);
    }
}

int *renameGetKeys(struct redisCommand *cmd,robj **argv, int argc, int *numkeys, int flags) {
    if (flags & REDIS_GETKEYS_PRELOAD) {
        int *keys = zmalloc(sizeof(int));
        *numkeys = 1;
        keys[0] = 1;
        return keys;
    } else {
        return getKeysUsingCommandTable(cmd,argv,argc,numkeys);
    }
}

int *zunionInterGetKeys(struct redisCommand *cmd,robj **argv, int argc, int *numkeys, int flags) {
    int i, num, *keys;
    REDIS_NOTUSED(cmd);
    REDIS_NOTUSED(flags);

    num = atoi(argv[2]->ptr);
    /* Sanity check. Don't return any key if the command is going to
     * reply with syntax error. */
    if (num > (argc-3)) {
        *numkeys = 0;
        return NULL;
    }
    keys = zmalloc(sizeof(int)*num);
    for (i = 0; i < num; i++) keys[i] = 3+i;
    *numkeys = num;
    return keys;
}

/* Slot to Key API. This is used by Redis Cluster in order to obtain in
 * a fast way a key that belongs to a specified hash slot. This is useful
 * while rehashing the cluster. */
void SlotToKeyAdd(robj *key) {
    unsigned int hashslot = keyHashSlot(key->ptr,sdslen(key->ptr));

    zslInsert(server.cluster.slots_to_keys,hashslot,key);
    incrRefCount(key);
}

void SlotToKeyDel(robj *key) {
    unsigned int hashslot = keyHashSlot(key->ptr,sdslen(key->ptr));

    zslDelete(server.cluster.slots_to_keys,hashslot,key);
}

unsigned int GetKeysInSlot(unsigned int hashslot, robj **keys, unsigned int count) {
    zskiplistNode *n;
    zrangespec range;
    int j = 0;

    range.min = range.max = hashslot;
    range.minex = range.maxex = 0;
    
    n = zslFirstInRange(server.cluster.slots_to_keys, range);
    while(n && n->score == hashslot && count--) {
        keys[j++] = n->obj;
        n = n->level[0].forward;
    }
    return j;
}
