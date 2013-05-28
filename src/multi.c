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

/* ================================ MULTI/EXEC ============================== */

/* Client state initialization for MULTI/EXEC */
/*
 * 初始化客户端的事务状态
 *
 * T = O(1)
 */
void initClientMultiState(redisClient *c) {
    c->mstate.commands = NULL;
    c->mstate.count = 0;
}

/* Release all the resources associated with MULTI/EXEC state */
/*
 * 释放所有在事务队列中的命令
 *
 * T = O(N^2)
 */
void freeClientMultiState(redisClient *c) {
    int j;

    // 释放所有命令
    for (j = 0; j < c->mstate.count; j++) {
        int i;
        multiCmd *mc = c->mstate.commands+j;

        // 释放命令的所有参数
        for (i = 0; i < mc->argc; i++)
            decrRefCount(mc->argv[i]);
        zfree(mc->argv);
    }
    zfree(c->mstate.commands);
}

/* Add a new command into the MULTI commands queue */
/*
 * 将新命令添加到事务队列中
 *
 * T = O(N)
 */
void queueMultiCommand(redisClient *c) {
    multiCmd *mc;
    int j;

    // 重分配空间，为新命令申请空间
    c->mstate.commands = zrealloc(c->mstate.commands,
            sizeof(multiCmd)*(c->mstate.count+1));

    // 指针指向新分配的空间
    // 并将命令内容保存进去
    mc = c->mstate.commands+c->mstate.count;
    mc->cmd = c->cmd;   // 保存要执行的命令
    mc->argc = c->argc; // 保存命令参数的数量
    mc->argv = zmalloc(sizeof(robj*)*c->argc);  // 为参数分配空间
    memcpy(mc->argv,c->argv,sizeof(robj*)*c->argc); // 复制参数
    for (j = 0; j < c->argc; j++)   // 为参数的引用计数增一
        incrRefCount(mc->argv[j]);

    // 入队命令数量增一
    c->mstate.count++;
}

/*
 * 放弃事务，清理并重置客户端的事务状态
 *
 * T = O(N^2)
 */
void discardTransaction(redisClient *c) {
    // 释放参数空间
    freeClientMultiState(c);
    // 重置事务状态
    initClientMultiState(c);
    // 关闭相关的 flag
    c->flags &= ~(REDIS_MULTI|REDIS_DIRTY_CAS|REDIS_DIRTY_EXEC);;
    // 取消所有 key 的监视, O(N^2)
    unwatchAllKeys(c);
}

/* Flag the transacation as DIRTY_EXEC so that EXEC will fail.
 * Should be called every time there is an error while queueing a command. */
/*
 * 如果在入队的过程中发生命令出错，
 * 那么让客户端变为“脏”，令下次事务执行失败
 *
 * T = O(1)
 */
void flagTransaction(redisClient *c) {
    if (c->flags & REDIS_MULTI)
        c->flags |= REDIS_DIRTY_EXEC;
}

/*
 * MULTI 命令的实现
 *
 * 打开客户端的 FLAG ，让命令入队到事务队列里
 *
 * T = O(1)
 */
void multiCommand(redisClient *c) {
    // MULTI 命令不能嵌套
    if (c->flags & REDIS_MULTI) {
        addReplyError(c,"MULTI calls can not be nested");
        return;
    }
    
    // 打开事务的 FLAG
    // 从此之后，除 DISCARD 和 EXEC 等少数几个命令之外
    // 其他所有的命令都会被添加到事务队列里
    c->flags |= REDIS_MULTI;
    addReply(c,shared.ok);
}

/*
 * DISCAD 命令的实现
 *
 * 放弃事务，并清理相关资源
 *
 * T = O(N)
 */
void discardCommand(redisClient *c) {
    // 只能在 MULTI 命令已启用的情况下使用
    if (!(c->flags & REDIS_MULTI)) {
        addReplyError(c,"DISCARD without MULTI");
        return;
    }
    // 放弃事务, O(N)
    discardTransaction(c);
    addReply(c,shared.ok);
}

/* Send a MULTI command to all the slaves and AOF file. Check the execCommand
 * implememntation for more information. */
/*
 * 向所有附属节点和 AOF 文件发送 MULTI 命令
 */
void execCommandReplicateMulti(redisClient *c) {
    robj *multistring = createStringObject("MULTI",5);

    if (server.aof_state != REDIS_AOF_OFF)
        feedAppendOnlyFile(server.multiCommand,c->db->id,&multistring,1);
    if (listLength(server.slaves))
        replicationFeedSlaves(server.slaves,c->db->id,&multistring,1);
    decrRefCount(multistring);
}

/*
 * EXEC 命令的实现
 */
void execCommand(redisClient *c) {
    int j;
    // 用于保存执行命令、命令的参数和参数数量的副本
    robj **orig_argv;
    int orig_argc;
    struct redisCommand *orig_cmd;

    // 只能在 MULTI 已启用的情况下执行
    if (!(c->flags & REDIS_MULTI)) {
        addReplyError(c,"EXEC without MULTI");
        return;
    }

    /* Check if we need to abort the EXEC because:
     * 以下情况发生时，取消事务
     *
     * 1) Some WATCHed key was touched.
     *    某些被监视的键已被修改（状态为 REDIS_DIRTY_CAS）
     *
     * 2) There was a previous error while queueing commands.
     *    有命令在入队时发生错误（状态为 REDIS_DIRTY_EXEC）
     *
     * A failed EXEC in the first case returns a multi bulk nil object
     * (technically it is not an error but a special behavior), while
     * in the second an EXECABORT error is returned.
     *
     * 第一种情况返回多个空白 NULL 对象，
     * 第二种情况返回一个 EXECABORT 错误。 
     */
    if (c->flags & (REDIS_DIRTY_CAS|REDIS_DIRTY_EXEC)) {
        // 根据状态，决定返回的错误的类型
        addReply(c, c->flags & REDIS_DIRTY_EXEC ? shared.execaborterr :
                                                  shared.nullmultibulk);

        // 以下四句可以用 discardTransaction() 来替换
        freeClientMultiState(c);
        initClientMultiState(c);
        c->flags &= ~(REDIS_MULTI|REDIS_DIRTY_CAS|REDIS_DIRTY_EXEC);
        unwatchAllKeys(c);

        goto handle_monitor;
    }

    /* Replicate a MULTI request now that we are sure the block is executed.
     * This way we'll deliver the MULTI/..../EXEC block as a whole and
     * both the AOF and the replication link will have the same consistency
     * and atomicity guarantees. */
    // 向所有附属节点和 AOF 文件发送 MULTI 命令
    execCommandReplicateMulti(c);

    /* Exec all the queued commands */
    unwatchAllKeys(c); /* Unwatch ASAP otherwise we'll waste CPU cycles */

    // 将三个原始参数备份起来
    orig_argv = c->argv;
    orig_argc = c->argc;
    orig_cmd = c->cmd;
    addReplyMultiBulkLen(c,c->mstate.count);
    // 执行所有入队的命令
    for (j = 0; j < c->mstate.count; j++) {
        // 因为 call 可能修改命令，而命令需要传送给其他同步节点
        // 所以这里将要执行的命令（及其参数）先备份起来
        c->argc = c->mstate.commands[j].argc;
        c->argv = c->mstate.commands[j].argv;
        c->cmd = c->mstate.commands[j].cmd;

        // 执行命令
        call(c,REDIS_CALL_FULL);

        /* Commands may alter argc/argv, restore mstate. */
        // 还原原始的参数到队列里
        c->mstate.commands[j].argc = c->argc;
        c->mstate.commands[j].argv = c->argv;
        c->mstate.commands[j].cmd = c->cmd;
    }
    // 还原三个原始命令
    c->argv = orig_argv;
    c->argc = orig_argc;
    c->cmd = orig_cmd;

    // 以下三句也可以用 discardTransaction() 来替换
    freeClientMultiState(c);
    initClientMultiState(c);
    c->flags &= ~(REDIS_MULTI|REDIS_DIRTY_CAS|REDIS_DIRTY_EXEC);
    /* Make sure the EXEC command is always replicated / AOF, since we
     * always send the MULTI command (we can't know beforehand if the
     * next operations will contain at least a modification to the DB). */
    server.dirty++;

handle_monitor:
    /* Send EXEC to clients waiting data from MONITOR. We do it here
     * since the natural order of commands execution is actually:
     * MUTLI, EXEC, ... commands inside transaction ...
     * Instead EXEC is flagged as REDIS_CMD_SKIP_MONITOR in the command
     * table, and we do it here with correct ordering. */
    // 向同步节点发送命令
    if (listLength(server.monitors) && !server.loading)
        replicationFeedMonitors(c,server.monitors,c->db->id,c->argv,c->argc);
}

/* ===================== WATCH (CAS alike for MULTI/EXEC) ===================
 *
 * The implementation uses a per-DB hash table mapping keys to list of clients
 * WATCHing those keys, so that given a key that is going to be modified
 * we can mark all the associated clients as dirty.
 *
 * 实现为每个 DB 准备一个字典（哈希表），字典的键为该数据库被 WATCHED 的 key
 * 而字典的值是一个链表，保存了所有监视这个 key 的客户端
 * 一旦某个 key 被修改，程序会将整个链表的所有客户端都设置为被污染
 *
 * Also every client contains a list of WATCHed keys so that's possible to
 * un-watch such keys when the client is freed or when UNWATCH is called. 
 *
 * 此外，客户端还维持这一个保存所有 WATCH key 的链表，
 * 这样就可以在事务执行或者 UNWATCH 调用时，一次清除所有 WATCH key 。
 */

/* In the client->watched_keys list we need to use watchedKey structures
 * as in order to identify a key in Redis we need both the key name and the
 * DB */
/*
 * 被监视的 key 的资料
 */
typedef struct watchedKey {
    // 被监视的 key
    robj *key;
    // key 所在的数据库
    redisDb *db;
} watchedKey;

/* Watch for the specified key */
/*
 * 监视给定 key
 *
 * T = O(N)
 */
void watchForKey(redisClient *c, robj *key) {
    list *clients = NULL;
    listIter li;
    listNode *ln;
    watchedKey *wk;

    /* Check if we are already watching for this key */
    // 检查该 key 是否已经被 WATCH 
    // （出现在 WATCH 命令调用时一个 key 被输入多次的情况）
    // 如果是的话，直接返回
    // O(N)
    listRewind(c->watched_keys,&li);
    while((ln = listNext(&li))) {
        wk = listNodeValue(ln);
        if (wk->db == c->db && equalStringObjects(key,wk->key))
            return; /* Key already watched */
    }

    // key 未被监视
    // 根据 key ，将客户端加入到 DB 的监视 key 字典中
    /* This key is not already watched in this DB. Let's add it */
    // O(1)
    clients = dictFetchValue(c->db->watched_keys,key);
    if (!clients) { 
        clients = listCreate();
        dictAdd(c->db->watched_keys,key,clients);
        incrRefCount(key);
    }
    listAddNodeTail(clients,c);

    // 将 key 添加到客户端的监视列表中
    /* Add the new key to the lits of keys watched by this client */
    // O(1)
    wk = zmalloc(sizeof(*wk));
    wk->key = key;
    wk->db = c->db;
    incrRefCount(key);
    listAddNodeTail(c->watched_keys,wk);
}

/* Unwatch all the keys watched by this client. To clean the EXEC dirty
 * flag is up to the caller. */
/*
 * 取消所有该客户端监视的 key 
 * 对事务状态的清除由调用者执行
 *
 * T = O(N^2)
 */
void unwatchAllKeys(redisClient *c) {
    listIter li;
    listNode *ln;

    // 没有键被 watch ，直接返回
    if (listLength(c->watched_keys) == 0) return;

    // 从客户端以及 DB 中删除所有监视 key 和客户端的资料
    // O(N^2)
    listRewind(c->watched_keys,&li);
    while((ln = listNext(&li))) {
        list *clients;
        watchedKey *wk;

        /* Lookup the watched key -> clients list and remove the client
         * from the list */
        // 取出 watchedKey 结构
        wk = listNodeValue(ln);
        // 删除 db 中的客户端信息, O(1)
        clients = dictFetchValue(wk->db->watched_keys, wk->key);
        redisAssertWithInfo(c,NULL,clients != NULL);
        // O(N)
        listDelNode(clients,listSearchKey(clients,c));

        /* Kill the entry at all if this was the only client */
        if (listLength(clients) == 0)
            dictDelete(wk->db->watched_keys, wk->key);
        /* Remove this watched key from the client->watched list */

        // 将 key 从客户端的监视列表中删除, O(1)
        listDelNode(c->watched_keys,ln);

        decrRefCount(wk->key);
        zfree(wk);
    }
}

/* "Touch" a key, so that if this key is being WATCHed by some client the
 * next EXEC will fail. */
/*
 * “碰触”（touch）给定 key ，如果这个 key 正在被监视的话，
 * 让监视它的客户端在执行 EXEC 命令时失败。
 *
 * T = O(N)
 */
void touchWatchedKey(redisDb *db, robj *key) {
    list *clients;
    listIter li;
    listNode *ln;

    // 如果数据库中没有任何 key 被监视，那么直接返回
    if (dictSize(db->watched_keys) == 0) return;

    // 取出数据库中所有监视给定 key 的客户端
    clients = dictFetchValue(db->watched_keys, key);
    if (!clients) return;

    /* Mark all the clients watching this key as REDIS_DIRTY_CAS */
    /* Check if we are already watching for this key */
    // 打开所有监视这个 key 的客户端的 REDIS_DIRTY_CAS 状态
    // O(N)
    listRewind(clients,&li);
    while((ln = listNext(&li))) {
        redisClient *c = listNodeValue(ln);

        c->flags |= REDIS_DIRTY_CAS;
    }
}

/* On FLUSHDB or FLUSHALL all the watched keys that are present before the
 * flush but will be deleted as effect of the flushing operation should
 * be touched. "dbid" is the DB that's getting the flush. -1 if it is
 * a FLUSHALL operation (all the DBs flushed). */
/*
 * 为 FLUSHDB 和 FLUSHALL 特别设置的触碰函数
 *
 * T = O(N^2)
 */
void touchWatchedKeysOnFlush(int dbid) {
    listIter li1, li2;
    listNode *ln;

    /* For every client, check all the waited keys */
    // 列出所有客户端，O(N)
    listRewind(server.clients,&li1);
    while((ln = listNext(&li1))) {
        redisClient *c = listNodeValue(ln);
        // 列出所有监视 key ,O(N)
        listRewind(c->watched_keys,&li2);
        while((ln = listNext(&li2))) {
            watchedKey *wk = listNodeValue(ln);

            /* For every watched key matching the specified DB, if the
             * key exists, mark the client as dirty, as the key will be
             * removed. */
            // 如果目标 db 和监视 key 的 DB 相同，
            // 那么打开客户端的 REDIS_DIRTY_CAS 选项
            // O(1)
            if (dbid == -1 || wk->db->id == dbid) {
                if (dictFind(wk->db->dict, wk->key->ptr) != NULL)
                    c->flags |= REDIS_DIRTY_CAS;
            }
        }
    }
}

/*
 * 将所有输入键添加到监视列表当中
 *
 * T = O(N^2)
 */
void watchCommand(redisClient *c) {
    int j;

    // 不能在事务中使用
    if (c->flags & REDIS_MULTI) {
        addReplyError(c,"WATCH inside MULTI is not allowed");
        return;
    }

    // 监视所有 key ，O(N^2)
    for (j = 1; j < c->argc; j++)
        // O(N)
        watchForKey(c,c->argv[j]);

    addReply(c,shared.ok);
}

/*
 * 取消对所有 key 的监视
 * 并关闭客户端的 REDIS_DIRTY_CAS 选项
 *
 * T = O(N^2)
 */
void unwatchCommand(redisClient *c) {
    // O(N^2)
    unwatchAllKeys(c);

    c->flags &= (~REDIS_DIRTY_CAS);

    addReply(c,shared.ok);
}
