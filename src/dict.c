/* Hash Tables Implementation.
 *
 * This file implements in memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto resize if needed
 * tables of power of two in size are used, collisions are handled by
 * chaining. See the source code for more information... :)
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include "fmacros.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>
#include <limits.h>
#include <sys/time.h>
#include <ctype.h>

#include "dict.h"
#include "zmalloc.h"

/* Using dictEnableResize() / dictDisableResize() we make possible to
 * enable/disable resizing of the hash table as needed. This is very important
 * for Redis, as we use copy-on-write and don't want to move too much memory
 * around when there is a child performing saving operations.
 *
 * Note that even when dict_can_resize is set to 0, not all resizes are
 * prevented: an hash table is still allowed to grow if the ratio between
 * the number of elements and the buckets > dict_force_resize_ratio. */
static int dict_can_resize = 1;
static unsigned int dict_force_resize_ratio = 5;

/* -------------------------- private prototypes ---------------------------- */

static int _dictExpandIfNeeded(dict *ht);
static unsigned long _dictNextPower(unsigned long size);
static int _dictKeyIndex(dict *ht, const void *key);
static int _dictInit(dict *ht, dictType *type, void *privDataPtr);

/* -------------------------- hash functions -------------------------------- */

/* Thomas Wang's 32 bit Mix Function */
unsigned int dictIntHashFunction(unsigned int key)
{
    key += ~(key << 15);
    key ^=  (key >> 10);
    key +=  (key << 3);
    key ^=  (key >> 6);
    key += ~(key << 11);
    key ^=  (key >> 16);
    return key;
}

/* Identity hash function for integer keys */
unsigned int dictIdentityHashFunction(unsigned int key)
{
    return key;
}

static uint32_t dict_hash_function_seed = 5381;

void dictSetHashFunctionSeed(uint32_t seed) {
    dict_hash_function_seed = seed;
}

uint32_t dictGetHashFunctionSeed(void) {
    return dict_hash_function_seed;
}

/* MurmurHash2, by Austin Appleby
 * Note - This code makes a few assumptions about how your machine behaves -
 * 1. We can read a 4-byte value from any address without crashing
 * 2. sizeof(int) == 4
 *
 * And it has a few limitations -
 *
 * 1. It will not work incrementally.
 * 2. It will not produce the same results on little-endian and big-endian
 *    machines.
 *
 * 算法的具体信息可以参考 http://code.google.com/p/smhasher/
 */
unsigned int dictGenHashFunction(const void *key, int len) {
    /* 'm' and 'r' are mixing constants generated offline.
     They're not really 'magic', they just happen to work well.  */
    uint32_t seed = dict_hash_function_seed;
    const uint32_t m = 0x5bd1e995;
    const int r = 24;

    /* Initialize the hash to a 'random' value */
    uint32_t h = seed ^ len;

    /* Mix 4 bytes at a time into the hash */
    const unsigned char *data = (const unsigned char *)key;

    while(len >= 4) {
        uint32_t k = *(uint32_t*)data;

        k *= m;
        k ^= k >> r;
        k *= m;

        h *= m;
        h ^= k;

        data += 4;
        len -= 4;
    }

    /* Handle the last few bytes of the input array  */
    switch(len) {
    case 3: h ^= data[2] << 16;
    case 2: h ^= data[1] << 8;
    case 1: h ^= data[0]; h *= m;
    };

    /* Do a few final mixes of the hash to ensure the last few
     * bytes are well-incorporated. */
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;

    return (unsigned int)h;
}

/* And a case insensitive hash function (based on djb hash) */
unsigned int dictGenCaseHashFunction(const unsigned char *buf, int len) {
    unsigned int hash = (unsigned int)dict_hash_function_seed;

    while (len--)
        hash = ((hash << 5) + hash) + (tolower(*buf++)); /* hash * 33 + c */
    return hash;
}

/* ----------------------------- API implementation ------------------------- */

/*
 * 重置哈希表的各项属性
 *
 * T = O(1)
 */
static void _dictReset(dictht *ht)
{
    ht->table = NULL;
    ht->size = 0;
    ht->sizemask = 0;
    ht->used = 0;
}

/*
 * 创建一个新字典
 *
 * T = O(1)
 */
dict *dictCreate(dictType *type,
        void *privDataPtr)
{
    // 分配空间
    dict *d = zmalloc(sizeof(*d));

    // 初始化字典
    _dictInit(d,type,privDataPtr);

    return d;
}

/*
 * 初始化字典
 *
 * T = O(1)
 */
int _dictInit(dict *d, dictType *type,
        void *privDataPtr)
{
    // 初始化 ht[0]
    _dictReset(&d->ht[0]);

    // 初始化 ht[1]
    _dictReset(&d->ht[1]);

    // 初始化字典属性
    d->type = type;
    d->privdata = privDataPtr;
    d->rehashidx = -1;
    d->iterators = 0;

    return DICT_OK;
}

/* Resize the table to the minimal size that contains all the elements,
 * but with the invariant of a USED/BUCKETS ratio near to <= 1 */
/*
 * 对字典进行紧缩，让节点数/桶数的比率接近 <= 1 。
 *
 * T = O(N)
 */
int dictResize(dict *d)
{
    int minimal;

    // 不能在 dict_can_resize 为假
    // 或者字典正在 rehash 时调用
    if (!dict_can_resize || dictIsRehashing(d)) return DICT_ERR;

    minimal = d->ht[0].used;

    if (minimal < DICT_HT_INITIAL_SIZE)
        minimal = DICT_HT_INITIAL_SIZE;

    return dictExpand(d, minimal);
}

/* Expand or create the hash table */
/*
 * 创建一个新哈希表，并视情况，进行以下动作之一：
 *  
 *   1) 如果字典里的 ht[0] 为空，将新哈希表赋值给它
 *   2) 如果字典里的 ht[0] 不为空，那么将新哈希表赋值给 ht[1] ，并打开 rehash 标识
 *
 * T = O(N)
 */
int dictExpand(dict *d, unsigned long size)
{
    dictht n; /* the new hash table */
    
    // 计算哈希表的真实大小
    // O(N)
    unsigned long realsize = _dictNextPower(size);

    /* the size is invalid if it is smaller than the number of
     * elements already inside the hash table */
    if (dictIsRehashing(d) || d->ht[0].used > size)
        return DICT_ERR;

    /* Allocate the new hash table and initialize all pointers to NULL */
    // 创建并初始化新哈希表
    // O(N)
    n.size = realsize;
    n.sizemask = realsize-1;
    n.table = zcalloc(realsize*sizeof(dictEntry*));
    n.used = 0;

    /* Is this the first initialization? If so it's not really a rehashing
     * we just set the first hash table so that it can accept keys. */
    // 如果 ht[0] 为空，那么这就是一次创建新哈希表行为
    // 将新哈希表设置为 ht[0] ，然后返回
    if (d->ht[0].table == NULL) {
        d->ht[0] = n;
        return DICT_OK;
    }

    /* Prepare a second hash table for incremental rehashing */
    // 如果 ht[0] 不为空，那么这就是一次扩展字典的行为
    // 将新哈希表设置为 ht[1] ，并打开 rehash 标识
    d->ht[1] = n;
    d->rehashidx = 0;

    return DICT_OK;
}

/*
 * 执行 N 步渐进式 rehash 。
 *
 * 如果执行之后哈希表还有元素需要 rehash ，那么返回 1 。
 * 如果哈希表里面所有元素已经迁移完毕，那么返回 0 。
 *
 * 每步 rehash 都会移动哈希表数组内某个索引上的整个链表节点，
 * 所以从 ht[0] 迁移到 ht[1] 的 key 可能不止一个。
 *
 * T = O(N)
 */
int dictRehash(dict *d, int n) {
    if (!dictIsRehashing(d)) return 0;

    while(n--) {
        dictEntry *de, *nextde;

        // 如果 ht[0] 已经为空，那么迁移完毕
        // 用 ht[1] 代替原来的 ht[0]
        if (d->ht[0].used == 0) {

            // 释放 ht[0] 的哈希表数组
            zfree(d->ht[0].table);

            // 将 ht[0] 指向 ht[1]
            d->ht[0] = d->ht[1];

            // 清空 ht[1] 的指针
            _dictReset(&d->ht[1]);

            // 关闭 rehash 标识
            d->rehashidx = -1;

            // 通知调用者， rehash 完毕
            return 0;
        }

        /* Note that rehashidx can't overflow as we are sure there are more
         * elements because ht[0].used != 0 */
        assert(d->ht[0].size > (unsigned)d->rehashidx);
        // 移动到数组中首个不为 NULL 链表的索引上
        while(d->ht[0].table[d->rehashidx] == NULL) d->rehashidx++;
        // 指向链表头
        de = d->ht[0].table[d->rehashidx];
        // 将链表内的所有元素从 ht[0] 迁移到 ht[1]
        // 因为桶内的元素通常只有一个，或者不多于某个特定比率
        // 所以可以将这个操作看作 O(1)
        while(de) {
            unsigned int h;

            nextde = de->next;

            /* Get the index in the new hash table */
            // 计算元素在 ht[1] 的哈希值
            h = dictHashKey(d, de->key) & d->ht[1].sizemask;

            // 添加节点到 ht[1] ，调整指针
            de->next = d->ht[1].table[h];
            d->ht[1].table[h] = de;

            // 更新计数器
            d->ht[0].used--;
            d->ht[1].used++;

            de = nextde;
        }

        // 设置指针为 NULL ，方便下次 rehash 时跳过
        d->ht[0].table[d->rehashidx] = NULL;

        // 前进至下一索引
        d->rehashidx++;
    }

    // 通知调用者，还有元素等待 rehash
    return 1;
}

/*
 * 以毫秒为单位，返回当前时间
 *
 * T = O(1)
 */
long long timeInMilliseconds(void) {
    struct timeval tv;

    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000)+(tv.tv_usec/1000);
}

/* Rehash for an amount of time between ms milliseconds and ms+1 milliseconds */
/*
 * 在给定毫秒数内，以 100 步为单位，对字典进行 rehash 。
 *
 * T = O(N)，N 为被 rehash 的 key-value 对数量
 */
int dictRehashMilliseconds(dict *d, int ms) {
    long long start = timeInMilliseconds();
    int rehashes = 0;

    while(dictRehash(d,100)) {
        rehashes += 100;
        if (timeInMilliseconds()-start > ms) break;
    }
    return rehashes;
}

/*
 * 如果条件允许的话，将一个元素从 ht[0] 迁移至 ht[1]
 *
 * 这个函数被其他查找和更新函数所调用，从而实现渐进式 rehash 。
 *
 * T = O(1)
 */
static void _dictRehashStep(dict *d) {
    // 只在没有安全迭代器的时候，才能进行迁移
    // 否则可能会产生重复元素，或者丢失元素
    if (d->iterators == 0) dictRehash(d,1);
}

/*
 * 添加给定 key-value 对到字典
 *
 * T = O(1)
 */
int dictAdd(dict *d, void *key, void *val)
{
    // 添加 key 到哈希表，返回包含该 key 的节点
    dictEntry *entry = dictAddRaw(d,key);

    // 添加失败？
    if (!entry) return DICT_ERR;

    // 设置节点的值
    dictSetVal(d, entry, val);

    return DICT_OK;
}

/* Low level add. This function adds the entry but instead of setting
 * a value returns the dictEntry structure to the user, that will make
 * sure to fill the value field as he wishes.
 *
 * This function is also directly exposed to the user API to be called
 * mainly in order to store non-pointers inside the hash value, example:
 *
 * entry = dictAddRaw(dict,mykey);
 * if (entry != NULL) dictSetSignedIntegerVal(entry,1000);
 *
 * Return values:
 *
 * If key already exists NULL is returned.
 * If key was added, the hash entry is returned to be manipulated by the caller.
 */
/*
 * 添加 key 到字典的底层实现，完成之后返回新节点。
 *
 * 如果 key 已经存在，返回 NULL 。
 *
 * T = O(1)
 */
dictEntry *dictAddRaw(dict *d, void *key)
{
    int index;
    dictEntry *entry;
    dictht *ht;

    // 尝试渐进式地 rehash 一个元素
    if (dictIsRehashing(d)) _dictRehashStep(d);

    // 查找可容纳新元素的索引位置
    // 如果元素已存在， index 为 -1
    if ((index = _dictKeyIndex(d, key)) == -1)
        return NULL;

    /* Allocate the memory and store the new entry */
    // 决定该把新元素放在那个哈希表
    ht = dictIsRehashing(d) ? &d->ht[1] : &d->ht[0];
    // 为新元素分配节点空间
    entry = zmalloc(sizeof(*entry));
    // 新节点的后继指针指向旧的表头节点
    entry->next = ht->table[index];
    // 设置新节点为表头
    ht->table[index] = entry;
    // 更新已有节点数量
    ht->used++;

    /* Set the hash entry fields. */
    // 关联起节点和 key
    dictSetKey(d, entry, key);

    // 返回新节点
    return entry;
}

/* Add an element, discarding the old if the key already exists.
 * Return 1 if the key was added from scratch, 0 if there was already an
 * element with such key and dictReplace() just performed a value update
 * operation. */
/*
 * 用新的值代替 key 原有的值。
 * 
 * 如果 key 不存在，将关联添加到哈希表中。
 *
 * 如果关联是新创建的，返回 1 ，如果关联是被更新的，返回 0 。
 *
 * T = O(1)
 */
int dictReplace(dict *d, void *key, void *val)
{
    dictEntry *entry, auxentry;

    /* Try to add the element. If the key
     * does not exists dictAdd will suceed. */
    // 尝试添加新元素到哈希表
    // 只要 key 不存在，添加就会成功。
    // O(1)
    if (dictAdd(d, key, val) == DICT_OK)
        return 1;

    // 如果添加失败，那么说明元素已经存在
    // 获取这个元素所对应的节点
    // O(1)
    entry = dictFind(d, key);

    /* Set the new value and free the old one. Note that it is important
     * to do that in this order, as the value may just be exactly the same
     * as the previous one. In this context, think to reference counting,
     * you want to increment (set), and then decrement (free), and not the
     * reverse. */
    auxentry = *entry;          // 指向旧值
    dictSetVal(d, entry, val);  // 设置新值
    dictFreeVal(d, &auxentry);  // 释放旧值

    return 0;
}

/* dictReplaceRaw() is simply a version of dictAddRaw() that always
 * returns the hash entry of the specified key, even if the key already
 * exists and can't be added (in that case the entry of the already
 * existing key is returned.)
 *
 * See dictAddRaw() for more information. */
/*
 * 类似于 dictAddRaw() ，
 * dictReplaceRaw 无论在新添加节点还是更新节点的情况下，
 * 都返回 key 所对应的节点
 *
 * T = O(1)
 */
dictEntry *dictReplaceRaw(dict *d, void *key) {
    // 查找
    dictEntry *entry = dictFind(d,key);

    // 没找到就添加，找到直接返回
    return entry ? entry : dictAddRaw(d,key);
}

/*
 * 按 key 查找并删除节点
 *
 * T = O(1)
 */
static int dictGenericDelete(dict *d, const void *key, int nofree)
{
    unsigned int h, idx;
    dictEntry *he, *prevHe;
    int table;

    // 空表？
    if (d->ht[0].size == 0) return DICT_ERR; /* d->ht[0].table is NULL */

    // 渐进式 rehash
    if (dictIsRehashing(d)) _dictRehashStep(d);

    // 计算哈希值
    h = dictHashKey(d, key);

    // 在两个哈希表中查找
    for (table = 0; table <= 1; table++) {
        // 索引值
        idx = h & d->ht[table].sizemask;
        // 索引在数组中对应的表头
        he = d->ht[table].table[idx];
        prevHe = NULL;
        // 遍历链表
        // 因为链表的元素数量通常为 1 ，或者维持在一个很小的比率
        // 因此可以将这个操作看作 O(1)
        while(he) {
            // 对比
            if (dictCompareKeys(d, key, he->key)) {
                /* Unlink the element from the list */
                if (prevHe)
                    prevHe->next = he->next;
                else
                    d->ht[table].table[idx] = he->next;
                // 释放节点的键和值
                if (!nofree) {
                    dictFreeKey(d, he);
                    dictFreeVal(d, he);
                }
                // 释放节点
                zfree(he);
                
                d->ht[table].used--;

                return DICT_OK;
            }
            prevHe = he;
            he = he->next;
        }

        // 如果不是正在进行 rehash ，
        // 那么无须遍历 ht[1] 
        if (!dictIsRehashing(d)) break;
    }

    return DICT_ERR; /* not found */
}

/*
 * 删除哈希表中的 key ，并且释放保存这个 key 的节点
 *
 * T = O(1)
 */
int dictDelete(dict *ht, const void *key) {
    return dictGenericDelete(ht,key,0);
}

/*
 * 删除哈希表中的 key ，但是并不释放保存这个 key 的节点
 *
 * T = O(1)
 */
int dictDeleteNoFree(dict *ht, const void *key) {
    return dictGenericDelete(ht,key,1);
}

/* Destroy an entire dictionary */
/*
 * 销毁给定哈希表
 *
 * T = O(N)
 */
int _dictClear(dict *d, dictht *ht)
{
    unsigned long i;

    /* Free all the elements */
    // 遍历哈希表数组
    for (i = 0; i < ht->size && ht->used > 0; i++) {
        dictEntry *he, *nextHe;

        if ((he = ht->table[i]) == NULL) continue;
        // 释放整个链表上的元素
        // 因为链表的元素数量通常为 1 ，或者维持在一个很小的比率
        // 因此可以将这个操作看作 O(1)
        while(he) {
            nextHe = he->next;

            dictFreeKey(d, he);
            dictFreeVal(d, he);

            zfree(he);

            ht->used--;

            he = nextHe;
        }
    }

    /* Free the table and the allocated cache structure */
    zfree(ht->table);

    /* Re-initialize the table */
    _dictReset(ht);

    return DICT_OK; /* never fails */
}

/*
 * 清空并释放字典
 *
 * T = O(N)
 */
void dictRelease(dict *d)
{
    _dictClear(d,&d->ht[0]);
    _dictClear(d,&d->ht[1]);

    zfree(d);
}

/*
 * 在字典中查找给定 key 所定义的节点
 *
 * 如果 key 不存在，返回 NULL
 *
 * T = O(1)
 */
dictEntry *dictFind(dict *d, const void *key)
{
    dictEntry *he;
    unsigned int h, idx, table;

    if (d->ht[0].size == 0) return NULL; /* We don't have a table at all */

    if (dictIsRehashing(d)) _dictRehashStep(d);
    
    // 计算哈希值
    h = dictHashKey(d, key);
    // 在两个哈希表中查找
    for (table = 0; table <= 1; table++) {
        // 索引值
        idx = h & d->ht[table].sizemask;
        // 节点链表
        he = d->ht[table].table[idx];
        // 在链表中查找
        // 因为链表的元素数量通常为 1 ，或者维持在一个很小的比率
        // 因此可以将这个操作看作 O(1)
        while(he) {
            // 找到并返回
            if (dictCompareKeys(d, key, he->key))
                return he;

            he = he->next;
        }

        // 如果 rehash 并不在进行中
        // 那么无须查找 ht[1]
        if (!dictIsRehashing(d)) return NULL;
    }

    return NULL;
}

/*
 * 返回在字典中， key 所对应的值 value
 *
 * 如果 key 不存在于字典，那么返回 NULL
 *
 * T = O(1)
 */
void *dictFetchValue(dict *d, const void *key) {
    dictEntry *he;

    he = dictFind(d,key);

    return he ? dictGetVal(he) : NULL;
}

/*
 * 根据给定字典，创建一个不安全迭代器。
 *
 * T = O(1)
 */
dictIterator *dictGetIterator(dict *d)
{
    dictIterator *iter = zmalloc(sizeof(*iter));

    iter->d = d;
    iter->table = 0;
    iter->index = -1;
    iter->safe = 0;
    iter->entry = NULL;
    iter->nextEntry = NULL;

    return iter;
}

/*
 * 根据给定字典，创建一个安全迭代器。
 *
 * T = O(1)
 */
dictIterator *dictGetSafeIterator(dict *d) {
    dictIterator *i = dictGetIterator(d);

    i->safe = 1;
    return i;
}

/*
 * 返回迭代器指向的当前节点。
 *
 * 如果字典已经迭代完毕，返回 NULL 。
 *
 * T = O(1)
 */
dictEntry *dictNext(dictIterator *iter)
{
    while (1) {
        if (iter->entry == NULL) {

            dictht *ht = &iter->d->ht[iter->table];

            // 在开始迭代之前，增加字典 iterators 计数器的值
            // 只有安全迭代器才会增加计数
            if (iter->safe &&
                iter->index == -1 &&
                iter->table == 0)
                iter->d->iterators++;

            // 增加索引
            iter->index++;

            // 当迭代的元素数量超过 ht->size 的值
            // 说明这个表已经迭代完毕了
            if (iter->index >= (signed) ht->size) {
                // 是否接着迭代 ht[1] ?
                if (dictIsRehashing(iter->d) && iter->table == 0) {
                    iter->table++;
                    iter->index = 0;
                    ht = &iter->d->ht[1];
                } else {
                // 如果没有 ht[1] ，或者已经迭代完了 ht[1] 到达这里
                // 跳出
                    break;
                }
            }

            // 指向下一索引的节点链表
            iter->entry = ht->table[iter->index];

        } else {
            // 指向链表的下一节点
            iter->entry = iter->nextEntry;
        }

        // 保存后继指针 nextEntry，
        // 以应对当前节点 entry 可能被修改的情况
        if (iter->entry) {
            /* We need to save the 'next' here, the iterator user
             * may delete the entry we are returning. */
            iter->nextEntry = iter->entry->next;
            return iter->entry;
        }
    }
    return NULL;
}

/*
 * 释放迭代器
 *
 * T = O(1)
 */
void dictReleaseIterator(dictIterator *iter)
{
    if (iter->safe && !(iter->index == -1 && iter->table == 0))
        iter->d->iterators--;

    zfree(iter);
}

/*
 * 从字典中返回一个随机节点。
 *
 * 可用于实现随机化算法。
 *
 * 如果字典为空，返回 NULL 。
 *
 * T = O(N)
 */
dictEntry *dictGetRandomKey(dict *d)
{
    dictEntry *he, *orighe;
    unsigned int h;
    int listlen, listele;

    // 空表，返回 NULL
    if (dictSize(d) == 0) return NULL;

    // 渐进式 rehash
    if (dictIsRehashing(d)) _dictRehashStep(d);

    // 根据哈希表的使用情况，随机从哈希表中挑选一个非空表头
    // O(N)
    if (dictIsRehashing(d)) {
        do {
            h = random() % (d->ht[0].size+d->ht[1].size);
            he = (h >= d->ht[0].size) ? d->ht[1].table[h - d->ht[0].size] :
                                      d->ht[0].table[h];
        } while(he == NULL);
    } else {
        do {
            h = random() & d->ht[0].sizemask;
            he = d->ht[0].table[h];
        } while(he == NULL);
    }

    /* Now we found a non empty bucket, but it is a linked
     * list and we need to get a random element from the list.
     * The only sane way to do so is counting the elements and
     * select a random index. */
    // 随机获取链表中的其中一个元素
    // 计算链表长度
    // 因为链表的元素数量通常为 1 或者一个很小的比率
    // 所以这个操作可以看作是 O(1)
    listlen = 0;
    orighe = he;
    while(he) {
        he = he->next;
        listlen++;
    }
    // 计算随机值
    listele = random() % listlen;

    // 取出对应节点
    he = orighe;
    while(listele--) he = he->next;

    // 返回
    return he;
}

/* ------------------------- private functions ------------------------------ */

/* Expand the hash table if needed */
/*
 * 根据需要，扩展字典的大小
 * （也即是对 ht[0] 进行 rehash）
 *
 * T = O(N)
 */
static int _dictExpandIfNeeded(dict *d)
{
    // 已经在渐进式 rehash 当中，直接返回
    if (dictIsRehashing(d)) return DICT_OK;

    // 如果哈希表为空，那么将它扩展为初始大小
    // O(N)
    if (d->ht[0].size == 0) return dictExpand(d, DICT_HT_INITIAL_SIZE);

    /* If we reached the 1:1 ratio, and we are allowed to resize the hash
     * table (global setting) or we should avoid it but the ratio between
     * elements/buckets is over the "safe" threshold, we resize doubling
     * the number of buckets. */    
    // 如果哈希表的已用节点数 >= 哈希表的大小，
    // 并且以下条件任一个为真：
    //   1) dict_can_resize 为真
    //   2) 已用节点数除以哈希表大小之比大于 
    //      dict_force_resize_ratio
    // 那么调用 dictExpand 对哈希表进行扩展
    // 扩展的体积至少为已使用节点数的两倍
    // O(N)
    if (d->ht[0].used >= d->ht[0].size &&
        (dict_can_resize ||
         d->ht[0].used/d->ht[0].size > dict_force_resize_ratio))
    {
        return dictExpand(d, d->ht[0].used*2);
    }

    return DICT_OK;
}

/*
 * 计算哈希表的真实体积
 *
 * 如果 size 小于等于 DICT_HT_INITIAL_SIZE ，
 * 那么返回 DICT_HT_INITIAL_SIZE ，
 * 否则这个值为第一个 >= size 的二次幂。
 *
 * T = O(N)
 */
static unsigned long _dictNextPower(unsigned long size)
{
    unsigned long i = DICT_HT_INITIAL_SIZE;

    if (size >= LONG_MAX) return LONG_MAX;
    while(1) {
        if (i >= size)
            return i;
        i *= 2;
    }
}

/* Returns the index of a free slot that can be populated with
 * an hash entry for the given 'key'.
 * If the key already exists, -1 is returned.
 *
 * Note that if we are in the process of rehashing the hash table, the
 * index is always returned in the context of the second (new) hash table. */
/*
 * 返回给定 key 可以哈希表数组存放的索引。
 *
 * 如果 key 已经存在于哈希表，返回 -1 。
 *
 * 当正在执行 rehash 的时候，
 * 返回的 index 总是应用于第二个（新的）哈希表
 *
 * T = O(1)
 */
static int _dictKeyIndex(dict *d, const void *key)
{
    unsigned int h, idx, table;
    dictEntry *he;

    // 如果有需要，对字典进行扩展
    if (_dictExpandIfNeeded(d) == DICT_ERR)
        return -1;

    // 计算 key 的哈希值
    h = dictHashKey(d, key);

    // 在两个哈希表中进行查找给定 key
    for (table = 0; table <= 1; table++) {

        // 根据哈希值和哈希表的 sizemask 
        // 计算出 key 可能出现在 table 数组中的哪个索引
        idx = h & d->ht[table].sizemask;

        // 在节点链表里查找给定 key
        // 因为链表的元素数量通常为 1 或者是一个很小的比率
        // 所以可以将这个操作看作 O(1) 来处理
        he = d->ht[table].table[idx];
        while(he) {
            // key 已经存在
            if (dictCompareKeys(d, key, he->key))
                return -1;

            he = he->next;
        }

        // 第一次进行运行到这里时，说明已经查找完 d->ht[0] 了
        // 这时如果哈希表不在 rehash 当中，就没有必要查找 d->ht[1]
        if (!dictIsRehashing(d)) break;
    }

    return idx;
}

/*
 * 清空整个字典
 *
 * T = O(N)
 */
void dictEmpty(dict *d) {
    _dictClear(d,&d->ht[0]);
    _dictClear(d,&d->ht[1]);
    d->rehashidx = -1;
    d->iterators = 0;
}

/*
 * 打开 rehash 标识
 *
 * T = O(1)
 */
void dictEnableResize(void) {
    dict_can_resize = 1;
}

/*
 * 关闭 rehash 标识
 *
 * T = O(1)
 */
void dictDisableResize(void) {
    dict_can_resize = 0;
}

#if 0

/* The following is code that we don't use for Redis currently, but that is part
of the library. */

/* ----------------------- Debugging ------------------------*/

#define DICT_STATS_VECTLEN 50
static void _dictPrintStatsHt(dictht *ht) {
    unsigned long i, slots = 0, chainlen, maxchainlen = 0;
    unsigned long totchainlen = 0;
    unsigned long clvector[DICT_STATS_VECTLEN];

    if (ht->used == 0) {
        printf("No stats available for empty dictionaries\n");
        return;
    }

    for (i = 0; i < DICT_STATS_VECTLEN; i++) clvector[i] = 0;
    for (i = 0; i < ht->size; i++) {
        dictEntry *he;

        if (ht->table[i] == NULL) {
            clvector[0]++;
            continue;
        }
        slots++;
        /* For each hash entry on this slot... */
        chainlen = 0;
        he = ht->table[i];
        while(he) {
            chainlen++;
            he = he->next;
        }
        clvector[(chainlen < DICT_STATS_VECTLEN) ? chainlen : (DICT_STATS_VECTLEN-1)]++;
        if (chainlen > maxchainlen) maxchainlen = chainlen;
        totchainlen += chainlen;
    }
    printf("Hash table stats:\n");
    printf(" table size: %ld\n", ht->size);
    printf(" number of elements: %ld\n", ht->used);
    printf(" different slots: %ld\n", slots);
    printf(" max chain length: %ld\n", maxchainlen);
    printf(" avg chain length (counted): %.02f\n", (float)totchainlen/slots);
    printf(" avg chain length (computed): %.02f\n", (float)ht->used/slots);
    printf(" Chain length distribution:\n");
    for (i = 0; i < DICT_STATS_VECTLEN-1; i++) {
        if (clvector[i] == 0) continue;
        printf("   %s%ld: %ld (%.02f%%)\n",(i == DICT_STATS_VECTLEN-1)?">= ":"", i, clvector[i], ((float)clvector[i]/ht->size)*100);
    }
}

void dictPrintStats(dict *d) {
    _dictPrintStatsHt(&d->ht[0]);
    if (dictIsRehashing(d)) {
        printf("-- Rehashing into ht[1]:\n");
        _dictPrintStatsHt(&d->ht[1]);
    }
}

/* ----------------------- StringCopy Hash Table Type ------------------------*/

static unsigned int _dictStringCopyHTHashFunction(const void *key)
{
    return dictGenHashFunction(key, strlen(key));
}

static void *_dictStringDup(void *privdata, const void *key)
{
    int len = strlen(key);
    char *copy = zmalloc(len+1);
    DICT_NOTUSED(privdata);

    memcpy(copy, key, len);
    copy[len] = '\0';
    return copy;
}

static int _dictStringCopyHTKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    DICT_NOTUSED(privdata);

    return strcmp(key1, key2) == 0;
}

static void _dictStringDestructor(void *privdata, void *key)
{
    DICT_NOTUSED(privdata);

    zfree(key);
}

dictType dictTypeHeapStringCopyKey = {
    _dictStringCopyHTHashFunction, /* hash function */
    _dictStringDup,                /* key dup */
    NULL,                          /* val dup */
    _dictStringCopyHTKeyCompare,   /* key compare */
    _dictStringDestructor,         /* key destructor */
    NULL                           /* val destructor */
};

/* This is like StringCopy but does not auto-duplicate the key.
 * It's used for intepreter's shared strings. */
dictType dictTypeHeapStrings = {
    _dictStringCopyHTHashFunction, /* hash function */
    NULL,                          /* key dup */
    NULL,                          /* val dup */
    _dictStringCopyHTKeyCompare,   /* key compare */
    _dictStringDestructor,         /* key destructor */
    NULL                           /* val destructor */
};

/* This is like StringCopy but also automatically handle dynamic
 * allocated C strings as values. */
dictType dictTypeHeapStringCopyKeyValue = {
    _dictStringCopyHTHashFunction, /* hash function */
    _dictStringDup,                /* key dup */
    _dictStringDup,                /* val dup */
    _dictStringCopyHTKeyCompare,   /* key compare */
    _dictStringDestructor,         /* key destructor */
    _dictStringDestructor,         /* val destructor */
};
#endif
