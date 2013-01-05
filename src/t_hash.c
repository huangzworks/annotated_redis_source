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
#include <math.h>

/*-----------------------------------------------------------------------------
 * Hash type API
 *----------------------------------------------------------------------------*/

/*
 * 对 argv 数组中的对象进行检查，
 * 看保存它们是否需要将 o 的编码从
 * REDIS_ENCODING_ZIPLIST 转换为 REDIS_ENCODING_HT
 *
 * 复杂度：O(N)
 *
 * 返回值：void
 */
void hashTypeTryConversion(robj *o, robj **argv, int start, int end) {
    int i;

    // 如果对象不是 ziplist 编码（的hash），直接返回
    if (o->encoding != REDIS_ENCODING_ZIPLIST) return;

    // 检查所有字符串参数的长度，看是否超过 server.hash_max_ziplist_value
    // 如果有一个结果为真的话，就对 o 进行转换
    for (i = start; i <= end; i++) {
        if (argv[i]->encoding == REDIS_ENCODING_RAW &&
            sdslen(argv[i]->ptr) > server.hash_max_ziplist_value)
        {
            // 转换
            hashTypeConvert(o, REDIS_ENCODING_HT);
            break;
        }
    }
}

/*
 * 当 subject 是 HASH 编码时，尝试对 o1 和 o2 进行就地(in-place)编码
 * 该编码可以尝试节省字符串的空间
 *
 * 复杂度：O(1)
 *
 * 返回值：void
 */
void hashTypeTryObjectEncoding(robj *subject, robj **o1, robj **o2) {
    if (subject->encoding == REDIS_ENCODING_HT) {
        if (o1) *o1 = tryObjectEncoding(*o1);
        if (o2) *o2 = tryObjectEncoding(*o2);
    }
}

/* Get the value from a ziplist encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
/*
 * 从 ziplist 中取出和 field 相对应的值
 *
 * 复杂度：O(n)
 *
 * 参数：
 *  field   域
 *  vstr    值是字符串时，将它保存到这个指针
 *  vlen    保存字符串的长度
 *  ll      值时整数时，将它保存到这个指针
 *
 * 返回值：
 *  查找失败返回 -1 ，否则返回 0 。
 */
int hashTypeGetFromZiplist(robj *o, robj *field,
                           unsigned char **vstr,
                           unsigned int *vlen,
                           long long *vll)
{
    unsigned char *zl,
                  *fptr = NULL,
                  *vptr = NULL;
    int ret;

    redisAssert(o->encoding == REDIS_ENCODING_ZIPLIST);
    
    // 解码域，因为 ziplist 不能使用对象
    field = getDecodedObject(field);

    // 遍历 ziplist ，定位域的位置
    zl = o->ptr;
    fptr = ziplistIndex(zl, ZIPLIST_HEAD);
    if (fptr != NULL) {
        // 定位域节点的位置
        fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
        if (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */
            // 定位值节点的位置
            vptr = ziplistNext(zl, fptr);
            redisAssert(vptr != NULL);
        }
    }

    decrRefCount(field);

    // 从 ziplist 节点中取出值
    if (vptr != NULL) {
        ret = ziplistGet(vptr, vstr, vlen, vll);
        redisAssert(ret);
        return 0;
    }

    return -1;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
/*
 * 从 HT 编码的哈希表中获取给定 field 的值。
 *
 * 复杂度：O(1)
 *
 * 参数：
 *  field   域
 *  value   保存值对象的指针
 *
 * 返回值：
 *  找到值时返回 0 ，没找到返回 -1 。
 */
int hashTypeGetFromHashTable(robj *o, robj *field, robj **value) {
    dictEntry *de;

    redisAssert(o->encoding == REDIS_ENCODING_HT);

    de = dictFind(o->ptr, field);
    if (de == NULL) return -1;
    *value = dictGetVal(de);
    return 0;
}

/* Higher level function of hashTypeGet*() that always returns a Redis
 * object (either new or with refcount incremented), so that the caller
 * can retain a reference or call decrRefCount after the usage.
 *
 * The lower level function can prevent copy on write so it is
 * the preferred way of doing read operations. */
/*
 * 多态获取函数
 *
 * 复杂度：O(N)
 *
 * 返回值：
 *  redisObject 保存了 field 域在哈希表中对应的值
 */
robj *hashTypeGetObject(robj *o, robj *field) {
    robj *value = NULL;

    // 从 ziplist 中获取
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) {
            if (vstr) {
                // 将字面值包装成对象再返回
                value = createStringObject((char*)vstr, vlen);
            } else {
                value = createStringObjectFromLongLong(vll);
            }
        }

    // 从字典中获取
    } else if (o->encoding == REDIS_ENCODING_HT) {
        robj *aux;

        if (hashTypeGetFromHashTable(o, field, &aux) == 0) {
            incrRefCount(aux);
            value = aux;
        }

    // 编码错误
    } else {
        redisPanic("Unknown hash encoding");
    }

    return value;
}

/* Test if the specified field exists in the given hash. Returns 1 if the field
 * exists, and 0 when it doesn't. */
/*
 * 检查给定 field 是否存在于 hash 对象 o 中。
 *
 * 复杂度：O(N)
 *
 * 返回值：
 *  存在返回 1 ， 否则返回 0 。
 */
int hashTypeExists(robj *o, robj *field) {

    // 检查 ziplist
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) return 1;

    // 检查字典
    } else if (o->encoding == REDIS_ENCODING_HT) {
        robj *aux;

        if (hashTypeGetFromHashTable(o, field, &aux) == 0) return 1;

    // 编码错误
    } else {
        redisPanic("Unknown hash encoding");
    }

    return 0;
}

/*
 * 将给定的 field-value pair 添加到 hash 。
 *
 * 这个函数负责对 field 和 value 参数进行引用计数自增。
 *
 * 复杂度：O(N)
 *
 * 返回值：
 *  返回 0 表示元素已存在，它的值已被更新。
 *  返回 1 表示元素是新添加的。
 */
int hashTypeSet(robj *o, robj *field, robj *value) {
    int update = 0;
    
    // 添加到 ziplist
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr, *vptr;

        // 解码成字符串或者数字
        field = getDecodedObject(field);
        value = getDecodedObject(value);

        // 遍历整个 ziplist ，尝试查找并更新 field （如果它已经存在）
        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            // 定位到域，O(N)
            fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                // 定位到值
                vptr = ziplistNext(zl, fptr);
                redisAssert(vptr != NULL);

                // 标识这次操作为更新操作
                update = 1;

                // 删除旧值
                zl = ziplistDelete(zl, &vptr);

                // 插入新值
                zl = ziplistInsert(zl, vptr, value->ptr, sdslen(value->ptr));
            }
        }

        // 如果这不是更新操作，那么这就是一个添加操作
        if (!update) {
            // 将新的域/值对 push 到 ziplist 的末尾
            zl = ziplistPush(zl, field->ptr, sdslen(field->ptr), ZIPLIST_TAIL);
            zl = ziplistPush(zl, value->ptr, sdslen(value->ptr), ZIPLIST_TAIL);
        }
        o->ptr = zl;
        decrRefCount(field);
        decrRefCount(value);

        /* Check if the ziplist needs to be converted to a hash table */
        if (hashTypeLength(o) > server.hash_max_ziplist_entries)
            hashTypeConvert(o, REDIS_ENCODING_HT);

    // 添加到字典，O(1)
    } else if (o->encoding == REDIS_ENCODING_HT) {
        if (dictReplace(o->ptr, field, value)) { /* Insert */
            incrRefCount(field);
        } else { /* Update */
            update = 1;
        }
        incrRefCount(value);

    // 编码错误
    } else {
        redisPanic("Unknown hash encoding");
    }

    return update;
}

/* Delete an element from a hash.
 * Return 1 on deleted and 0 on not found. */
/*
 * 将给定 field 及其 value 从哈希表中删除
 *
 * 复杂度：
 *  O(N)
 *
 * 返回值：
 *  删除成功返回 1 ，否则返回 0 。
 */
int hashTypeDelete(robj *o, robj *field) {
    int deleted = 0;

    // ziplist
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr;

        field = getDecodedObject(field);

        // 遍历 ziplist ，尝试删除 field-value 对
        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            fptr = ziplistFind(fptr, field->ptr, sdslen(field->ptr), 1);
            // 找到目标 field
            if (fptr != NULL) {
                zl = ziplistDelete(zl,&fptr);
                zl = ziplistDelete(zl,&fptr);
                o->ptr = zl;
                deleted = 1;
            }
        }

        decrRefCount(field);

    } else if (o->encoding == REDIS_ENCODING_HT) {
        if (dictDelete((dict*)o->ptr, field) == REDIS_OK) {
            deleted = 1;

            /* Always check if the dictionary needs a resize after a delete. */
            if (htNeedsResize(o->ptr)) dictResize(o->ptr);
        }

    } else {
        redisPanic("Unknown hash encoding");
    }

    return deleted;
}

/* Return the number of elements in a hash. */
/*
 * 返回哈希表的 field-value 对数量
 *
 * 复杂度：O(1)
 *
 * 返回值：
 *  length
 */
unsigned long hashTypeLength(robj *o) {
    unsigned long length = ULONG_MAX;

    // ziplist
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        // 一个 field-value 对占用两个节点
        length = ziplistLen(o->ptr) / 2;

    // dict
    } else if (o->encoding == REDIS_ENCODING_HT) {
        length = dictSize((dict*)o->ptr);
    
    // wrong encoding
    } else {
        redisPanic("Unknown hash encoding");
    }

    return length;
}

/*
 * 创建一个哈希类型的迭代器
 * hashTypeIterator 类型定义在 redis.h
 *
 * 复杂度：O(1)
 *
 * 返回值：
 *  hashTypeIterator
 */
hashTypeIterator *hashTypeInitIterator(robj *subject) {
    hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));
    // 保存对象和编码方式
    hi->subject = subject;
    hi->encoding = subject->encoding;

    // ziplist 编码
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        hi->fptr = NULL;
        hi->vptr = NULL;

    // dict 编码
    } else if (hi->encoding == REDIS_ENCODING_HT) {
        hi->di = dictGetIterator(subject->ptr);

    } else {
        redisPanic("Unknown hash encoding");
    }

    return hi;
}

/*
 * 释放迭代器
 *
 * 复杂度：O(1)
 *
 * 返回值：void
 */
void hashTypeReleaseIterator(hashTypeIterator *hi) {
    // 释放字典迭代器
    if (hi->encoding == REDIS_ENCODING_HT) {
        dictReleaseIterator(hi->di);
    }

    // 释放 ziplist 的迭代器
    zfree(hi);
}

/* Move to the next entry in the hash. Return REDIS_OK when the next entry
 * could be found and REDIS_ERR when the iterator reaches the end. */
/*
 * 获取 hash 中的下一个节点，并将它保存到迭代器。
 *
 * 复杂度：O(1)
 *
 * 返回值：
 *  如果获取成功，返回 REDIS_OK ，
 *  如果已经没有元素可获取，那么返回 REDIS_ERR 。
 */
int hashTypeNext(hashTypeIterator *hi) {
    // 迭代 ziplist 
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        // 第一次执行时，初始化指针
        if (fptr == NULL) {
            /* Initialize cursor */
            redisAssert(vptr == NULL);
            fptr = ziplistIndex(zl, 0);
       
        // 获取下一个迭代节点
        } else {
            /* Advance cursor */
            redisAssert(vptr != NULL);
            fptr = ziplistNext(zl, vptr);
        }
        // 迭代完
        if (fptr == NULL) return REDIS_ERR;

        /* Grab pointer to the value (fptr points to the field) */
        vptr = ziplistNext(zl, fptr);
        redisAssert(vptr != NULL);

        /* fptr, vptr now point to the first or next pair */
        hi->fptr = fptr;
        hi->vptr = vptr;

    // 迭代字典
    } else if (hi->encoding == REDIS_ENCODING_HT) {
        if ((hi->de = dictNext(hi->di)) == NULL) return REDIS_ERR;

    // wrong encoding
    } else {
        redisPanic("Unknown hash encoding");
    }

    return REDIS_OK;
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromZiplist`. */
/*
 * 根据迭代器的指针，从 ziplist 中取出所指向的节点 field 或者 value 。
 *
 * 复杂度：O(1)
 */
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what,
                                unsigned char **vstr,
                                unsigned int *vlen,
                                long long *vll)
{
    int ret;

    redisAssert(hi->encoding == REDIS_ENCODING_ZIPLIST);

    if (what & REDIS_HASH_KEY) {
        ret = ziplistGet(hi->fptr, vstr, vlen, vll);
        redisAssert(ret);
    } else {
        ret = ziplistGet(hi->vptr, vstr, vlen, vll);
        redisAssert(ret);
    }
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromHashTable`. */
/*
 * 根据迭代器的指针，从字典中取出所指向节点的 field 或者 value 。
 *
 * 复杂度：O(1)
 */
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, robj **dst) {
    redisAssert(hi->encoding == REDIS_ENCODING_HT);

    if (what & REDIS_HASH_KEY) {
        *dst = dictGetKey(hi->de);
    } else {
        *dst = dictGetVal(hi->de);
    }
}

/* A non copy-on-write friendly but higher level version of hashTypeCurrent*()
 * that returns an object with incremented refcount (or a new object). It is up
 * to the caller to decrRefCount() the object if no reference is retained. */
/*
 * 从迭代器中取出当前值
 *
 * 复杂度：O(1)
 *
 * 返回值： robj 一个保存当前值的对象
 */
robj *hashTypeCurrentObject(hashTypeIterator *hi, int what) {
    robj *dst;

    // ziplist
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr) {
            // 总是返回值对象
            dst = createStringObject((char*)vstr, vlen);
        } else {
            dst = createStringObjectFromLongLong(vll);
        }
    
    // 字典
    } else if (hi->encoding == REDIS_ENCODING_HT) {
        hashTypeCurrentFromHashTable(hi, what, &dst);
        incrRefCount(dst);

    } else {
        redisPanic("Unknown hash encoding");
    }

    return dst;
}

/*
 * 按 key 查找 hash 对象
 *
 * 复杂度：O(1)
 *
 * 返回值：
 *  如果对象不存在，就创建一个新的 hash 并返回它。
 *  如果对象不是 hash ，那么返回错误。
 */
robj *hashTypeLookupWriteOrCreate(redisClient *c, robj *key) {
    // 在数据库中查找
    robj *o = lookupKeyWrite(c->db,key);

    // o 不存在，创建一个新的 hash
    if (o == NULL) {
    
        o = createHashObject();
        dbAdd(c->db,key,o);
    // o 存在
    } else {
        // o 不是 hash ，错误
        if (o->type != REDIS_HASH) {
            addReply(c,shared.wrongtypeerr);
            return NULL;
        }
    }

    return o;
}

/*
 * 将一个 ziplist 编码的哈希对象 o 转换成其他编码
 * （比如 dict）
 *
 * 复杂度：O(N)
 */
void hashTypeConvertZiplist(robj *o, int enc) {
    redisAssert(o->encoding == REDIS_ENCODING_ZIPLIST);

    if (enc == REDIS_ENCODING_ZIPLIST) {
        /* Nothing to do... */

    } else if (enc == REDIS_ENCODING_HT) {
        hashTypeIterator *hi;
        dict *dict;
        int ret;

        // 创建 o 的迭代器
        hi = hashTypeInitIterator(o);
        // 创建新字典
        dict = dictCreate(&hashDictType, NULL);

        // 遍历整个 ziplist 
        while (hashTypeNext(hi) != REDIS_ERR) {
            robj *field, *value;

            // 取出 ziplist 里的键
            field = hashTypeCurrentObject(hi, REDIS_HASH_KEY);
            field = tryObjectEncoding(field);

            // 取出 ziplist 里的值
            value = hashTypeCurrentObject(hi, REDIS_HASH_VALUE);
            value = tryObjectEncoding(value);

            // 将键值对添加到字典
            ret = dictAdd(dict, field, value);
            if (ret != DICT_OK) {
                redisLogHexDump(REDIS_WARNING,"ziplist with dup elements dump",
                    o->ptr,ziplistBlobLen(o->ptr));
                redisAssert(ret == DICT_OK);
            }
        }

        // 释放 ziplist 的迭代器
        hashTypeReleaseIterator(hi);
        // 释放 ziplist
        zfree(o->ptr);

        // 更新 key 对象的编码和值
        o->encoding = REDIS_ENCODING_HT;
        o->ptr = dict;

    } else {
        redisPanic("Unknown hash encoding");
    }
}

/*
 * 对 hash 对象 o 的编码方式进行转换
 *
 * 目前只支持从 ziplist 转换为 dict
 *
 * 复杂度：O(N)
 */
void hashTypeConvert(robj *o, int enc) {
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        hashTypeConvertZiplist(o, enc);
    } else if (o->encoding == REDIS_ENCODING_HT) {
        redisPanic("Not implemented");
    } else {
        redisPanic("Unknown hash encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Hash type commands
 *----------------------------------------------------------------------------*/

/*
 * HSET 命令的实现
 *
 * T = O(N)
 */
void hsetCommand(redisClient *c) {
    int update;
    robj *o;

    // 查找 hash
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    // 根据输入参数，如果需要的话，将 o 转换为 dict 编码
    hashTypeTryConversion(o,c->argv,2,3);

    // 编码 field 和 value 以节省空间
    hashTypeTryObjectEncoding(o,&c->argv[2], &c->argv[3]);

    // 设置 field 和 value 到 hash
    // O(N)
    update = hashTypeSet(o,c->argv[2],c->argv[3]);

    // 返回状态：更新/新添加
    addReply(c, update ? shared.czero : shared.cone);

    signalModifiedKey(c->db,c->argv[1]);

    server.dirty++;
}

/*
 * HSETNX 命令的实现
 *
 * T = O(N)
 */
void hsetnxCommand(redisClient *c) {
    robj *o;
    // 创建或查找给定 key 对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    // 对输入参数进行检查，看是否需要将 o 转换为字典编码
    // O(N)
    hashTypeTryConversion(o,c->argv,2,3);

    if (hashTypeExists(o, c->argv[2])) {
        // 如果 field 已存在，直接返回
        addReply(c, shared.czero);
    } else {
        // 未存在，进行设置

        hashTypeTryObjectEncoding(o,&c->argv[2], &c->argv[3]);

        hashTypeSet(o,c->argv[2],c->argv[3]);

        addReply(c, shared.cone);

        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;
    }
}

/*
 * HMSET 命令的实现
 *
 * T = O(N^2)
 */
void hmsetCommand(redisClient *c) {
    int i;
    robj *o;

    // 参数的个数必须是成双成对的
    if ((c->argc % 2) == 1) {
        addReplyError(c,"wrong number of arguments for HMSET");
        return;
    }

    // 查找或创建 hash 对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    // 检查所有输入参数，看是否需要将 hash 的编码转换为字典
    hashTypeTryConversion(o,c->argv,2,c->argc-1);

    // 批量插入
    for (i = 2; i < c->argc; i += 2) {
        // 对 field 和 value 进行编码
        hashTypeTryObjectEncoding(o,&c->argv[i], &c->argv[i+1]);
        // 设置，O(N)
        hashTypeSet(o,c->argv[i],c->argv[i+1]);
    }

    addReply(c, shared.ok);

    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

/*
 * HINCRBY 命令的实现
 *
 * T = O(N)
 */
void hincrbyCommand(redisClient *c) {
    long long value, incr, oldvalue;
    robj *o, *current, *new;

    // 取出增量参数
    if (getLongLongFromObjectOrReply(c,c->argv[3],&incr,NULL) != REDIS_OK) return;

    // 查找或创建 hash 对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    // field 已存在？，O(N)
    if ((current = hashTypeGetObject(o,c->argv[2])) != NULL) {
        // 获取 field 的值
        if (getLongLongFromObjectOrReply(c,current,&value,
            "hash value is not an integer") != REDIS_OK) {
            decrRefCount(current);
            return;
        }
        decrRefCount(current);
    } else {
        // field-value 对不存在，设置值为 0
        value = 0;
    }

    // 检查值是否溢出
    oldvalue = value;
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }

    // 计算和
    value += incr;

    // 将和保存到 sds
    new = createStringObjectFromLongLong(value);

    // 对 field 进行编码
    hashTypeTryObjectEncoding(o,&c->argv[2],NULL);

    // 设置 field-value 对
    hashTypeSet(o,c->argv[2],new);

    // 计数减一（ hashTypeSet 会处理 field 和 value 的计数）
    decrRefCount(new);

    addReplyLongLong(c,value);
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

/*
 * HINCRBYFLOAT 命令的实现
 *
 * T = O(N)
 */
void hincrbyfloatCommand(redisClient *c) {
    double long value, incr;
    robj *o, *current, *new, *aux;

    // 取出增量
    if (getLongDoubleFromObjectOrReply(c,c->argv[3],&incr,NULL) != REDIS_OK) return;

    // 检查对象存在性和类型
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;

    // 取出当前值，O(N)
    if ((current = hashTypeGetObject(o,c->argv[2])) != NULL) {
        // 如果值不能被表示为浮点数，那么直接失败
        if (getLongDoubleFromObjectOrReply(c,current,&value,
            "hash value is not a valid float") != REDIS_OK) {
            decrRefCount(current);
            return;
        }
        decrRefCount(current);
    } else {
        value = 0;
    }

    // 计算新值
    value += incr;
    // 更新新值
    new = createStringObjectFromLongDouble(value);
    hashTypeTryObjectEncoding(o,&c->argv[2],NULL);
    hashTypeSet(o,c->argv[2],new);

    addReplyBulk(c,new);
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;

    /* Always replicate HINCRBYFLOAT as an HSET command with the final value
     * in order to make sure that differences in float pricision or formatting
     * will not create differences in replicas or after an AOF restart. */
    aux = createStringObject("HSET",4);
    rewriteClientCommandArgument(c,0,aux);
    decrRefCount(aux);
    rewriteClientCommandArgument(c,3,new);
    decrRefCount(new);
}

/*
 * 将 hash 里 field 的值添加到回复中
 * 用作命令的返回手段
 *
 * T = O(N)
 */
static void addHashFieldToReply(redisClient *c, robj *o, robj *field) {
    int ret;

    if (o == NULL) {
        addReply(c, shared.nullbulk);
        return;
    }

    // ziplist
    if (o->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        // 取出值，O(N)
        ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
        if (ret < 0) {
            addReply(c, shared.nullbulk);
        } else {
            if (vstr) {
                addReplyBulkCBuffer(c, vstr, vlen);
            } else {
                addReplyBulkLongLong(c, vll);
            }
        }

    // 字典
    } else if (o->encoding == REDIS_ENCODING_HT) {
        robj *value;

        // 取出值，O(1)
        ret = hashTypeGetFromHashTable(o, field, &value);
        if (ret < 0) {
            addReply(c, shared.nullbulk);
        } else {
            addReplyBulk(c, value);
        }

    } else {
        redisPanic("Unknown hash encoding");
    }
}

/*
 * HGET 命令的实现
 *
 * T = O(1)
 */
void hgetCommand(redisClient *c) {
    robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    addHashFieldToReply(c, o, c->argv[2]);
}

/*
 * HMGET 命令的实现
 *
 * T = O(N^2)
 */
void hmgetCommand(redisClient *c) {
    robj *o;
    int i;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * hashes, where HMGET should respond with a series of null bulks. */
    // 获取或创建一个字典
    o = lookupKeyRead(c->db, c->argv[1]);
    if (o != NULL && o->type != REDIS_HASH) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    // 获取 field 的值
    addReplyMultiBulkLen(c, c->argc-2);
    for (i = 2; i < c->argc; i++) {
        addHashFieldToReply(c, o, c->argv[i]);
    }
}

/*
 * HDEL 命令的实现
 *
 * T = O(N^2)
 */
void hdelCommand(redisClient *c) {
    robj *o;
    int j, deleted = 0;

    // 获取或创建一个字典
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    for (j = 2; j < c->argc; j++) {
        // 删除
        if (hashTypeDelete(o,c->argv[j])) {
            // 计数
            deleted++;

            // 如果 hash 已经为空，那么删除它
            if (hashTypeLength(o) == 0) {
                dbDelete(c->db,c->argv[1]);
                break;
            }
        }
    }

    // 如果至少删除了一个 field-value 对
    // 那么通知 db 键已被修改
    if (deleted) {
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty += deleted;
    }

    addReplyLongLong(c,deleted);
}

/*
 * HLEN 命令的实现
 *
 * T = O(1)
 */
void hlenCommand(redisClient *c) {
    robj *o;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    addReplyLongLong(c,hashTypeLength(o));
}

/*
 * 取出当前 hash 节点的 field 或者  value 。
 *
 * T = O(1)
 */
static void addHashIteratorCursorToReply(redisClient *c, hashTypeIterator *hi, int what) {
    if (hi->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        // 从 ziplist 的节点中取出 field 所对应的值
        // O(1)
        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr) {
            addReplyBulkCBuffer(c, vstr, vlen);
        } else {
            addReplyBulkLongLong(c, vll);
        }

    } else if (hi->encoding == REDIS_ENCODING_HT) {
        robj *value;

        // 从字典中取出 field 的值
        // O(1)
        hashTypeCurrentFromHashTable(hi, what, &value);
        addReplyBulk(c, value);

    } else {
        redisPanic("Unknown hash encoding");
    }
}

/*
 * GETALL 等命令的底层实现
 *
 * T = O(N)
 */
void genericHgetallCommand(redisClient *c, int flags) {
    robj *o;
    hashTypeIterator *hi;
    int multiplier = 0;
    int length, count = 0;

    // 创建或查找 hash
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL
        || checkType(c,o,REDIS_HASH)) return;

    // 选择该返回那些内容
    if (flags & REDIS_HASH_KEY) multiplier++;
    if (flags & REDIS_HASH_VALUE) multiplier++;

    // 根据要返回的内容，设置要获取的元素的数量
    // O(1)
    length = hashTypeLength(o) * multiplier;
    addReplyMultiBulkLen(c, length);

    // 开始迭代
    // O(N)
    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != REDIS_ERR) {
        if (flags & REDIS_HASH_KEY) {
            addHashIteratorCursorToReply(c, hi, REDIS_HASH_KEY);
            count++;
        }
        if (flags & REDIS_HASH_VALUE) {
            addHashIteratorCursorToReply(c, hi, REDIS_HASH_VALUE);
            count++;
        }
    }

    // 释放迭代器
    hashTypeReleaseIterator(hi);

    redisAssert(count == length);
}

/*
 * HKEYS 命令的实现
 *
 * T = O(N)
 */
void hkeysCommand(redisClient *c) {
    // 值取出键
    genericHgetallCommand(c,REDIS_HASH_KEY);
}

/*
 * HVALS 命令的实现
 *
 * T = O(N)
 */
void hvalsCommand(redisClient *c) {
    // 只取出值
    genericHgetallCommand(c,REDIS_HASH_VALUE);
}

/*
 * HGETALL 命令的实现
 */
void hgetallCommand(redisClient *c) {
    // 取出键和值
    genericHgetallCommand(c,REDIS_HASH_KEY|REDIS_HASH_VALUE);
}

/*
 * HEXISTS 命令的实现
 *
 * 复杂度：O(N)
 */
void hexistsCommand(redisClient *c) {
    robj *o;

    // 取出对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_HASH)) return;

    // 测试给定域是否存在
    addReply(c, hashTypeExists(o,c->argv[2]) ? shared.cone : shared.czero);
}
