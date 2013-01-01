/* Redis Object implementation.
 *
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
#include <ctype.h>

/*
 * 根据给定类型和值，创建新对象
 */
robj *createObject(int type, void *ptr) {

    // 分配空间
    robj *o = zmalloc(sizeof(*o));

    // 初始化对象域
    o->type = type;
    o->encoding = REDIS_ENCODING_RAW;   // 默认编码
    o->ptr = ptr;
    o->refcount = 1;

    /* Set the LRU to the current lruclock (minutes resolution). */
    o->lru = server.lruclock;

    return o;
}

/*
 * 根据给定字符数组，创建一个 String 对象
 */
robj *createStringObject(char *ptr, size_t len) {
    return createObject(REDIS_STRING,sdsnewlen(ptr,len));
}

/*
 * 根据给定数字值 value ，创建一个 String 对象
 */
robj *createStringObjectFromLongLong(long long value) {

    robj *o;

    if (value >= 0 && value < REDIS_SHARED_INTEGERS) {
        // 如果条件允许，使用共享对象
        incrRefCount(shared.integers[value]);
        o = shared.integers[value];
    } else {
        // 否则，创建新 String 对象
        if (value >= LONG_MIN && value <= LONG_MAX) {
            // long 类型的数字值以 long 类型保存
            o = createObject(REDIS_STRING, NULL);
            o->encoding = REDIS_ENCODING_INT;   // 设置编码
            o->ptr = (void*)((long)value);
        } else {
            // long long 类型的数字值编码成字符串来保存
            o = createObject(REDIS_STRING,sdsfromlonglong(value));
        }
    }

    return o;
}

/* Note: this function is defined into object.c since here it is where it
 * belongs but it is actually designed to be used just for INCRBYFLOAT */
/*
 * 根据给定 long double 值 value ，创建 String 对象
 */
robj *createStringObjectFromLongDouble(long double value) {
    char buf[256];
    int len;

    /* We use 17 digits precision since with 128 bit floats that precision
     * after rouding is able to represent most small decimal numbers in a way
     * that is "non surprising" for the user (that is, most small decimal
     * numbers will be represented in a way that when converted back into
     * a string are exactly the same as what the user typed.) */
    // 将 long double 值转换为字符串
    len = snprintf(buf,sizeof(buf),"%.17Lf", value);
    /* Now remove trailing zeroes after the '.' */
    if (strchr(buf,'.') != NULL) {
        char *p = buf+len-1;
        while(*p == '0') {
            p--;
            len--;
        }
        if (*p == '.') len--;
    }
    return createStringObject(buf,len);
}

/*
 * 复制一个 String 对象的副本
 */
robj *dupStringObject(robj *o) {
    redisAssertWithInfo(NULL,o,o->encoding == REDIS_ENCODING_RAW);
    return createStringObject(o->ptr,sdslen(o->ptr));
}

/*
 * 创建一个 list 对象
 */
robj *createListObject(void) {
    // 使用双端链表(adlist)
    list *l = listCreate();

    // 创建对象
    robj *o = createObject(REDIS_LIST,l);

    // 因为列表里的值也是其他对象
    // 所以要设置 decrRefCount 作为值的释放器
    listSetFreeMethod(l,decrRefCount);

    // 设置编码
    o->encoding = REDIS_ENCODING_LINKEDLIST;

    return o;
}

/*
 * 创建一个 ziplist 对象
 */
robj *createZiplistObject(void) {
    unsigned char *zl = ziplistNew();
    robj *o = createObject(REDIS_LIST,zl);
    o->encoding = REDIS_ENCODING_ZIPLIST;
    return o;
}

/*
 * 创建一个 set 对象
 */
robj *createSetObject(void) {
    dict *d = dictCreate(&setDictType,NULL);
    robj *o = createObject(REDIS_SET,d);
    o->encoding = REDIS_ENCODING_HT;
    return o;
}

/*
 * 创建一个 intset 对象
 */
robj *createIntsetObject(void) {
    intset *is = intsetNew();
    robj *o = createObject(REDIS_SET,is);
    o->encoding = REDIS_ENCODING_INTSET;
    return o;
}

/*
 * 创建一个 hash 对象
 */
robj *createHashObject(void) {
    unsigned char *zl = ziplistNew();
    robj *o = createObject(REDIS_HASH, zl);
    o->encoding = REDIS_ENCODING_ZIPLIST;
    return o;
}

/*
 * 创建一个 zset 对象
 */
robj *createZsetObject(void) {
    zset *zs = zmalloc(sizeof(*zs));
    robj *o;

    // zset 使用 dict 和 skiplist 两个数据结构
    zs->dict = dictCreate(&zsetDictType,NULL);
    zs->zsl = zslCreate();

    o = createObject(REDIS_ZSET,zs);

    o->encoding = REDIS_ENCODING_SKIPLIST;

    return o;
}

/*
 * 创建一个 ziplist 表示的 zset 对象
 */
robj *createZsetZiplistObject(void) {
    unsigned char *zl = ziplistNew();
    robj *o = createObject(REDIS_ZSET,zl);
    o->encoding = REDIS_ENCODING_ZIPLIST;
    return o;
}

/*
 * 释放 string 对象
 */
void freeStringObject(robj *o) {
    if (o->encoding == REDIS_ENCODING_RAW) {
        sdsfree(o->ptr);
    }
}

/*
 * 释放 list 对象
 */
void freeListObject(robj *o) {
    switch (o->encoding) {
    // 释放双端链表
    case REDIS_ENCODING_LINKEDLIST:
        listRelease((list*) o->ptr);
        break;
    // 释放 ziplist 
    case REDIS_ENCODING_ZIPLIST:
        zfree(o->ptr);
        break;
    default:
        redisPanic("Unknown list encoding type");
    }
}

/*
 * 释放 set 对象
 */
void freeSetObject(robj *o) {
    switch (o->encoding) {
    // hash 表示
    case REDIS_ENCODING_HT:
        dictRelease((dict*) o->ptr);
        break;
    // intset 表示
    case REDIS_ENCODING_INTSET:
        zfree(o->ptr);
        break;
    default:
        redisPanic("Unknown set encoding type");
    }
}

/*
 *  释放 zset 对象
 */
void freeZsetObject(robj *o) {
    zset *zs;
    switch (o->encoding) {
    // skiplist 表示
    case REDIS_ENCODING_SKIPLIST:
        zs = o->ptr;
        dictRelease(zs->dict);
        zslFree(zs->zsl);
        zfree(zs);
        break;
    // ziplist 表示
    case REDIS_ENCODING_ZIPLIST:
        zfree(o->ptr);
        break;
    default:
        redisPanic("Unknown sorted set encoding");
    }
}

/*
 * 释放 hash 对象
 */
void freeHashObject(robj *o) {
    switch (o->encoding) {
    // hash 表示
    case REDIS_ENCODING_HT:
        dictRelease((dict*) o->ptr);
        break;
    // ziplist 表示
    case REDIS_ENCODING_ZIPLIST:
        zfree(o->ptr);
        break;
    default:
        redisPanic("Unknown hash encoding type");
        break;
    }
}

/*
 * 增加对象的引用计数
 */
void incrRefCount(robj *o) {
    o->refcount++;
}

/*
 * 减少对象的引用计数
 *
 * 当对象的引用计数降为 0 时，释放这个对象
 */
void decrRefCount(void *obj) {
    robj *o = obj;

    if (o->refcount <= 0) redisPanic("decrRefCount against refcount <= 0");

    if (o->refcount == 1) {
        // 如果引用数降为 0 
        // 根据对象类型，调用相应的对象释放函数来释放对象的值
        switch(o->type) {
        case REDIS_STRING: freeStringObject(o); break;
        case REDIS_LIST: freeListObject(o); break;
        case REDIS_SET: freeSetObject(o); break;
        case REDIS_ZSET: freeZsetObject(o); break;
        case REDIS_HASH: freeHashObject(o); break;
        default: redisPanic("Unknown object type"); break;
        }
        // 释放对象本身
        zfree(o);
    } else {
        // 否则，只降低引用数
        o->refcount--;
    }
}

/* This function set the ref count to zero without freeing the object.
 * It is useful in order to pass a new object to functions incrementing
 * the ref count of the received object. Example:
 *
 *    functionThatWillIncrementRefCount(resetRefCount(CreateObject(...)));
 *
 * Otherwise you need to resort to the less elegant pattern:
 *
 *    *obj = createObject(...);
 *    functionThatWillIncrementRefCount(obj);
 *    decrRefCount(obj);
 */
/*
 * 将对象的引用数设置为 0 ，但不释放该对象
 */
robj *resetRefCount(robj *obj) {
    obj->refcount = 0;
    return obj;
}

/*
 * 检查给定对象 o 的类型是否为给定类型 type
 *
 * 类型不相同时返回 1 ，并向客户端报告类型错误。
 * 类型相同时返回 0 。
 */
int checkType(redisClient *c, robj *o, int type) {
    if (o->type != type) {
        addReply(c,shared.wrongtypeerr);
        return 1;
    }
    return 0;
}

/*
 * 检查给定的 string 对象能否表示为 long long 类型值
 */
int isObjectRepresentableAsLongLong(robj *o, long long *llval) {
    redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);
    if (o->encoding == REDIS_ENCODING_INT) {
        if (llval) *llval = (long) o->ptr;
        return REDIS_OK;
    } else {
        return string2ll(o->ptr,sdslen(o->ptr),llval) ? REDIS_OK : REDIS_ERR;
    }
}

/* Try to encode a string object in order to save space */
/*
 * 尝试将对象 o 编码成整数，并尝试将它加入到共享对象里面
 */
robj *tryObjectEncoding(robj *o) {
    long value;
    sds s = o->ptr;

    // 对象已编码?
    if (o->encoding != REDIS_ENCODING_RAW)
        return o; /* Already encoded */

    /* It's not safe to encode shared objects: shared objects can be shared
     * everywhere in the "object space" of Redis. Encoded objects can only
     * appear as "values" (and not, for instance, as keys) */
     // 不对共享对象进行编码
     if (o->refcount > 1) return o;

    /* Currently we try to encode only strings */
    // 只尝试对 string 对象进行编码
    redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);

    /* Check if we can represent this string as a long integer */
    // 尝试将字符串值转换为 long 整数
    // 转换失败直接返回 o ，转换成功时继续执行
    if (!string2l(s,sdslen(s),&value)) return o;

    /* Ok, this object can be encoded...
     *
     * Can I use a shared object? Only if the object is inside a given range
     *
     * Note that we also avoid using shared integers when maxmemory is used
     * because every object needs to have a private LRU field for the LRU
     * algorithm to work well. */
    // 执行到这一行时， value 保存的已经是一个 long 整数了
    if (server.maxmemory == 0 && value >= 0 && value < REDIS_SHARED_INTEGERS) {
        // 看看 value 是否属于可共享值的范围
        // 如果是的话就用共享对象代替这个对象 o
        decrRefCount(o);
        incrRefCount(shared.integers[value]);

        // 将共享对象返回
        return shared.integers[value];
    } else {
        // value 不属于共享范围，将它保存到对象 o 中
        o->encoding = REDIS_ENCODING_INT;   // 更新编码方式
        sdsfree(o->ptr);        // 释放旧值
        o->ptr = (void*) value; // 设置新值

        return o;
    }
}

/* Get a decoded version of an encoded object (returned as a new object).
 * If the object is already raw-encoded just increment the ref count. */
/*
 * 返回一个对象的未编码版本
 *
 * 如果输入对象是已编码的，那么返回的对象是输入对象的新对象副本
 * 如果输入对象是未编码的，那么为它的引用计数增一，然后返回它
 */
robj *getDecodedObject(robj *o) {
    robj *dec;

    // 返回未编码对象
    if (o->encoding == REDIS_ENCODING_RAW) {
        incrRefCount(o);
        return o;
    }

    // 返回已编码对象的未编码版本
    if (o->type == REDIS_STRING &&
        o->encoding == REDIS_ENCODING_INT) 
    {
        char buf[32];

        // 将整数值转换回一个字符串对象
        ll2string(buf,32,(long)o->ptr);
        dec = createStringObject(buf,strlen(buf));
        return dec;
    } else {
        redisPanic("Unknown encoding type");
    }
}

/* Compare two string objects via strcmp() or alike.
 * Note that the objects may be integer-encoded. In such a case we
 * use ll2string() to get a string representation of the numbers on the stack
 * and compare the strings, it's much faster than calling getDecodedObject().
 *
 * Important note: if objects are not integer encoded, but binary-safe strings,
 * sdscmp() from sds.c will apply memcmp() so this function ca be considered
 * binary safe. */
int compareStringObjects(robj *a, robj *b) {
    redisAssertWithInfo(NULL,a,a->type == REDIS_STRING && b->type == REDIS_STRING);
    char bufa[128], bufb[128], *astr, *bstr;
    int bothsds = 1;

    if (a == b) return 0;
    if (a->encoding != REDIS_ENCODING_RAW) {
        ll2string(bufa,sizeof(bufa),(long) a->ptr);
        astr = bufa;
        bothsds = 0;
    } else {
        astr = a->ptr;
    }
    if (b->encoding != REDIS_ENCODING_RAW) {
        ll2string(bufb,sizeof(bufb),(long) b->ptr);
        bstr = bufb;
        bothsds = 0;
    } else {
        bstr = b->ptr;
    }
    return bothsds ? sdscmp(astr,bstr) : strcmp(astr,bstr);
}

/* Equal string objects return 1 if the two objects are the same from the
 * point of view of a string comparison, otherwise 0 is returned. Note that
 * this function is faster then checking for (compareStringObject(a,b) == 0)
 * because it can perform some more optimization. */
int equalStringObjects(robj *a, robj *b) {
    if (a->encoding != REDIS_ENCODING_RAW && b->encoding != REDIS_ENCODING_RAW){
        return a->ptr == b->ptr;
    } else {
        return compareStringObjects(a,b) == 0;
    }
}

size_t stringObjectLen(robj *o) {
    redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);
    if (o->encoding == REDIS_ENCODING_RAW) {
        return sdslen(o->ptr);
    } else {
        char buf[32];

        return ll2string(buf,32,(long)o->ptr);
    }
}

int getDoubleFromObject(robj *o, double *target) {
    double value;
    char *eptr;

    if (o == NULL) {
        value = 0;
    } else {
        redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);
        if (o->encoding == REDIS_ENCODING_RAW) {
            errno = 0;
            value = strtod(o->ptr, &eptr);
            if (isspace(((char*)o->ptr)[0]) || eptr[0] != '\0' ||
                errno == ERANGE || isnan(value))
                return REDIS_ERR;
        } else if (o->encoding == REDIS_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            redisPanic("Unknown string encoding");
        }
    }
    *target = value;
    return REDIS_OK;
}

int getDoubleFromObjectOrReply(redisClient *c, robj *o, double *target, const char *msg) {
    double value;
    if (getDoubleFromObject(o, &value) != REDIS_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not a valid float");
        }
        return REDIS_ERR;
    }
    *target = value;
    return REDIS_OK;
}

int getLongDoubleFromObject(robj *o, long double *target) {
    long double value;
    char *eptr;

    if (o == NULL) {
        value = 0;
    } else {
        redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);
        if (o->encoding == REDIS_ENCODING_RAW) {
            errno = 0;
            value = strtold(o->ptr, &eptr);
            if (isspace(((char*)o->ptr)[0]) || eptr[0] != '\0' ||
                errno == ERANGE || isnan(value))
                return REDIS_ERR;
        } else if (o->encoding == REDIS_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            redisPanic("Unknown string encoding");
        }
    }
    *target = value;
    return REDIS_OK;
}

int getLongDoubleFromObjectOrReply(redisClient *c, robj *o, long double *target, const char *msg) {
    long double value;
    if (getLongDoubleFromObject(o, &value) != REDIS_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not a valid float");
        }
        return REDIS_ERR;
    }
    *target = value;
    return REDIS_OK;
}

/*
 * 从对象 o 中提取 long long 值，并使用指针保存所提取的值
 */
int getLongLongFromObject(
    robj *o,            // 提取对象
    long long *target   // 保存值的指针
)
{
    long long value;
    char *eptr;

    if (o == NULL) {
        value = 0;
    } else {
        redisAssertWithInfo(NULL,o,o->type == REDIS_STRING);
        // 根据不同编码，取出值
        if (o->encoding == REDIS_ENCODING_RAW) {
            errno = 0;
            value = strtoll(o->ptr, &eptr, 10);
            if (isspace(((char*)o->ptr)[0]) || eptr[0] != '\0' ||
                errno == ERANGE)
                return REDIS_ERR;
        } else if (o->encoding == REDIS_ENCODING_INT) {
            value = (long)o->ptr;
        } else {
            redisPanic("Unknown string encoding");
        }
    }
    if (target) *target = value;
    return REDIS_OK;
}

/* 
 * 将给定对象 o 的 long long 值取出，并通过指针 target 进行保存
 * 如果提取失败，添加错误信息 msg 到客户端 c
 */
int getLongLongFromObjectOrReply(
    redisClient *c,
    robj *o,            // 要提取 long long 值的对象
    long long *target,  // 保存值的指针
    const char *msg     // 错误信息
)
{
    long long value;
    if (getLongLongFromObject(o, &value) != REDIS_OK) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not an integer or out of range");
        }
        return REDIS_ERR;
    }
    *target = value;
    return REDIS_OK;
}

int getLongFromObjectOrReply(redisClient *c, robj *o, long *target, const char *msg) {
    long long value;

    if (getLongLongFromObjectOrReply(c, o, &value, msg) != REDIS_OK) return REDIS_ERR;
    if (value < LONG_MIN || value > LONG_MAX) {
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is out of range");
        }
        return REDIS_ERR;
    }
    *target = value;
    return REDIS_OK;
}

/*
 * 返回编码的字符串形式
 */
char *strEncoding(int encoding) {
    switch(encoding) {
    case REDIS_ENCODING_RAW: return "raw";
    case REDIS_ENCODING_INT: return "int";
    case REDIS_ENCODING_HT: return "hashtable";
    case REDIS_ENCODING_LINKEDLIST: return "linkedlist";
    case REDIS_ENCODING_ZIPLIST: return "ziplist";
    case REDIS_ENCODING_INTSET: return "intset";
    case REDIS_ENCODING_SKIPLIST: return "skiplist";
    default: return "unknown";
    }
}

/* Given an object returns the min number of seconds the object was never
 * requested, using an approximated LRU algorithm. */
unsigned long estimateObjectIdleTime(robj *o) {
    if (server.lruclock >= o->lru) {
        return (server.lruclock - o->lru) * REDIS_LRU_CLOCK_RESOLUTION;
    } else {
        return ((REDIS_LRU_CLOCK_MAX - o->lru) + server.lruclock) *
                    REDIS_LRU_CLOCK_RESOLUTION;
    }
}

/* This is an helper function for the DEBUG command. We need to lookup keys
 * without any modification of LRU or other parameters. */
robj *objectCommandLookup(redisClient *c, robj *key) {
    dictEntry *de;

    if ((de = dictFind(c->db->dict,key->ptr)) == NULL) return NULL;
    return (robj*) dictGetVal(de);
}

robj *objectCommandLookupOrReply(redisClient *c, robj *key, robj *reply) {
    robj *o = objectCommandLookup(c,key);

    if (!o) addReply(c, reply);
    return o;
}

/*
 * OBJECT 命令的实现
 */
void objectCommand(redisClient *c) {
    robj *o;

    // 查看引用计数
    if (!strcasecmp(c->argv[1]->ptr,"refcount") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyLongLong(c,o->refcount);
    // 查看编码方式
    } else if (!strcasecmp(c->argv[1]->ptr,"encoding") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyBulkCString(c,strEncoding(o->encoding));
    // 查看空转时间
    } else if (!strcasecmp(c->argv[1]->ptr,"idletime") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyLongLong(c,estimateObjectIdleTime(o));
    } else {
        addReplyError(c,"Syntax error. Try OBJECT (refcount|encoding|idletime)");
    }
}

