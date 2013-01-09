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

/*-----------------------------------------------------------------------------
 * Set Commands
 *----------------------------------------------------------------------------*/

void sunionDiffGenericCommand(redisClient *c, robj **setkeys, int setnum, robj *dstkey, int op);

/* Factory method to return a set that *can* hold "value". When the object has
 * an integer-encodable value, an intset will be returned. Otherwise a regular
 * hash table. */
/*
 * 根据给定值 value ，决定是创建 intset 编码还是字典编码的集合
 *
 * T = O(1)
 */
robj *setTypeCreate(robj *value) {
    if (isObjectRepresentableAsLongLong(value,NULL) == REDIS_OK)
        return createIntsetObject();
    return createSetObject();
}

/*
 * 多态添加操作
 *
 * T = O(N)
 */
int setTypeAdd(robj *subject, robj *value) {
    long long llval;
    // subject 为字典,O(1)
    if (subject->encoding == REDIS_ENCODING_HT) {
        if (dictAdd(subject->ptr,value,NULL) == DICT_OK) {
            incrRefCount(value);
            return 1;
        }
    // subject 为intset , O(N)
    } else if (subject->encoding == REDIS_ENCODING_INTSET) {
        // value 可以表示为 long long 类型
        if (isObjectRepresentableAsLongLong(value,&llval) == REDIS_OK) {
            uint8_t success = 0;
            // 添加值, O(N)
            subject->ptr = intsetAdd(subject->ptr,llval,&success);
            // 添加成功
            if (success) {
                /* Convert to regular set when the intset contains
                 * too many entries. */
                // 检查是否需要将 intset 转换为字典
                if (intsetLen(subject->ptr) > server.set_max_intset_entries)
                    setTypeConvert(subject,REDIS_ENCODING_HT);
                return 1;
            }
        // value 不能保存为 long long 类型，必须转换为字典
        } else {
            /* Failed to get integer from object, convert to regular set. */
            setTypeConvert(subject,REDIS_ENCODING_HT);

            /* The set *was* an intset and this value is not integer
             * encodable, so dictAdd should always work. */
             // 添加值
            redisAssertWithInfo(NULL,value,dictAdd(subject->ptr,value,NULL) == DICT_OK);

            incrRefCount(value);
            return 1;
        }
    } else {
        redisPanic("Unknown set encoding");
    }
    return 0;
}

/*
 * 多态删除操作
 *
 * T = O(N)
 */
int setTypeRemove(robj *setobj, robj *value) {
    long long llval;
    // 字典编码, O(N)
    if (setobj->encoding == REDIS_ENCODING_HT) {
        // O(1)
        if (dictDelete(setobj->ptr,value) == DICT_OK) {
            // 如果有需要，缩小字典, O(N)
            if (htNeedsResize(setobj->ptr)) dictResize(setobj->ptr);
            return 1;
        }
    // intset 编码
    } else if (setobj->encoding == REDIS_ENCODING_INTSET) {
        // 如果 value 可以编码成 long long 类型，
        // 那么尝试在 intset 删除它 
        if (isObjectRepresentableAsLongLong(value,&llval) == REDIS_OK) {
            int success;
            // O(N)
            setobj->ptr = intsetRemove(setobj->ptr,llval,&success);
            if (success) return 1;
        }
    } else {
        redisPanic("Unknown set encoding");
    }
    return 0;
}

/*
 * 多态成员检查操作
 *
 * T = O(lg N)
 */
int setTypeIsMember(robj *subject, robj *value) {
    long long llval;

    // 字典
    if (subject->encoding == REDIS_ENCODING_HT) {
        // O(1)
        return dictFind((dict*)subject->ptr,value) != NULL;

    // intset
    } else if (subject->encoding == REDIS_ENCODING_INTSET) {
        if (isObjectRepresentableAsLongLong(value,&llval) == REDIS_OK) {
            // O(lg N)
            return intsetFind((intset*)subject->ptr,llval);
        }

    } else {
        redisPanic("Unknown set encoding");
    }

    return 0;
}

/*
 * 创建一个多态迭代器
 *
 * T = O(1)
 */
setTypeIterator *setTypeInitIterator(robj *subject) {
    setTypeIterator *si = zmalloc(sizeof(setTypeIterator));

    si->subject = subject;
    si->encoding = subject->encoding;

    if (si->encoding == REDIS_ENCODING_HT) {
        si->di = dictGetIterator(subject->ptr);
    } else if (si->encoding == REDIS_ENCODING_INTSET) {
        si->ii = 0;
    } else {
        redisPanic("Unknown set encoding");
    }

    return si;
}

/*
 * 释放多态迭代器
 *
 * T = O(1)
 */
void setTypeReleaseIterator(setTypeIterator *si) {
    if (si->encoding == REDIS_ENCODING_HT)
        dictReleaseIterator(si->di);
    zfree(si);
}

/* Move to the next entry in the set. Returns the object at the current
 * position.
 *
 * Since set elements can be internally be stored as redis objects or
 * simple arrays of integers, setTypeNext returns the encoding of the
 * set object you are iterating, and will populate the appropriate pointer
 * (eobj) or (llobj) accordingly.
 *
 * When there are no longer elements -1 is returned.
 * Returned objects ref count is not incremented, so this function is
 * copy on write friendly. */
/*
 * 取出迭代器指向的当前元素
 *
 * robj 参数保存字典编码的值， llele 参数保存 intset 保存的值
 *
 * 返回值指示到底哪种编码的值被取出了，返回 -1 表示集合为空
 *
 * T = O(1)
 */
int setTypeNext(setTypeIterator *si, robj **objele, int64_t *llele) {

    // 字典
    if (si->encoding == REDIS_ENCODING_HT) {
        dictEntry *de = dictNext(si->di);
        if (de == NULL) return -1;
        *objele = dictGetKey(de);
    // intset
    } else if (si->encoding == REDIS_ENCODING_INTSET) {
        if (!intsetGet(si->subject->ptr,si->ii++,llele))
            return -1;
    }

    return si->encoding;
}

/* The not copy on write friendly version but easy to use version
 * of setTypeNext() is setTypeNextObject(), returning new objects
 * or incrementing the ref count of returned objects. So if you don't
 * retain a pointer to this object you should call decrRefCount() against it.
 *
 * This function is the way to go for write operations where COW is not
 * an issue as the result will be anyway of incrementing the ref count. */
/*
 * 获取迭代器的当前元素，并返回包含它的一个对象。
 * 如果调用者不使用这个对象的话，必须在使用完毕之后释放它。
 *
 * T = O(1)
 */
robj *setTypeNextObject(setTypeIterator *si) {
    int64_t intele;
    robj *objele;
    int encoding;

    encoding = setTypeNext(si,&objele,&intele);
    switch(encoding) {
        case -1:    return NULL;
        case REDIS_ENCODING_INTSET:
            return createStringObjectFromLongLong(intele);
        case REDIS_ENCODING_HT:
            incrRefCount(objele);
            return objele;
        default:
            redisPanic("Unsupported encoding");
    }

    return NULL; /* just to suppress warnings */
}

/* Return random element from a non empty set.
 * The returned element can be a int64_t value if the set is encoded
 * as an "intset" blob of integers, or a redis object if the set
 * is a regular set.
 *
 * The caller provides both pointers to be populated with the right
 * object. The return value of the function is the object->encoding
 * field of the object and is used by the caller to check if the
 * int64_t pointer or the redis object pointere was populated.
 *
 * When an object is returned (the set was a real set) the ref count
 * of the object is not incremented so this function can be considered
 * copy on write friendly. */
/*
 * 多态随机元素返回函数
 *
 * objele 保存字典编码的值， llele 保存 intset 编码的值
 *
 * 返回值指示到底那种编码的值被保存了。
 *
 * T = O(N)
 */
int setTypeRandomElement(robj *setobj, robj **objele, int64_t *llele) {

    // 字典
    if (setobj->encoding == REDIS_ENCODING_HT) {
        // O(N)
        dictEntry *de = dictGetRandomKey(setobj->ptr);
        // O(1)
        *objele = dictGetKey(de);

    // intset
    } else if (setobj->encoding == REDIS_ENCODING_INTSET) {
        // O(1)
        *llele = intsetRandom(setobj->ptr);

    } else {
        redisPanic("Unknown set encoding");
    }

    return setobj->encoding;
}

/*
 * 多态元素个数返回函数
 *
 * T = O(1)
 */
unsigned long setTypeSize(robj *subject) {
    // 字典
    if (subject->encoding == REDIS_ENCODING_HT) {
        return dictSize((dict*)subject->ptr);

    // intset
    } else if (subject->encoding == REDIS_ENCODING_INTSET) {
        return intsetLen((intset*)subject->ptr);

    } else {
        redisPanic("Unknown set encoding");
    }
}

/* Convert the set to specified encoding. The resulting dict (when converting
 * to a hash table) is presized to hold the number of elements in the original
 * set. */
/*
 * 将集合对象 setobj 转换为 enc 指定的编码
 *
 * 目前只支持将 intset 转换为 HT 编码
 *
 * T = O(N)
 */
void setTypeConvert(robj *setobj, int enc) {
    setTypeIterator *si;
    redisAssertWithInfo(NULL,setobj,setobj->type == REDIS_SET &&
                             setobj->encoding == REDIS_ENCODING_INTSET);

    if (enc == REDIS_ENCODING_HT) {
        int64_t intele;
        dict *d = dictCreate(&setDictType,NULL);
        robj *element;

        /* Presize the dict to avoid rehashing */
        // O(N)
        dictExpand(d,intsetLen(setobj->ptr));

        /* To add the elements we extract integers and create redis objects */
        // O(N)
        si = setTypeInitIterator(setobj);
        while (setTypeNext(si,NULL,&intele) != -1) {
            element = createStringObjectFromLongLong(intele);
            redisAssertWithInfo(NULL,element,dictAdd(d,element,NULL) == DICT_OK);
        }
        setTypeReleaseIterator(si);

        setobj->encoding = REDIS_ENCODING_HT;
        zfree(setobj->ptr);
        setobj->ptr = d;
    } else {
        redisPanic("Unsupported set conversion");
    }
}

/*
 * T = O(N^2)
 */
void saddCommand(redisClient *c) {
    robj *set;
    int j, added = 0;

    // 查找集合对象
    set = lookupKeyWrite(c->db,c->argv[1]);

    // 没找到，创建一个空集合, O(1)
    if (set == NULL) {
        set = setTypeCreate(c->argv[2]);
        dbAdd(c->db,c->argv[1],set);
    // 找到，但不是集合，报错
    } else {
        if (set->type != REDIS_SET) {
            addReply(c,shared.wrongtypeerr);
            return;
        }
    }

    // 将所有输入元素添加到集合
    // O(N^2)
    for (j = 2; j < c->argc; j++) {
        c->argv[j] = tryObjectEncoding(c->argv[j]);
        if (setTypeAdd(set,c->argv[j])) added++;
    }

    // 如果至少有一个元素添加成功，那么通知 db ，key 被修改
    if (added) signalModifiedKey(c->db,c->argv[1]);

    // 返回添加元素的个数
    server.dirty += added;
    addReplyLongLong(c,added);
}

/*
 * T = O(N^2)
 */
void sremCommand(redisClient *c) {
    robj *set;
    int j, deleted = 0;

    // 查找集合，如果集合不存在或类型错误，直接返回
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,REDIS_SET)) return;

    // 移除多个元素, O(N^2)
    for (j = 2; j < c->argc; j++) {
        if (setTypeRemove(set,c->argv[j])) {
            deleted++;
            if (setTypeSize(set) == 0) {
                dbDelete(c->db,c->argv[1]);
                break;
            }
        }
    }

    // 如果至少有一个元素被移除，那么通知 db ，key 被修改
    if (deleted) {
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty += deleted;
    }

    addReplyLongLong(c,deleted);
}

/*
 * T = O(N)
 */
void smoveCommand(redisClient *c) {
    robj *srcset, *dstset, *ele;

    // 源集合
    srcset = lookupKeyWrite(c->db,c->argv[1]);
    // 目标集合
    dstset = lookupKeyWrite(c->db,c->argv[2]);

    // 要被移动的元素
    ele = c->argv[3] = tryObjectEncoding(c->argv[3]);

    /* If the source key does not exist return 0 */
    // 源集合不存在，直接返回
    if (srcset == NULL) {
        addReply(c,shared.czero);
        return;
    }

    /* If the source key has the wrong type, or the destination key
     * is set and has the wrong type, return with an error. */
    // 如果源对象或者目标对象不是集合，直接返回 
    if (checkType(c,srcset,REDIS_SET) ||
        (dstset && checkType(c,dstset,REDIS_SET))) return;

    /* If srcset and dstset are equal, SMOVE is a no-op */
    // 源集合和目标集合相同，直接返回 
    if (srcset == dstset) {
        addReply(c,shared.cone);
        return;
    }

    /* If the element cannot be removed from the src set, return 0. */
    // 从源集合中删除目标元素
    // O(N)
    if (!setTypeRemove(srcset,ele)) {
        addReply(c,shared.czero);
        return;
    }

    /* Remove the src set from the database when empty */
    // 如果源集合已经为空，那么删除它
    if (setTypeSize(srcset) == 0) dbDelete(c->db,c->argv[1]);

    // 通知 db ， key 被修改
    signalModifiedKey(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[2]);
    server.dirty++;

    /* Create the destination set when it doesn't exist */
    // 如果目标集合不存在，创建一个新的
    // O(1)
    if (!dstset) {
        dstset = setTypeCreate(ele);
        dbAdd(c->db,c->argv[2],dstset);
    }

    /* An extra key has changed when ele was successfully added to dstset */
    // 将目标元素添加到集合
    // O(N)
    if (setTypeAdd(dstset,ele)) server.dirty++;

    addReply(c,shared.cone);
}

/*
 * T = O(lg N)
 */
void sismemberCommand(redisClient *c) {
    robj *set;
    
    // 查找对象，检查类型
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,REDIS_SET)) return;

    // 对输入元素进行编码
    c->argv[2] = tryObjectEncoding(c->argv[2]);

    // 返回结果, O(lg N)
    if (setTypeIsMember(set,c->argv[2]))
        addReply(c,shared.cone);
    else
        addReply(c,shared.czero);
}

/*
 * T = O(1)
 */
void scardCommand(redisClient *c) {
    robj *o;

    // 查找对象，检查类型
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_SET)) return;

    // 返回 size ,O(1)
    addReplyLongLong(c,setTypeSize(o));
}

/*
 * T = O(N)
 */
void spopCommand(redisClient *c) {
    robj *set, *ele, *aux;
    int64_t llele;
    int encoding;

    // 查找对象，检查类型
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,set,REDIS_SET)) return;

    // 在多态函数中获取一个对象或者一个 long long 值
    // O(N)
    encoding = setTypeRandomElement(set,&ele,&llele);

    // 根据返回值判断那个指针保存了元素
    if (encoding == REDIS_ENCODING_INTSET) {
        // 复制元素作为返回值
        ele = createStringObjectFromLongLong(llele);
        // 删除 intset 中的元素, O(N)
        set->ptr = intsetRemove(set->ptr,llele,NULL);
    } else {
        // 为元素的计数增一，以返回它
        incrRefCount(ele);
        // 删除字典中原有的元素
        // O(1)
        setTypeRemove(set,ele);
    }

    /* Replicate/AOF this command as an SREM operation */
    aux = createStringObject("SREM",4);
    rewriteClientCommandVector(c,3,aux,c->argv[1],ele);
    decrRefCount(ele);
    decrRefCount(aux);

    addReplyBulk(c,ele);
    if (setTypeSize(set) == 0) dbDelete(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

/* handle the "SRANDMEMBER key <count>" variant. The normal version of the
 * command is handled by the srandmemberCommand() function itself. */

/* How many times bigger should be the set compared to the requested size
 * for us to don't use the "remove elements" strategy? Read later in the
 * implementation for more info. */
#define SRANDMEMBER_SUB_STRATEGY_MUL 3

/*
 * 带 count 参数的 SRANDMEMBER 命令的实现
 *
 * T = O(N^2)
 */
void srandmemberWithCountCommand(redisClient *c) {
    long l;
    unsigned long count, size;
    int uniq = 1;
    robj *set, *ele;
    int64_t llele;
    int encoding;

    dict *d;

    // 获取 count 参数
    if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != REDIS_OK) return;

    if (l >= 0) {
        count = (unsigned) l;
    } else {
        /* A negative count means: return the same elements multiple times
         * (i.e. don't remove the extracted element after every extraction). */
        count = -l;
        uniq = 0;
    }

    // 查找对象，检查类型, O(1)
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk))
        == NULL || checkType(c,set,REDIS_SET)) return;
    size = setTypeSize(set);

    /* If count is zero, serve it ASAP to avoid special cases later. */
    // 如果 count 为 0 ，直接返回
    if (count == 0) {
        addReply(c,shared.emptymultibulk);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. */
    // 返回 count 个元素，元素可以有重复
    if (!uniq) {
        addReplyMultiBulkLen(c,count);
        // O(N^2)
        while(count--) {
            // O(N)
            encoding = setTypeRandomElement(set,&ele,&llele);
            if (encoding == REDIS_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
            } else {
                addReplyBulk(c,ele);
            }
        }
        return;
    }

    /* CASE 2:
     * The number of requested elements is greater than the number of
     * elements inside the set: simply return the whole set. */
    // count 大于集合基数，返回整个集合
    if (count >= size) {
        // O(N^2)
        sunionDiffGenericCommand(c,c->argv,c->argc-1,NULL,REDIS_OP_UNION);
        return;
    }

    /* For CASE 3 and CASE 4 we need an auxiliary dictionary. */
    // CASE 3 和 CASE 4 使用字典保存结果, O(1)
    d = dictCreate(&setDictType,NULL);

    /* CASE 3:
     * The number of elements inside the set is not greater than
     * SRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a set from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requsted elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 3 is highly inefficient. */
    // 如果 count * SRANDMEMBER_SUB_STRATEGY_MUL 比集合的大小(size)还大
    // 那么遍历集合，将集合的元素保存到另一个新集合里
    // 然后从新集合里随机删除元素，直到剩下 count 个元素为止，最后删除新集合
    // 这个比直接计算 count 个随机元素效率更高
    if (count*SRANDMEMBER_SUB_STRATEGY_MUL > size) {
        setTypeIterator *si;

        /* Add all the elements into the temporary dictionary. */
        // 拷贝整个集合
        // O(N)
        si = setTypeInitIterator(set);
        while((encoding = setTypeNext(si,&ele,&llele)) != -1) {
            int retval;

            if (encoding == REDIS_ENCODING_INTSET) {
                retval = dictAdd(d,createStringObjectFromLongLong(llele),NULL);
            } else if (ele->encoding == REDIS_ENCODING_RAW) {
                retval = dictAdd(d,dupStringObject(ele),NULL);
            } else if (ele->encoding == REDIS_ENCODING_INT) {
                retval = dictAdd(d,
                    createStringObjectFromLongLong((long)ele->ptr),NULL);
            }
            redisAssert(retval == DICT_OK);
        }
        setTypeReleaseIterator(si);
        redisAssert(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        // 随机删除元素直到 size == count 为止
        // O(N^2)
        while(size > count) {
            dictEntry *de;

            // O(N)
            de = dictGetRandomKey(d);
            dictDelete(d,dictGetKey(de));
            size--;
        }
    }
    
    /* CASE 4: We have a big set compared to the requested number of elements.
     * In this case we can simply get random elements from the set and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. */
    // 集合的基数比 count 要小，这种情况下，随机返回 count 个元素即可
    else {
        unsigned long added = 0;

        // O(N^2)
        while(added < count) {

            // O(N)
            encoding = setTypeRandomElement(set,&ele,&llele);

            if (encoding == REDIS_ENCODING_INTSET) {
                ele = createStringObjectFromLongLong(llele);
            } else if (ele->encoding == REDIS_ENCODING_RAW) {
                ele = dupStringObject(ele);
            } else if (ele->encoding == REDIS_ENCODING_INT) {
                ele = createStringObjectFromLongLong((long)ele->ptr);
            }

            /* Try to add the object to the dictionary. If it already exists
             * free it, otherwise increment the number of objects we have
             * in the result dictionary. */
            // O(1)
            if (dictAdd(d,ele,NULL) == DICT_OK)
                added++;
            else
                decrRefCount(ele);
        }
    }

    /* CASE 3 & 4: send the result to the user. */
    // O(N)
    {
        dictIterator *di;
        dictEntry *de;

        addReplyMultiBulkLen(c,count);
        di = dictGetIterator(d);
        while((de = dictNext(di)) != NULL)
            addReplyBulk(c,dictGetKey(de));
        dictReleaseIterator(di);
        dictRelease(d);
    }
}

/*
 * 返回集合中的单个随机元素
 *
 * T = O(N)
 */
void srandmemberCommand(redisClient *c) {
    robj *set, *ele;
    int64_t llele;
    int encoding;

    if (c->argc == 3) {
        srandmemberWithCountCommand(c);
        return;
    } else if (c->argc > 3) {
        addReply(c,shared.syntaxerr);
        return;
    }

    // 获取/创建对象，并检查它是否集合
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,set,REDIS_SET)) return;

    // 获取随机元素
    // O(N)
    encoding = setTypeRandomElement(set,&ele,&llele);
    if (encoding == REDIS_ENCODING_INTSET) {
        addReplyBulkLongLong(c,llele);
    } else {
        addReplyBulk(c,ele);
    }
}

/*
 * 对比两个集合的基数
 *
 * T = O(1)
 */
int qsortCompareSetsByCardinality(const void *s1, const void *s2) {
    return setTypeSize(*(robj**)s1)-setTypeSize(*(robj**)s2);
}

/* This is used by SDIFF and in this case we can receive NULL that should
 * be handled as empty sets. */
/*
 * 对比两个集合的基数是否相同
 *
 * T = O(1)
 */
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2) {
    robj *o1 = *(robj**)s1, *o2 = *(robj**)s2;

    return  (o2 ? setTypeSize(o2) : 0) - (o1 ? setTypeSize(o1) : 0);
}

/*
 * T = O(N^2 lg N)
 */
void sinterGenericCommand(redisClient *c, robj **setkeys, unsigned long setnum, robj *dstkey) {
    // 分配指针数组，数量等于 setnum
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    // 集合迭代器
    setTypeIterator *si;
    robj *eleobj,
         *dstset = NULL;
    int64_t intobj;
    void *replylen = NULL;
    unsigned long j, cardinality = 0;
    int encoding;

    // 将所有集合对象指针保存到 sets 数组
    // O(N)
    for (j = 0; j < setnum; j++) {
        robj *setobj = dstkey ?
            lookupKeyWrite(c->db,setkeys[j]) :
            lookupKeyRead(c->db,setkeys[j]);
        if (!setobj) {
            zfree(sets);
            if (dstkey) {
                if (dbDelete(c->db,dstkey)) {
                    signalModifiedKey(c->db,dstkey);
                    server.dirty++;
                }
                addReply(c,shared.czero);
            } else {
                addReply(c,shared.emptymultibulk);
            }
            return;
        }
        if (checkType(c,setobj,REDIS_SET)) {
            zfree(sets);
            return;
        }
        sets[j] = setobj;
    }

    /* Sort sets from the smallest to largest, this will improve our
     * algorithm's performace */
    // 按集合的基数从小到大排序集合
    // O(N^2)
    qsort(sets,setnum,sizeof(robj*),qsortCompareSetsByCardinality);

    /* The first thing we should output is the total number of elements...
     * since this is a multi-bulk write, but at this stage we don't know
     * the intersection set size, so we use a trick, append an empty object
     * to the output list and save the pointer to later modify it with the
     * right length */
    if (!dstkey) {
        replylen = addDeferredMultiBulkLength(c);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with an empty set inside */
        dstset = createIntsetObject();
    }

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    // 取出 sets[0] 的元素，和其他元素进行交集操作，
    // 只要某个集合有一个不包含 sets[0] 的元素，
    // 那么这个元素就不被包含在交集结果集里面

    // 创建迭代器
    si = setTypeInitIterator(sets[0]);
    // 遍历 sets[0] , O(N^2 lg N)
    while((encoding = setTypeNext(si,&eleobj,&intobj)) != -1) {
        // 和其他集合做交集操作
        // O(N lg N)
        for (j = 1; j < setnum; j++) {
            // 跳过相同的集合
            if (sets[j] == sets[0]) continue;

            // sets[0] 是 intset 时。。。
            if (encoding == REDIS_ENCODING_INTSET) {
                /* intset with intset is simple... and fast */
                // O(lg N)
                if (sets[j]->encoding == REDIS_ENCODING_INTSET &&
                    !intsetFind((intset*)sets[j]->ptr,intobj))
                {
                    break;
                /* in order to compare an integer with an object we
                 * have to use the generic function, creating an object
                 * for this */
                } else if (sets[j]->encoding == REDIS_ENCODING_HT) {
                    eleobj = createStringObjectFromLongLong(intobj);
                    // O(1)
                    if (!setTypeIsMember(sets[j],eleobj)) {
                        decrRefCount(eleobj);
                        break;
                    }
                    decrRefCount(eleobj);
                }
            // sets[0] 是字典时。。。
            } else if (encoding == REDIS_ENCODING_HT) {
                /* Optimization... if the source object is integer
                 * encoded AND the target set is an intset, we can get
                 * a much faster path. */
                if (eleobj->encoding == REDIS_ENCODING_INT &&
                    sets[j]->encoding == REDIS_ENCODING_INTSET &&
                    !intsetFind((intset*)sets[j]->ptr,(long)eleobj->ptr))
                {
                    break;
                /* else... object to object check is easy as we use the
                 * type agnostic API here. */
                } else if (!setTypeIsMember(sets[j],eleobj)) {
                    break;
                }
            }
        }

        /* Only take action when all sets contain the member */
        // 只在所有集合都带有 eleobj/intobj 时，才输出它
        if (j == setnum) {
            // 没有 dstkey ，直接返回给输出
            if (!dstkey) {
                if (encoding == REDIS_ENCODING_HT)
                    addReplyBulk(c,eleobj);
                else
                    addReplyBulkLongLong(c,intobj);
                cardinality++;
            // 有 dstkey ，添加到 dstkey
            } else {
                if (encoding == REDIS_ENCODING_INTSET) {
                    eleobj = createStringObjectFromLongLong(intobj);
                    setTypeAdd(dstset,eleobj);
                    decrRefCount(eleobj);
                } else {
                    setTypeAdd(dstset,eleobj);
                }
            }
        }
    }
    // 释放迭代器
    setTypeReleaseIterator(si);

    if (dstkey) {
        /* Store the resulting set into the target, if the intersection
         * is not an empty set. */
        // 用结果集合代替原有的 dstkey
        dbDelete(c->db,dstkey);
        if (setTypeSize(dstset) > 0) {
            dbAdd(c->db,dstkey,dstset);
            addReplyLongLong(c,setTypeSize(dstset));
        } else {
            decrRefCount(dstset);
            addReply(c,shared.czero);
        }
        signalModifiedKey(c->db,dstkey);
        server.dirty++;
    } else {
        setDeferredMultiBulkLength(c,replylen,cardinality);
    }

    zfree(sets);
}

void sinterCommand(redisClient *c) {
    sinterGenericCommand(c,c->argv+1,c->argc-1,NULL);
}

void sinterstoreCommand(redisClient *c) {
    sinterGenericCommand(c,c->argv+2,c->argc-2,c->argv[1]);
}

#define REDIS_OP_UNION 0
#define REDIS_OP_DIFF 1
#define REDIS_OP_INTER 2

/*
 * T = O(N^2)
 */
void sunionDiffGenericCommand(redisClient *c, robj **setkeys, int setnum, robj *dstkey, int op) {
    // 集合指针数组
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    // 集合迭代器
    setTypeIterator *si;
    robj *ele, *dstset = NULL;
    int j, cardinality = 0;
    int diff_algo = 1;

    // 收集所有集合对象指针
    // O(N)
    for (j = 0; j < setnum; j++) {
        robj *setobj = dstkey ?
            lookupKeyWrite(c->db,setkeys[j]) :
            lookupKeyRead(c->db,setkeys[j]);
        if (!setobj) {
            sets[j] = NULL;
            continue;
        }
        if (checkType(c,setobj,REDIS_SET)) {
            zfree(sets);
            return;
        }
        sets[j] = setobj;
    }

    /* Select what DIFF algorithm to use.
     *
     * Algorithm 1 is O(N*M) where N is the size of the element first set
     * and M the total number of sets.
     *
     * Algorithm 2 is O(N) where N is the total number of elements in all
     * the sets.
     *
     * We compute what is the best bet with the current input here. */
    // 根据集合的大小选择恰当的算法
    if (op == REDIS_OP_DIFF && sets[0]) {
        long long algo_one_work = 0, algo_two_work = 0;

        for (j = 0; j < setnum; j++) {
            if (sets[j] == NULL) continue;

            // 计算 N * M
            algo_one_work += setTypeSize(sets[0]);
            // 计算所有集合的总大小
            algo_two_work += setTypeSize(sets[j]);
        }

        /* Algorithm 1 has better constant times and performs less operations
         * if there are elements in common. Give it some advantage. */
        algo_one_work /= 2;
    
        // 选择算法
        diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2;

        if (diff_algo == 1 && setnum > 1) {
            /* With algorithm 1 it is better to order the sets to subtract
             * by decreasing size, so that we are more likely to find
             * duplicated elements ASAP. */
            qsort(sets+1,setnum-1,sizeof(robj*),
                qsortCompareSetsByRevCardinality);
        }
    }

    /* We need a temp set object to store our union. If the dstkey
     * is not NULL (that is, we are inside an SUNIONSTORE operation) then
     * this set object will be the resulting object to set into the target key*/
    // 用于保存 union 结果的对象
    // 如果 dstkey 不为空，将来这个值就会被保存为 dstkey
    dstset = createIntsetObject();

    // union 操作
    if (op == REDIS_OP_UNION) {
        /* Union is trivial, just add every element of every set to the
         * temporary set. */
        // 遍历所有集合的所有元素，将它们添加到 dstset 上去
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* non existing keys are like empty sets */

            si = setTypeInitIterator(sets[j]);
            while((ele = setTypeNextObject(si)) != NULL) {
                // 已有的元素不会被计数
                if (setTypeAdd(dstset,ele)) cardinality++;
                decrRefCount(ele);
            }
            setTypeReleaseIterator(si);
        }
    } else if (op == REDIS_OP_DIFF && sets[0] && diff_algo == 1) {
        /* DIFF Algorithm 1:
         *
         * We perform the diff by iterating all the elements of the first set,
         * and only adding it to the target set if the element does not exist
         * into all the other sets.
         *
         * This way we perform at max N*M operations, where N is the size of
         * the first set, and M the number of sets. */
        // 遍历 sets[0] ，对于其中的每个元素 elem ，
        // 只有 elem 不存在与其他任何集合时，才将它添加到 dstset
        si = setTypeInitIterator(sets[0]);
        while((ele = setTypeNextObject(si)) != NULL) {
            for (j = 1; j < setnum; j++) {
                if (!sets[j]) continue; /* no key is an empty set. */
                if (setTypeIsMember(sets[j],ele)) break;
            }
            if (j == setnum) {
                /* There is no other set with this element. Add it. */
                // 没有一个集合有 elem 元素，可以添加它
                setTypeAdd(dstset,ele);
                cardinality++;
            }
            decrRefCount(ele);
        }
        setTypeReleaseIterator(si);
    } else if (op == REDIS_OP_DIFF && sets[0] && diff_algo == 2) {
        /* DIFF Algorithm 2:
         *
         * Add all the elements of the first set to the auxiliary set.
         * Then remove all the elements of all the next sets from it.
         *
         * This is O(N) where N is the sum of all the elements in every
         * set. */
        // 将 sets[0] 的所有元素保存到 dstset 对象中
        // 遍历其余的所有集合，如果被遍历集合和 dstset 有相同的元素，
        // 那么从 dstkey 中删除那个元素
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* non existing keys are like empty sets */

            si = setTypeInitIterator(sets[j]);
            while((ele = setTypeNextObject(si)) != NULL) {
                if (j == 0) {
                    // 添加所有元素到 sets[0]
                    if (setTypeAdd(dstset,ele)) cardinality++;
                } else {
                    // 如果在其他集合碰见相同的元素，那么删除它
                    if (setTypeRemove(dstset,ele)) cardinality--;
                }
                decrRefCount(ele);
            }
            setTypeReleaseIterator(si);

            /* Exit if result set is empty as any additional removal
             * of elements will have no effect. */
            if (cardinality == 0) break;
        }
    }

    /* Output the content of the resulting set, if not in STORE mode */
    // 没有 dstkey ，直接输出结果
    if (!dstkey) {
        addReplyMultiBulkLen(c,cardinality);
        si = setTypeInitIterator(dstset);
        while((ele = setTypeNextObject(si)) != NULL) {
            addReplyBulk(c,ele);
            decrRefCount(ele);
        }
        setTypeReleaseIterator(si);
        decrRefCount(dstset);
    // 有 dstkey ，用 dstset 替换原来 dstkey 的对象
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with the result set inside */
        dbDelete(c->db,dstkey);
        if (setTypeSize(dstset) > 0) {
            dbAdd(c->db,dstkey,dstset);
            addReplyLongLong(c,setTypeSize(dstset));
        } else {
            decrRefCount(dstset);
            addReply(c,shared.czero);
        }
        signalModifiedKey(c->db,dstkey);
        server.dirty++;
    }
    zfree(sets);
}

void sunionCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,REDIS_OP_UNION);
}

void sunionstoreCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],REDIS_OP_UNION);
}

void sdiffCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,REDIS_OP_DIFF);
}

void sdiffstoreCommand(redisClient *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],REDIS_OP_DIFF);
}
