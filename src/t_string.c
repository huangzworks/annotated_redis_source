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
#include <math.h> /* isnan(), isinf() */

/*-----------------------------------------------------------------------------
 * String Commands
 *----------------------------------------------------------------------------*/

/*
 * 检查长度 size 是否超过 Redis 的最大限制
 *
 * 复杂度：O(1)
 *
 * 返回值：
 *  REDIS_ERR   超过
 *  REDIS_OK    未超过
 */
static int checkStringLength(redisClient *c, long long size) {
    if (size > 512*1024*1024) {
        addReplyError(c,"string exceeds maximum allowed size (512MB)");
        return REDIS_ERR;
    }
    return REDIS_OK;
}

/*
 * 通用 set 命令，用于 SET / SETEX 和 SETNX 等命令的底层实现
 *
 * 参数：
 *  c   客户端
 *  nx  如果不为 0 ，那么表示只有在 key 不存在时才进行 set 操作
 *  key 
 *  val
 *  expire  过期时间
 *  unit    过期时间的单位，分为 UNIT_SECONDS 和 UNIT_MILLISECONDS
 *
 * 复杂度：O(1)
 *
 * 返回值：void
 */
void setGenericCommand(redisClient *c, int nx, robj *key, robj *val, robj *expire, int unit)        
{
    long long milliseconds = 0; /* initialized to avoid an harmness warning */

    // 如果带有 expire 参数，那么将它从 sds 转为 long long 类型
    if (expire) {
        if (getLongLongFromObjectOrReply(c, expire, &milliseconds, NULL) != REDIS_OK)
            return;
        if (milliseconds <= 0) {
            addReplyError(c,"invalid expire time in SETEX");
            return;
        }
        
        // 决定过期时间是秒还是毫秒
        if (unit == UNIT_SECONDS) milliseconds *= 1000;
    }

    // 如果给定了 nx 参数，并且 key 已经存在，那么直接向客户端返回
    if (nx && lookupKeyWrite(c->db,key) != NULL) {
        addReply(c,shared.czero);
        return;
    }

    // 设置 key-value 对
    setKey(c->db,key,val);

    server.dirty++;

    // 为 key 设置过期时间
    if (expire) setExpire(c->db,key,mstime()+milliseconds);

    // 向客户端返回回复
    addReply(c, nx ? shared.cone : shared.ok);
}

/*
 * SET 命令的实现
 *
 * 复杂度：O(1)
 *
 * 返回值：void
 */
void setCommand(redisClient *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,0,c->argv[1],c->argv[2],NULL,0);
}

/*
 * SETNX 命令的实现
 *
 * 复杂度：
 *  O(1)
 *
 * 返回值：void
 */
void setnxCommand(redisClient *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,1,c->argv[1],c->argv[2],NULL,0);
}

/*
 * SETEX 命令的实现
 *
 * 复杂度：
 *  O(1)
 *
 * 返回值：void
 */
void setexCommand(redisClient *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,0,c->argv[1],c->argv[3],c->argv[2],UNIT_SECONDS);
}

/*
 * PSETEX 命令的实现
 *
 * 复杂度：
 *  O(1)
 *
 * 返回值：void
 */
void psetexCommand(redisClient *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,0,c->argv[1],c->argv[3],c->argv[2],UNIT_MILLISECONDS);
}

/*
 * 根据客户端指定的 key ，查找相应的值。
 * 各种 get 命令的底层实现。
 *
 * 复杂度：
 *  O(1)
 *
 * 返回值：
 *  REDIS_OK    查找完成（可能找到，也可能没找到）
 *  REDIS_ERR   找到，但 key 不是字符串类型
 */
int getGenericCommand(redisClient *c) {
    robj *o;

    // 查找
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL)
        return REDIS_OK;

    // 返回
    if (o->type != REDIS_STRING) {
        addReply(c,shared.wrongtypeerr);
        return REDIS_ERR;
    } else {
        addReplyBulk(c,o);
        return REDIS_OK;
    }
}

/*
 * GET 命令的实现
 *
 * 复杂度：O(1)
 *
 * 返回值：void
 */
void getCommand(redisClient *c) {
    getGenericCommand(c);
}

/*
 * GETSET 命令的实现
 *
 * 复杂度：O(1)
 *
 * 返回值：void
 */
void getsetCommand(redisClient *c) {

    // 获取现有值，并添加到客户端回复 buffer 中
    if (getGenericCommand(c) == REDIS_ERR) return;

    // 设置新值
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setKey(c->db,c->argv[1],c->argv[2]);

    server.dirty++;
}

/*
 * SETRANGE 命令的实现
 *
 * 复杂度：O(N)
 *
 * 返回值：void
 */
void setrangeCommand(redisClient *c) {
    robj *o;
    long offset;

    // 用来替换旧内容的字符串
    sds value = c->argv[3]->ptr;

    // 将 offset 转换为 long 类型值
    if (getLongFromObjectOrReply(c,c->argv[2],&offset,NULL) != REDIS_OK)
        return;

    // 检查 offset 是否位于合法范围
    if (offset < 0) {
        addReplyError(c,"offset is out of range");
        return;
    }

    // 查找给定 key
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        // key 不存在 ...

        // 当 value 为空字符串，且 key 不存在时，返回 0
        if (sdslen(value) == 0) {
            addReply(c,shared.czero);
            return;
        }

        // 当 value 的长度过大时，直接返回
        if (checkStringLength(c,offset+sdslen(value)) != REDIS_OK)
            return;

        // 将 key 设置为空字符串对象
        o = createObject(REDIS_STRING,sdsempty());
        dbAdd(c->db,c->argv[1],o);
    } else {
        // key 存在 ...

        size_t olen;

        // 检查 key 是否字符串
        if (checkType(c,o,REDIS_STRING))
            return;

        // 如果 value 为空字符串，那么直接返回原有字符串的长度
        olen = stringObjectLen(o);
        if (sdslen(value) == 0) {
            addReplyLongLong(c,olen);
            return;
        }

        // 检查修改后的字符串长度会否超过最大限制
        if (checkStringLength(c,offset+sdslen(value)) != REDIS_OK)
            return;

        // 当 o 是共享对象或者编码对象时，创建它的一个副本
        if (o->refcount != 1 || o->encoding != REDIS_ENCODING_RAW) {
            robj *decoded = getDecodedObject(o);
            o = createStringObject(decoded->ptr, sdslen(decoded->ptr));
            decrRefCount(decoded);
            dbOverwrite(c->db,c->argv[1],o);
        }
    }

    // 进行修改操作
    if (sdslen(value) > 0) {
        // 先用 0 字节填充整个范围
        o->ptr = sdsgrowzero(o->ptr,offset+sdslen(value));

        // 复制内容到 key
        memcpy((char*)o->ptr+offset,value,sdslen(value));

        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;
    }

    // 将修改后字符串的长度返回给客户端
    addReplyLongLong(c,sdslen(o->ptr));
}

/*
 * GETRANGE 命令的实现
 *
 * 复杂度：O(N)
 *
 * 返回值：void
 */
void getrangeCommand(redisClient *c) {
    robj *o;
    long start, end;
    char *str, llbuf[32];
    size_t strlen;

    // 获取 start 索引
    if (getLongFromObjectOrReply(c,c->argv[2],&start,NULL) != REDIS_OK)
        return;

    // 获取 end 索引
    if (getLongFromObjectOrReply(c,c->argv[3],&end,NULL) != REDIS_OK)
        return;

    // 如果 key 不存在，或者 key 不是字符串类型，那么直接返回
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptybulk)) == NULL ||
        checkType(c,o,REDIS_STRING)) return;

    // 获取字符串，以及它的长度
    if (o->encoding == REDIS_ENCODING_INT) {
        str = llbuf;
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr);
    } else {
        str = o->ptr;
        strlen = sdslen(str);
    }

    // 对负数索引进行转换
    if (start < 0) start = strlen+start;
    if (end < 0) end = strlen+end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    if ((unsigned)end >= strlen) end = strlen-1;

    /* Precondition: end >= 0 && end < strlen, so the only condition where
     * nothing can be returned is: start > end. */
    if (start > end) {
        addReply(c,shared.emptybulk);
    } else {
        // 返回字符串给定索引的内容
        addReplyBulkCBuffer(c,(char*)str+start,end-start+1);
    }
}

/*
 * MGET 命令的实现
 *
 * 复杂度：O(N)
 *
 * 返回值：void
 */
void mgetCommand(redisClient *c) {
    int j;

    // 执行多个读取
    addReplyMultiBulkLen(c,c->argc-1);
    for (j = 1; j < c->argc; j++) {
        robj *o = lookupKeyRead(c->db,c->argv[j]);
        if (o == NULL) {
            addReply(c,shared.nullbulk);
        } else {
            if (o->type != REDIS_STRING) {
                addReply(c,shared.nullbulk);
            } else {
                addReplyBulk(c,o);
            }
        }
    }
}

/*
 * MSET / MSETNX 命令的底层实现
 *
 * 参数：
 *  nx  如果不为 0 ，那么只有在 key 不存在时才进行设置
 *
 * 复杂度：O(N)
 *
 * 返回值：void
 */
void msetGenericCommand(redisClient *c, int nx) {
    int j, busykeys = 0;

    // 检查输入参数是否成对
    if ((c->argc % 2) == 0) {
        addReplyError(c,"wrong number of arguments for MSET");
        return;
    }

    // 当 nx 选项打开时，检查给定的 key 是否已经存在
    // 只要任何一个 key 存在，那么就不进行修改，直接返回 0
    if (nx) {
        for (j = 1; j < c->argc; j += 2) {
            if (lookupKeyWrite(c->db,c->argv[j]) != NULL) {
                busykeys++;
            }
        }
        // 如果有已存在 key ，不做动作直接返回
        if (busykeys) {
            addReply(c, shared.czero);
            return;
        }
    }

    // 执行多个写入
    for (j = 1; j < c->argc; j += 2) {
        c->argv[j+1] = tryObjectEncoding(c->argv[j+1]);
        setKey(c->db,c->argv[j],c->argv[j+1]);
    }

    server.dirty += (c->argc-1)/2;

    addReply(c, nx ? shared.cone : shared.ok);
}

/*
 * MSET 命令的实现
 *
 * 复杂度：O(N)
 *
 * 返回值：void
 */
void msetCommand(redisClient *c) {
    msetGenericCommand(c,0);
}

/*
 * MSETNX 命令的实现
 *
 * 复杂度：O(N)
 *
 * 返回值：void
 */
void msetnxCommand(redisClient *c) {
    msetGenericCommand(c,1);
}

/*
 * 对给定字符串保存的数值进行加法或者减法操作
 * incr / decr / incrby 和 decrby 等命令的底层实现
 *
 * 复杂度：O(1)
 *
 * 返回值：void
 */
void incrDecrCommand(redisClient *c, long long incr) {
    long long value, oldvalue;
    robj *o, *new;

    // 查找 key
    o = lookupKeyWrite(c->db,c->argv[1]);

    // 如果 key 非空且 key 类型错误，直接返回
    if (o != NULL && checkType(c,o,REDIS_STRING)) return;

    // 如果值不能转换为数字，直接返回
    if (getLongLongFromObjectOrReply(c,o,&value,NULL) != REDIS_OK) return;

    // 检查和值是否会溢出
    oldvalue = value;
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }

    // 计算和值
    value += incr;
    // 保存结果到对象
    new = createStringObjectFromLongLong(value);
    // 根据 o 对象是否存在，选择覆写或者新建对象
    if (o)
        dbOverwrite(c->db,c->argv[1],new);
    else
        dbAdd(c->db,c->argv[1],new);

    signalModifiedKey(c->db,c->argv[1]);

    server.dirty++;

    addReply(c,shared.colon);
    addReply(c,new);
    addReply(c,shared.crlf);
}

/*
 * INCR 命令的实现
 *
 * 复杂度：O(1)
 *
 * 返回值：void
 */
void incrCommand(redisClient *c) {
    incrDecrCommand(c,1);
}

/*
 * DECR 命令的实现
 *
 * 复杂度：O(1)
 *
 * 返回值：void
 */
void decrCommand(redisClient *c) {
    incrDecrCommand(c,-1);
}

/*
 * incrby 命令的实现
 *
 * 复杂度：O(1)
 *
 * 返回值：void
 */
void incrbyCommand(redisClient *c) {
    long long incr;

    // 获取增量数值
    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK) return;

    incrDecrCommand(c,incr);
}

/*
 * decrby 命令的实现
 *
 * 复杂度：O(1)
 *
 * 返回值：void
 */
void decrbyCommand(redisClient *c) {
    long long incr;

    // 获取减量数值
    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK) return;

    incrDecrCommand(c,-incr);
}

/*
 * INCRBYFLOAT 命令的实现
 *
 * 复杂度：O(1)
 *
 * 返回值：void
 */
void incrbyfloatCommand(redisClient *c) {
    long double incr, value;
    robj *o, *new, *aux;

    // 获取 key 对象
    o = lookupKeyWrite(c->db,c->argv[1]);

    // 如果对象存在且不为字符串类型，直接返回
    if (o != NULL && checkType(c,o,REDIS_STRING)) return;

    // 如果对象 o 或者传入增量参数不能转换为浮点数，直接返回
    if (getLongDoubleFromObjectOrReply(c,o,&value,NULL) != REDIS_OK ||
        getLongDoubleFromObjectOrReply(c,c->argv[2],&incr,NULL) != REDIS_OK)
        return;

    // 计算和
    value += incr;
    // 溢出检查
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }

    // 将值保存到新对象，并覆写原有对象
    new = createStringObjectFromLongDouble(value);
    if (o)
        dbOverwrite(c->db,c->argv[1],new);
    else
        dbAdd(c->db,c->argv[1],new);

    signalModifiedKey(c->db,c->argv[1]);

    server.dirty++;

    addReplyBulk(c,new);

    /* Always replicate INCRBYFLOAT as a SET command with the final value
     * in order to make sure that differences in float pricision or formatting
     * will not create differences in replicas or after an AOF restart. */
    aux = createStringObject("SET",3);
    rewriteClientCommandArgument(c,0,aux);
    decrRefCount(aux);
    rewriteClientCommandArgument(c,2,new);
}

/*
 * APPEND 命令的实现
 *
 * 复杂度：O(N)
 *
 * 返回值：void
 */
void appendCommand(redisClient *c) {
    size_t totlen;
    robj *o, *append;

    // 查找 key 对象
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        // 对象不存在 ...
        
        // 创建字符串对象，并将它添加到数据库
        c->argv[2] = tryObjectEncoding(c->argv[2]);
        dbAdd(c->db,c->argv[1],c->argv[2]);
        incrRefCount(c->argv[2]);
        totlen = stringObjectLen(c->argv[2]);
    } else {
        // 对象存在...
        
        // 如果不是字符串，直接返回
        if (checkType(c,o,REDIS_STRING))
            return;

        // 检查拼接完成后，字符串的长度是否合法
        append = c->argv[2];
        totlen = stringObjectLen(o) + sdslen(append->ptr);
        if (checkStringLength(c,totlen) != REDIS_OK)
            return;

        // 如果 key 对象是被共享或未被编码的，那么创建一个副本
        if (o->refcount != 1 || o->encoding != REDIS_ENCODING_RAW) {
            robj *decoded = getDecodedObject(o);
            o = createStringObject(decoded->ptr, sdslen(decoded->ptr));
            decrRefCount(decoded);
            dbOverwrite(c->db,c->argv[1],o);
        }

        // 进行拼接
        o->ptr = sdscatlen(o->ptr,append->ptr,sdslen(append->ptr));
        totlen = sdslen(o->ptr);
    }
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
    addReplyLongLong(c,totlen);
}

/*
 * STRLEN 命令的实现
 *
 * 复杂度：O(1)
 *
 * 返回值：void
 */
void strlenCommand(redisClient *c) {
    robj *o;

    // 如果 o 不存在，或者不为 string 类型，直接返回
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_STRING)) return;

    addReplyLongLong(c,stringObjectLen(o));
}
