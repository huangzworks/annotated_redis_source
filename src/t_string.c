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
 * 检查字符串大小是否超过限制
 */
static int checkStringLength(redisClient *c, long long size) {
    if (size > 512*1024*1024) {
        addReplyError(c,"string exceeds maximum allowed size (512MB)");
        return REDIS_ERR;
    }
    return REDIS_OK;
}

/*
 * 通用 set 命令，用于实现 set / setex / setnx 等命令 
 */
void setGenericCommand(
    redisClient *c,  // 客户端
    int nx,          // Not eXists 命令？
    robj *key,       // key
    robj *val,       // val
    robj *expire,    // expire 时间
    int unit         // 过期时间以秒计算？
)        
{
    long long milliseconds = 0; /* initialized to avoid an harmness warning */

    // 如果带有 expire 参数，那么将它从字符串转为 long long 类型
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

    // 如果给定了 nx 参数，那么只有在 key 不存在时设置才继续
    // 否则返回 shared.czero ，也即是 0
    if (nx && lookupKeyWrite(c->db,key) != NULL) {
        addReply(c,shared.czero);
        return;
    }

    // 设置 key-value
    setKey(c->db,key,val);
    server.dirty++;

    // 设置 expire time
    if (expire) setExpire(c->db,key,mstime()+milliseconds);

    // 向客户端返回回复
    addReply(c, nx ? shared.cone : shared.ok);
}

void setCommand(redisClient *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,0,c->argv[1],c->argv[2],NULL,0);
}

void setnxCommand(redisClient *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,1,c->argv[1],c->argv[2],NULL,0);
}

void setexCommand(redisClient *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,0,c->argv[1],c->argv[3],c->argv[2],UNIT_SECONDS);
}

void psetexCommand(redisClient *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,0,c->argv[1],c->argv[3],c->argv[2],UNIT_MILLISECONDS);
}

/*
 * 根据客户端指定的 key ，查找相应的值。
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

void getCommand(redisClient *c) {
    getGenericCommand(c);
}

void getsetCommand(redisClient *c) {
    // 获取现有值，并添加到客户端回复 buffer 中
    if (getGenericCommand(c) == REDIS_ERR) return;
    // 设置新值
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setKey(c->db,c->argv[1],c->argv[2]);
    server.dirty++;
}

void setrangeCommand(redisClient *c) {
    robj *o;
    long offset;

    // 用来替换旧内容的字符串
    sds value = c->argv[3]->ptr;

    // 检查 offset 是否为 long 类型值
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
        // key 存在 ....

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

        // 当 o 是共享对象或者编码对象时，创建一个副本
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
        // 复制
        memcpy((char*)o->ptr+offset,value,sdslen(value));
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;
    }

    // 将修改后的字符串的长度返回给客户端
    addReplyLongLong(c,sdslen(o->ptr));
}

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

    // 检查 key 是否不存在，或者不为 String 类型
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
        addReplyBulkCBuffer(c,(char*)str+start,end-start+1);
    }
}

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

void msetGenericCommand(redisClient *c, int nx) {
    int j, busykeys = 0;

    if ((c->argc % 2) == 0) {
        addReplyError(c,"wrong number of arguments for MSET");
        return;
    }

    // 当 NX 选项打开时，检查给定的 key 是否已经存在
    // 如果任一个 key 存在的话，不进行修改，直接返回 0
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

void msetCommand(redisClient *c) {
    msetGenericCommand(c,0);
}

void msetnxCommand(redisClient *c) {
    msetGenericCommand(c,1);
}

void incrDecrCommand(redisClient *c, long long incr) {
    long long value, oldvalue;
    robj *o, *new;

    // 查找 key
    o = lookupKeyWrite(c->db,c->argv[1]);

    // 如果 key 非空且 key 类型错误，直接返回
    if (o != NULL && checkType(c,o,REDIS_STRING)) return;

    // 如果值不能转换为数字，直接返回
    if (getLongLongFromObjectOrReply(c,o,&value,NULL) != REDIS_OK) return;

    // 溢出和值是否会溢出
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

void incrCommand(redisClient *c) {
    incrDecrCommand(c,1);
}

void decrCommand(redisClient *c) {
    incrDecrCommand(c,-1);
}

void incrbyCommand(redisClient *c) {
    long long incr;

    // 获取增量数值
    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK) return;

    incrDecrCommand(c,incr);
}

void decrbyCommand(redisClient *c) {
    long long incr;

    // 获取减量数值
    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != REDIS_OK) return;

    incrDecrCommand(c,-incr);
}

void incrbyfloatCommand(redisClient *c) {
    long double incr, value;
    robj *o, *new, *aux;

    // 获取 key 对象
    o = lookupKeyWrite(c->db,c->argv[1]);

    // 如果对象存在且不为 string 类型，直接返回
    if (o != NULL && checkType(c,o,REDIS_STRING)) return;

    // 如果对象 o 或者传入增量参数不是浮点数，直接返回
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

    // 创建并保存新对象
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
 */
void appendCommand(redisClient *c) {
    size_t totlen;
    robj *o, *append;

    // 查找 key 对象
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        // 对象不存在，创建
        c->argv[2] = tryObjectEncoding(c->argv[2]);
        dbAdd(c->db,c->argv[1],c->argv[2]);
        incrRefCount(c->argv[2]);
        totlen = stringObjectLen(c->argv[2]);
    } else {
        // 对象存在，检查它是否为字符串类型
        if (checkType(c,o,REDIS_STRING))
            return;

        // 检查 append 参数和 key 的字符串的长度是否合法
        append = c->argv[2];
        totlen = stringObjectLen(o) + sdslen(append->ptr);
        if (checkStringLength(c,totlen) != REDIS_OK)
            return;

        // 如果 key 对象是共享或被编码的，那么创建一个副本
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

void strlenCommand(redisClient *c) {
    robj *o;

    // 如果 o 不存在，或者不为 string 类型，直接返回
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,REDIS_STRING)) return;

    addReplyLongLong(c,stringObjectLen(o));
}
