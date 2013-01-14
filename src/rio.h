/*
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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


#ifndef __REDIS_RIO_H
#define __REDIS_RIO_H

#include <stdio.h>
#include <stdint.h>
#include "sds.h"

struct _rio {
    /* Backend functions.
     * 后端函数
     * Since this functions do not tolerate short writes or reads the return
     * 因为这些函数不返回 short count 
     * value is simplified to: zero on error, non zero on complete success.
     * 如果值为 0 ，那么发生错误，不为 0 则无错误
     */
    size_t (*read)(struct _rio *, void *buf, size_t len);
    size_t (*write)(struct _rio *, const void *buf, size_t len);
    off_t (*tell)(struct _rio *);
    /* The update_cksum method if not NULL is used to compute the checksum of all the
     * data that was read or written so far. 
     * 如果 update_cksum 函数不为空，那么用它计算所有已写入或读取的数据的校验值。
     * The method should be designed so that
     * can be called with the current checksum, and the buf and len fields pointing
     * to the new block of data to add to the checksum computation. 
     * 函数设计为可以使用当前的校验和调用，而 buf 和 len 则指向要计算校验和的新块。
     */
    void (*update_cksum)(struct _rio *, const void *buf, size_t len);

    /* The current checksum */
    // 当前校验和
    uint64_t cksum;

    /* Backend-specific vars. */
    // 后端变量
    union {
        // 处理字节/字符串时使用
        struct {
            sds ptr;
            off_t pos;
        } buffer;
        // 处理文件时使用
        struct {
            FILE *fp;
        } file;
    } io;
};

typedef struct _rio rio;

/* The following functions are our interface with the stream. They'll call the
 * actual implementation of read / write / tell, and will update the checksum
 * if needed. */

/*
 * 写入函数
 *
 * 写入成功返回 1 ，否则返回 0 。
 */
static inline size_t rioWrite(rio *r, const void *buf, size_t len) {
    // 更新校验和
    if (r->update_cksum) r->update_cksum(r,buf,len);
    // 写入数据
    return r->write(r,buf,len);
}

/*
 * 读取函数
 *
 * 读取成功返回 1 ，否则返回 0 。
 */
static inline size_t rioRead(rio *r, void *buf, size_t len) {
    // 读取成功
    if (r->read(r,buf,len) == 1) {
        // 更新校验和，并返回 1 
        if (r->update_cksum) r->update_cksum(r,buf,len);
        return 1;
    }
    return 0;
}

/*
 * 返回当前的偏移量
 */
static inline off_t rioTell(rio *r) {
    return r->tell(r);
}

void rioInitWithFile(rio *r, FILE *fp);
void rioInitWithBuffer(rio *r, sds s);

size_t rioWriteBulkCount(rio *r, char prefix, int count);
size_t rioWriteBulkString(rio *r, const char *buf, size_t len);
size_t rioWriteBulkLongLong(rio *r, long long l);
size_t rioWriteBulkDouble(rio *r, double d);

void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len);

#endif
