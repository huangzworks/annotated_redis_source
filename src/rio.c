/* rio.c is a simple stream-oriented I/O abstraction that provides an interface
 * to write code that can consume/produce data using different concrete input
 * and output devices. 
 *
 * RIO 是一个可以面向流、可用于对多种不同的输入
 * （目前是文件和内存字节）进行编程的抽象。
 *
 * For instance the same rdb.c code using the rio
 * abstraction can be used to read and write the RDB format using in-memory
 * buffers or files.
 *
 * 比如说，RIO 可以同时对内存或文件中的 RDB 格式进行读写。
 *
 * A rio object provides the following methods:
 * 一个 RIO 对象提供以下方法：
 *
 *  read: read from stream.
 *        从流中读取
 *  write: write to stream.
 *         写入到流中
 *  tell: get the current offset.
 *        获取当前的偏移量
 *
 * It is also possible to set a 'checksum' method that is used by rio.c in order
 * to compute a checksum of the data written or read, or to query the rio object
 * for the current checksum.
 * 还可以通过设置 checksum 函数，计算写入/读取内容的校验和，
 * 或者取出的那个前 rio 对象的校验和。
 *
 * ----------------------------------------------------------------------------
 *
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


#include "fmacros.h"
#include <string.h>
#include <stdio.h>
#include "rio.h"
#include "util.h"
#include "crc64.h"

/* Returns 1 or 0 for success/failure. */
/*
 * 将给定内容 buf 写入到缓存中，长度为 len 。
 *
 * 成功返回 1 ，失败返回 0 。
 */
static size_t rioBufferWrite(rio *r, const void *buf, size_t len) {
    r->io.buffer.ptr = sdscatlen(r->io.buffer.ptr,(char*)buf,len);
    r->io.buffer.pos += len;
    return 1;
}

/* Returns 1 or 0 for success/failure. */
/*
 * 从 RIO 缓存复制 len 字节内容到 buf 里面。
 *
 * 复制成功返回 1 ，否则返回 0 。
 */
static size_t rioBufferRead(rio *r, void *buf, size_t len) {
    // RIO 缓存没有足够空间
    if (sdslen(r->io.buffer.ptr)-r->io.buffer.pos < len)
        return 0; /* not enough buffer to return len bytes. */
    
    // 复制到缓存
    memcpy(buf,r->io.buffer.ptr+r->io.buffer.pos,len);
    r->io.buffer.pos += len;

    return 1;
}

/* Returns read/write position in buffer. */
/*
 * 返回缓存的当前字节位置
 */
static off_t rioBufferTell(rio *r) {
    return r->io.buffer.pos;
}

/* Returns 1 or 0 for success/failure. */
/*
 * 将长度为 len 的内容 buf 写入到文件中。
 *
 * 成功返回 1 ，失败返回 0 。
 */
static size_t rioFileWrite(rio *r, const void *buf, size_t len) {
    return fwrite(buf,len,1,r->io.file.fp);
}

/* Returns 1 or 0 for success/failure. */
/*
 * 从文件中读取 len 字节到 buf 中。
 *
 * 返回值为读取的字节数。
 */
static size_t rioFileRead(rio *r, void *buf, size_t len) {
    return fread(buf,len,1,r->io.file.fp);
}

/* Returns read/write position in file. */
/*
 * 返回当前的文件位置
 */
static off_t rioFileTell(rio *r) {
    return ftello(r->io.file.fp);
}

/*
 * 流为内存时所使用的结构
 */
static const rio rioBufferIO = {
    rioBufferRead,
    rioBufferWrite,
    rioBufferTell,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    { { NULL, 0 } } /* union for io-specific vars */
};

/*
 * 流为文件时所使用的结构
 */
static const rio rioFileIO = {
    rioFileRead,
    rioFileWrite,
    rioFileTell,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    { { NULL, 0 } } /* union for io-specific vars */
};

/*
 * 初始化文件流
 */
void rioInitWithFile(rio *r, FILE *fp) {
    *r = rioFileIO;
    r->io.file.fp = fp;
}

/*
 * 初始化内存流
 */
void rioInitWithBuffer(rio *r, sds s) {
    *r = rioBufferIO;
    r->io.buffer.ptr = s;
    r->io.buffer.pos = 0;
}

/* This function can be installed both in memory and file streams when checksum
 * computation is needed. */
/*
 * 通用校验和计算函数
 */
void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len) {
    r->cksum = crc64(r->cksum,buf,len);
}

/* ------------------------------ Higher level interface ---------------------------
 * The following higher level functions use lower level rio.c functions to help
 * generating the Redis protocol for the Append Only File.
 *
 * 以下高阶函数通过调用上面的底层函数来生成 AOF 文件所需的协议
 */

/* Write multi bulk count in the format: "*<count>\r\n". */
/*
 * 以带 '\r\n' 后缀的形式写入字符串表示的 count 到 RIO 
 *
 * 成功返回写入的数量，失败返回 0 。
 */
size_t rioWriteBulkCount(rio *r, char prefix, int count) {
    char cbuf[128];
    int clen;

    // cbuf = prefix ++ count ++ '\r\n'
    cbuf[0] = prefix;
    clen = 1+ll2string(cbuf+1,sizeof(cbuf)-1,count);
    cbuf[clen++] = '\r';
    cbuf[clen++] = '\n';

    if (rioWrite(r,cbuf,clen) == 0) return 0;
    return clen;
}

/* Write binary-safe string in the format: "$<count>\r\n<payload>\r\n". */
/*
 * 以 "$<count>\r\n<payload>\r\n" 的形式写入二进制安全字符
 */
size_t rioWriteBulkString(rio *r, const char *buf, size_t len) {
    size_t nwritten;

    if ((nwritten = rioWriteBulkCount(r,'$',len)) == 0) return 0;
    if (len > 0 && rioWrite(r,buf,len) == 0) return 0;
    if (rioWrite(r,"\r\n",2) == 0) return 0;
    return nwritten+len+2;
}

/* Write a long long value in format: "$<count>\r\n<payload>\r\n". */
/*
 * 以 "$<count>\r\n<payload>\r\n" 的格式写入 long long 值
 */
size_t rioWriteBulkLongLong(rio *r, long long l) {
    char lbuf[32];
    unsigned int llen;

    llen = ll2string(lbuf,sizeof(lbuf),l);
    return rioWriteBulkString(r,lbuf,llen);
}

/* Write a double value in the format: "$<count>\r\n<payload>\r\n" */
/*
 * 以 "$<count>\r\n<payload>\r\n" 的格式写入 double 值
 */
size_t rioWriteBulkDouble(rio *r, double d) {
    char dbuf[128];
    unsigned int dlen;

    dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
    return rioWriteBulkString(r,dbuf,dlen);
}
