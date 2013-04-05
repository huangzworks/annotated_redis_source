/* The ziplist is a specially encoded dually linked list that is designed
 * to be very memory efficient. It stores both strings and integer values,
 * where integers are encoded as actual integers instead of a series of
 * characters. It allows push and pop operations on either side of the list
 * in O(1) time. However, because every operation requires a reallocation of
 * the memory used by the ziplist, the actual complexity is related to the
 * amount of memory used by the ziplist.
 *
 * Ziplist 是为内存占用而特别优化的双链表。
 *
 * 它可以保存字符串和整数，其中整数以整数类型而不是字符串来进行编码和保存。
 *
 * 对 ziplist 的两端进行 push 和 pop 的复杂度都为 O(1) ，
 * 不过，因为对 ziplist 的每次修改操作都需要进行内存重分配，
 * 因此，实际的时间复杂度与 ziplist 使用的内存大小有关。
 *
 * ----------------------------------------------------------------------------
 *
 * ZIPLIST OVERALL LAYOUT:
 *
 * The general layout of the ziplist is as follows:
 *
 * 以下是 ziplist 的内存结构：
 *
 * <zlbytes><zltail><zllen><entry><entry><zlend>
 *
 * <zlbytes> is an unsigned integer to hold the number of bytes that the
 * ziplist occupies. This value needs to be stored to be able to resize the
 * entire structure without the need to traverse it first.
 *
 * <zlbytes> 是一个无符号整数(uint32_t)，用于记录整个 ziplist 所占用的字节数量。
 * 通过保存这个值，可以在不遍历整个 ziplist 的前提下，对整个 ziplist 进行内存重分配。
 *
 * <zltail> is the offset to the last entry in the list. This allows a pop
 * operation on the far side of the list without the need for full traversal.
 *
 * <zltail> 是到列表中最后一个节点的偏移量(同样为 uint32_t)。
 * 有了这个偏移量，就可以在常数复杂度内对表尾进行操作，而不必遍历整个列表。
 *
 * <zllen> is the number of entries.When this value is larger than 2**16-2,
 * we need to traverse the entire list to know how many items it holds.
 *
 * <zllen> 是节点的数量，为 ``uint16_t`` 。
 * 当这个值大于 2**16-2 时，需要遍历整个列表，才能计算出列表的长度
 *
 * <zlend> is a single byte special value, equal to 255, which indicates the
 * end of the list.
 *
 * <zlend> 是一个单字节的特殊值，等于 255 ，它标识了列表的末端。
 *
 * ZIPLIST ENTRIES:
 *
 * Every entry in the ziplist is prefixed by a header that contains two pieces
 * of information. First, the length of the previous entry is stored to be
 * able to traverse the list from back to front. Second, the encoding with an
 * optional string length of the entry itself is stored.
 *
 * Ziplist 中的每个节点，都带有一个 header 作为前缀。
 *
 * Header 包括两部分：
 * 1) 前一个节点的长度，在从后往前遍历时使用
 * 2) 当前节点所保存的值的类型和长度
 *
 * The length of the previous entry is encoded in the following way:
 * If this length is smaller than 254 bytes, it will only consume a single
 * byte that takes the length as value. When the length is greater than or
 * equal to 254, it will consume 5 bytes. The first byte is set to 254 to
 * indicate a larger value is following. The remaining 4 bytes take the
 * length of the previous entry as value.
 *
 * 前一个节点的长度的储存方式如下：
 * 1) 如果节点的长度 < 254 字节，那么直接用一个字节保存这个值。
 * 2) 如果节点的长度 >= 254 字节，那么将第一个字节设置为 254 ，
 *    再在之后用 4 个字节来表示节点的实际长度（共使用 5 个字节）。
 *
 * The other header field of the entry itself depends on the contents of the
 * entry. When the entry is a string, the first 2 bits of this header will hold
 * the type of encoding used to store the length of the string, followed by the
 * actual length of the string. When the entry is an integer the first 2 bits
 * are both set to 1. The following 2 bits are used to specify what kind of
 * integer will be stored after this header. An overview of the different
 * types and encodings is as follows:
 *
 * 另一个 header 域保存的信息取决于这个节点所保存的内容本身。
 *
 * 当节点保存的是字符串时，header 的前 2 位用于指示保存内容长度所使用的编码方式，
 * 之后跟着的是内容长度的值。
 *
 * 当节点保存的是整数时，header 的前 2 位都设置为 1 ，
 * 之后的 2 位用于指示保存的整数值的类型（这个类型决定了内容所占用的空间）。
 *
 * 以下是不同类型 header 的概览：
 * 
 * |00pppppp| - 1 byte
 *      String value with length less than or equal to 63 bytes (6 bits).
 *      长度 <= 63 字节(6 位)的字符串值
 * |01pppppp|qqqqqqqq| - 2 bytes
 *      String value with length less than or equal to 16383 bytes (14 bits).
 *      长度 <= 16383 字节(14 位)的字符串值
 * |10______|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
 *      String value with length greater than or equal to 16384 bytes.
 *      长度 >= 16384 字节， <= 4294967295 的字符串值，
 *
 * |11000000| - 1 byte
 *      Integer encoded as int16_t (2 bytes).
 *      以 int16_t (2 字节)类型编码的整数
 * |11010000| - 1 byte
 *      Integer encoded as int32_t (4 bytes).
 *      以 int32_t (4 字节)类型编码的整数
 * |11100000| - 1 byte
 *      Integer encoded as int64_t (8 bytes).
 *      以 int64_t (8 字节)类型编码的整数
 * |11110000| - 1 byte
 *      Integer encoded as 24 bit signed (3 bytes).
 *      24 位(3 字节)有符号编码整数
 * |11111110| - 1 byte
 *      Integer encoded as 8 bit signed (1 byte).
 *      8 位(1 字节)有符号编码整数
 * |1111xxxx| - (with xxxx between 0000 and 1101) immediate 4 bit integer.
 *      Unsigned integer from 0 to 12. The encoded value is actually from
 *      1 to 13 because 0000 and 1111 can not be used, so 1 should be
 *      subtracted from the encoded 4 bit value to obtain the right value.
 *      (介于 0000 和 1101 之间)的 4 位整数，可用于表示无符号整数 0 至 12 。
 *      因为 0000 和 1111 都已经被占用，因此，可被编码的值实际上只能是 1 至 13 ，
 *      要将这个值减去 1 ，才能得到正确的值。
 * |11111111| - End of ziplist.
 *      ziplist 的终结符
 *
 * All the integers are represented in little endian byte order.
 * 
 * 所有整数都以小端表示。
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <limits.h>
#include "zmalloc.h"
#include "util.h"
#include "ziplist.h"
#include "endianconv.h"

#define ZIP_END 255
#define ZIP_BIGLEN 254

/* Different encoding/length possibilities */
#define ZIP_STR_MASK 0xc0   // 1100, 0000
#define ZIP_INT_MASK 0x30
#define ZIP_STR_06B (0 << 6)
#define ZIP_STR_14B (1 << 6)
#define ZIP_STR_32B (2 << 6)
#define ZIP_INT_16B (0xc0 | 0<<4)
#define ZIP_INT_32B (0xc0 | 1<<4)
#define ZIP_INT_64B (0xc0 | 2<<4)
#define ZIP_INT_24B (0xc0 | 3<<4)
#define ZIP_INT_8B 0xfe
/* 4 bit integer immediate encoding */
#define ZIP_INT_IMM_MASK 0x0f
#define ZIP_INT_IMM_MIN 0xf1    /* 11110001 */
#define ZIP_INT_IMM_MAX 0xfd    /* 11111101 */
#define ZIP_INT_IMM_VAL(v) (v & ZIP_INT_IMM_MASK)

#define INT24_MAX 0x7fffff
#define INT24_MIN (-INT24_MAX - 1)

/* Macro to determine type */
#define ZIP_IS_STR(enc) (((enc) & ZIP_STR_MASK) < ZIP_STR_MASK)

/*
 * 用于取出 zl 各部分值的宏
 *
 * 所有宏复杂度都为 O(1)
 */
// 取出列表以字节计算的列表长度(内存的 0 - 31 位，整数)
#define ZIPLIST_BYTES(zl)       (*((uint32_t*)(zl)))    
// 取出列表的表尾偏移量(内存的 32 - 63 位，整数)
#define ZIPLIST_TAIL_OFFSET(zl) (*((uint32_t*)((zl)+sizeof(uint32_t))))
// 取出列表的长度(内存的 64 - 79 位，整数)
#define ZIPLIST_LENGTH(zl)      (*((uint16_t*)((zl)+sizeof(uint32_t)*2)))
// 列表的 header 长度
#define ZIPLIST_HEADER_SIZE     (sizeof(uint32_t)*2+sizeof(uint16_t))   // 32*2 bit + 16 bit
// 返回列表的 header 之后的位置
#define ZIPLIST_ENTRY_HEAD(zl)  ((zl)+ZIPLIST_HEADER_SIZE)
// 返回列表最后一个元素之后的位置
#define ZIPLIST_ENTRY_TAIL(zl)  ((zl)+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)))
// 返回列表的结束符之前的位置
#define ZIPLIST_ENTRY_END(zl)   ((zl)+intrev32ifbe(ZIPLIST_BYTES(zl))-1)

/* We know a positive increment can only be 1 because entries can only be
 * pushed one at a time. */
/*
 * 对 <zllen> 增一
 *
 * ZIPLIST_LENGTH(zl) 的最大值为 UINT16_MAX
 *
 * 复杂度：O(1)
 */
#define ZIPLIST_INCR_LENGTH(zl,incr) { \
    if (ZIPLIST_LENGTH(zl) < UINT16_MAX) \
        ZIPLIST_LENGTH(zl) = intrev16ifbe(intrev16ifbe(ZIPLIST_LENGTH(zl))+incr); \
}

/*
 * 节点结构
 */
typedef struct zlentry {
    unsigned int prevrawlensize,    // 保存前一节点的长度所需的长度 
                 prevrawlen;        // 前一节点的长度
    unsigned int lensize,           // 保存节点的长度所需的长度
                 len;               // 节点的长度
    unsigned int headersize;        // header 长度
    unsigned char encoding;         // 编码方式
    unsigned char *p;               // 内容
} zlentry;

/* Extract the encoding from the byte pointed by 'ptr' and set it into
 * 'encoding'. */
/*
 * 从 ptr 指针中取出编码类型，并将它保存到 encoding
 *
 * 复杂度：O(1)
 */
#define ZIP_ENTRY_ENCODING(ptr, encoding) do {  \
    (encoding) = (ptr[0]); \
    if ((encoding) < ZIP_STR_MASK) (encoding) &= ZIP_STR_MASK; \
} while(0)

/*
 * 返回 encoding 指定的整数编码方式所需的长度
 *
 * 复杂度：O(1)
 */
static unsigned int zipIntSize(unsigned char encoding) {
    switch(encoding) {
    case ZIP_INT_8B:  return 1;
    case ZIP_INT_16B: return 2;
    case ZIP_INT_24B: return 3;
    case ZIP_INT_32B: return 4;
    case ZIP_INT_64B: return 8;
    default: return 0; /* 4 bit immediate */
    }
    assert(NULL);
    return 0;
}

/* Encode the length 'l' writing it in 'p'. If p is NULL it just returns
 * the amount of bytes required to encode such a length. */
/*
 * 编码长度 l ，并将它写入到 p 。
 * 
 * 如果 p 为 NULL ，那么返回编码 rawlen 所需的字节数
 *
 * 复杂度：O(1)
 */
static unsigned int zipEncodeLength(unsigned char *p, unsigned char encoding, unsigned int rawlen) {
    unsigned char len = 1, buf[5];

    if (ZIP_IS_STR(encoding)) {
        // 字符串编码
        /* Although encoding is given it may not be set for strings,
         * so we determine it here using the raw length. */
        if (rawlen <= 0x3f) {
            // 长度 6 bit ，长度 + 编码 8 bit (1 byte)
            if (!p) return len;
            buf[0] = ZIP_STR_06B | rawlen;
        } else if (rawlen <= 0x3fff) {
            // 长度 14 bit，长度 + 编码 16 bit (2 btyes)
            len += 1;
            if (!p) return len;
            buf[0] = ZIP_STR_14B | ((rawlen >> 8) & 0x3f);
            buf[1] = rawlen & 0xff;
        } else {
            // 长度 32 bit (4 bytes)，长度 + 编码 40 bit (5 bytes)
            len += 4;
            if (!p) return len;
            buf[0] = ZIP_STR_32B;
            buf[1] = (rawlen >> 24) & 0xff;
            buf[2] = (rawlen >> 16) & 0xff;
            buf[3] = (rawlen >> 8) & 0xff;
            buf[4] = rawlen & 0xff;
        }
    } else {
        // 编码为整数，长度总为 1 bytes
        /* Implies integer encoding, so length is always 1. */
        if (!p) return len;
        buf[0] = encoding;
    }

    /* Store this length at p */
    memcpy(p,buf,len);
    return len;
}

/* Decode the length encoded in 'ptr'. The 'encoding' variable will hold the
 * entries encoding, the 'lensize' variable will hold the number of bytes
 * required to encode the entries length, and the 'len' variable will hold the
 * entries length. */
/*
 * 从 ptr 指针中取出节点的编码、保存节点长度所需的长度、以及节点的长度
 *
 * 复杂度：O(1)
 *
 * 返回值：
 *  int 编码节点所需的长度
 */
#define ZIP_DECODE_LENGTH(ptr, encoding, lensize, len) do {                    \
    /* 取出节点的编码 */                                                       \
    ZIP_ENTRY_ENCODING((ptr), (encoding));                                     \
    if ((encoding) < ZIP_STR_MASK) {                                           \
        /* 节点保存的是字符串，取出编码 */                                     \
        if ((encoding) == ZIP_STR_06B) {                                       \
            (lensize) = 1;                                                     \
            (len) = (ptr)[0] & 0x3f;                                           \
        } else if ((encoding) == ZIP_STR_14B) {                                \
            (lensize) = 2;                                                     \
            (len) = (((ptr)[0] & 0x3f) << 8) | (ptr)[1];                       \
        } else if (encoding == ZIP_STR_32B) {                                  \
            (lensize) = 5;                                                     \
            (len) = ((ptr)[1] << 24) |                                         \
                    ((ptr)[2] << 16) |                                         \
                    ((ptr)[3] <<  8) |                                         \
                    ((ptr)[4]);                                                \
        } else {                                                               \
            assert(NULL);                                                      \
        }                                                                      \
    } else {                                                                   \
        /* 节点保存的是整数，取出编码 */                                       \
        (lensize) = 1;                                                         \
        (len) = zipIntSize(encoding);                                          \
    }                                                                          \
} while(0);

/* Encode the length of the previous entry and write it to "p". Return the
 * number of bytes needed to encode this length if "p" is NULL. */
/*
 * 编码前置节点的长度，并将它写入 p 。
 *
 * 如果 p 为 NULL ，那么返回编码 len 所需的字节数。
 *
 * 复杂度：O(1)
 */
static unsigned int zipPrevEncodeLength(unsigned char *p, unsigned int len) {
    if (p == NULL) {
        return (len < ZIP_BIGLEN) ? 1 : sizeof(len)+1;
    } else {
        if (len < ZIP_BIGLEN) {
            p[0] = len;
            return 1;
        } else {
            p[0] = ZIP_BIGLEN;
            memcpy(p+1,&len,sizeof(len));
            memrev32ifbe(p+1);
            return 1+sizeof(len);
        }
    }
}

/* Encode the length of the previous entry and write it to "p". This only
 * uses the larger encoding (required in __ziplistCascadeUpdate). */
/*
 * 将前驱节点的长度 len 写入到 p 中
 * 其中 p 的空间比保存 len 所需的实际空间要更大
 *
 * 这适用于将一个比原节点更短的新节点插入到某个节点时使用
 * 具体参考 __ziplistCascadeUpdate 函数的头注释
 *
 * 复杂度：O(1)
 */
static void zipPrevEncodeLengthForceLarge(unsigned char *p, unsigned int len) {
    if (p == NULL) return;
    p[0] = ZIP_BIGLEN;
    memcpy(p+1,&len,sizeof(len));
    memrev32ifbe(p+1);
}

/* Decode the number of bytes required to store the length of the previous
 * element, from the perspective of the entry pointed to by 'ptr'. */
/*
 * 从指针 ptr 中取出保存前一个节点的长度所需的字节数
 *
 * 复杂度：O(1)
 *
 * 返回值：
 *  unsigned int 
 */
#define ZIP_DECODE_PREVLENSIZE(ptr, prevlensize) do {                          \
    if ((ptr)[0] < ZIP_BIGLEN) {                                               \
        (prevlensize) = 1;                                                     \
    } else {                                                                   \
        (prevlensize) = 5;                                                     \
    }                                                                          \
} while(0);

/* Decode the length of the previous element, from the perspective of the entry
 * pointed to by 'ptr'. */
/*
* 从指针 ptr 中取出前一个节点的长度
*
* 复杂度：O(1)
*
* 返回值：
*   unsigned int
*/
#define ZIP_DECODE_PREVLEN(ptr, prevlensize, prevlen) do {                     \
    /* 取得保存前一个节点的长度所需的字节数 */                                 \
    ZIP_DECODE_PREVLENSIZE(ptr, prevlensize);                                  \
    /* 获取长度值 */                                                           \
    if ((prevlensize) == 1) {                                                  \
        (prevlen) = (ptr)[0];                                                  \
    } else if ((prevlensize) == 5) {                                           \
        assert(sizeof((prevlensize)) == 4);                                    \
        memcpy(&(prevlen), ((char*)(ptr)) + 1, 4);                             \
        memrev32ifbe(&prevlen);                                                \
    }                                                                          \
} while(0);

/* Return the difference in number of bytes needed to store the length of the
 * previous element 'len', in the entry pointed to by 'p'. */
/*
 * 返回编码 len 所需的长度减去编码 p 的前一个节点的大小所需的长度之差
 * 
 * 复杂度：O(1)
 */
static int zipPrevLenByteDiff(unsigned char *p, unsigned int len) {
    // 获取编码前一节点所需的长度
    unsigned int prevlensize;
    ZIP_DECODE_PREVLENSIZE(p, prevlensize);
    // 计算差
    return zipPrevEncodeLength(NULL, len) - prevlensize;
}

/* Return the total number of bytes used by the entry pointed to by 'p'. */
/*
 * 返回 p 指向的节点的空间总长度
 * 
 * 复杂度：O(1)
 */
static unsigned int zipRawEntryLength(unsigned char *p) {
    unsigned int prevlensize, encoding, lensize, len;

    // 保存前驱节点长度的空间长度
    ZIP_DECODE_PREVLENSIZE(p, prevlensize);

    // 保存本节点的空间长度
    ZIP_DECODE_LENGTH(p + prevlensize, encoding, lensize, len);
    
    return prevlensize + lensize + len;
}

/* Check if string pointed to by 'entry' can be encoded as an integer.
 * Stores the integer value in 'v' and its encoding in 'encoding'. */
/*
 * 检查 entry 所保存的值，看它能否编码为整数
 *
 * 复杂度：O(N)，N 为 entry 所保存字符串值的长度
 *
 * 返回值：
 *  如果可以的话，返回 1 ，并将值保存在 v ，将编码保存在 encoding
 *  否则，返回 0
 */
static int zipTryEncoding(unsigned char *entry, unsigned int entrylen, long long *v, unsigned char *encoding) {
    long long value;

    if (entrylen >= 32 || entrylen == 0) return 0;
    // 尝试转换为整数
    if (string2ll((char*)entry,entrylen,&value)) {
        /* Great, the string can be encoded. Check what's the smallest
         * of our encoding types that can hold this value. */
        // 选择整数编码
        if (value >= 0 && value <= 12) {
            *encoding = ZIP_INT_IMM_MIN+value;
        } else if (value >= INT8_MIN && value <= INT8_MAX) {
            *encoding = ZIP_INT_8B;
        } else if (value >= INT16_MIN && value <= INT16_MAX) {
            *encoding = ZIP_INT_16B;
        } else if (value >= INT24_MIN && value <= INT24_MAX) {
            *encoding = ZIP_INT_24B;
        } else if (value >= INT32_MIN && value <= INT32_MAX) {
            *encoding = ZIP_INT_32B;
        } else {
            *encoding = ZIP_INT_64B;
        }
        *v = value;
        return 1;
    }
    return 0;
}

/* Store integer 'value' at 'p', encoded as 'encoding' */
/*
 * 将 value 保存到 p ，并设置编码为 encoding
 *
 * 复杂度：O(1)
 */
static void zipSaveInteger(unsigned char *p, int64_t value, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64;
    // 8 bit 整数
    if (encoding == ZIP_INT_8B) {
        ((int8_t*)p)[0] = (int8_t)value;
    // 16 bit 整数
    } else if (encoding == ZIP_INT_16B) {
        i16 = value;
        memcpy(p,&i16,sizeof(i16));
        memrev16ifbe(p);
    // 24 bit 整数
    } else if (encoding == ZIP_INT_24B) {
        i32 = value<<8;
        memrev32ifbe(&i32);
        memcpy(p,((uint8_t*)&i32)+1,sizeof(i32)-sizeof(uint8_t));
    // 32 bit 整数
    } else if (encoding == ZIP_INT_32B) {
        i32 = value;
        memcpy(p,&i32,sizeof(i32));
        memrev32ifbe(p);
    // 64 bit 整数
    } else if (encoding == ZIP_INT_64B) {
        i64 = value;
        memcpy(p,&i64,sizeof(i64));
        memrev64ifbe(p);
    // 值和编码保存在同一个 byte
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
        /* Nothing to do, the value is stored in the encoding itself. */
    } else {
        assert(NULL);
    }
}

/* Read integer encoded as 'encoding' from 'p' */
/*
 * 根据 encoding ，从指针 p 中取出整数值
 *
 * 复杂度：O(1)
 */
static int64_t zipLoadInteger(unsigned char *p, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64, ret = 0;
    // 8 bit
    if (encoding == ZIP_INT_8B) {
        ret = ((int8_t*)p)[0];
    // 16 bit
    } else if (encoding == ZIP_INT_16B) {
        memcpy(&i16,p,sizeof(i16));
        memrev16ifbe(&i16);
        ret = i16;
    // 32 bit
    } else if (encoding == ZIP_INT_32B) {
        memcpy(&i32,p,sizeof(i32));
        memrev32ifbe(&i32);
        ret = i32;
    // 24 bit
    } else if (encoding == ZIP_INT_24B) {
        i32 = 0;
        memcpy(((uint8_t*)&i32)+1,p,sizeof(i32)-sizeof(uint8_t));
        memrev32ifbe(&i32);
        ret = i32>>8;
    // 64 bit
    } else if (encoding == ZIP_INT_64B) {
        memcpy(&i64,p,sizeof(i64));
        memrev64ifbe(&i64);
        ret = i64;
    // 值和编码保存在同一个 byte
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
        ret = (encoding & ZIP_INT_IMM_MASK)-1;
    } else {
        assert(NULL);
    }
    return ret;
}

/* Return a struct with all information about an entry. */
/*
 * 从指针 p 中提取出节点的各个属性，并将属性保存到 zlentry 结构，然后返回
 *
 * 复杂度：O(1)
 */
static zlentry zipEntry(unsigned char *p) {
    zlentry e;

    // 取出前一个节点的长度
    ZIP_DECODE_PREVLEN(p, e.prevrawlensize, e.prevrawlen);

    // 取出当前节点的编码、保存节点的长度所需的长度、以及节点的长度
    ZIP_DECODE_LENGTH(p + e.prevrawlensize, e.encoding, e.lensize, e.len);
    
    // 记录 header 的长度
    e.headersize = e.prevrawlensize + e.lensize;

    // 记录指针 p
    e.p = p;

    return e;
}

/*
 * 新创建一个空 ziplist
 * 
 * 复杂度：O(1)
 *
 * 返回值：新创建的 ziplist
 */
unsigned char *ziplistNew(void) {
    // 分配 2 个 32 bit，一个 16 bit，以及一个 8 bit
    // 分别用于 <zlbytes><zltail><zllen> 和 <zlend>
    unsigned int bytes = ZIPLIST_HEADER_SIZE+1;
    unsigned char *zl = zmalloc(bytes);

    // 设置长度
    ZIPLIST_BYTES(zl) = intrev32ifbe(bytes);

    // 设置表尾偏移量
    ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(ZIPLIST_HEADER_SIZE);

    // 设置列表项数量
    ZIPLIST_LENGTH(zl) = 0;

    // 设置表尾标识
    zl[bytes-1] = ZIP_END;

    return zl;
}

/* Resize the ziplist. */
/*
 * 对 zl 进行空间重非配，并更新相关属性
 *
 * 复杂度：O(N)
 *
 * 返回值：更新后的 ziplist
 */
static unsigned char *ziplistResize(unsigned char *zl, unsigned int len) {
    // 重分配
    zl = zrealloc(zl,len);
    // 更新长度
    ZIPLIST_BYTES(zl) = intrev32ifbe(len);
    // 设置表尾
    zl[len-1] = ZIP_END;

    return zl;
}

/* When an entry is inserted, we need to set the prevlen field of the next
 * entry to equal the length of the inserted entry. It can occur that this
 * length cannot be encoded in 1 byte and the next entry needs to be grow
 * a bit larger to hold the 5-byte encoded prevlen. This can be done for free,
 * because this only happens when an entry is already being inserted (which
 * causes a realloc and memmove). However, encoding the prevlen may require
 * that this entry is grown as well. This effect may cascade throughout
 * the ziplist when there are consecutive entries with a size close to
 * ZIP_BIGLEN, so we need to check that the prevlen can be encoded in every
 * consecutive entry.
 *
 * Note that this effect can also happen in reverse, where the bytes required
 * to encode the prevlen field can shrink. This effect is deliberately ignored,
 * because it can cause a "flapping" effect where a chain prevlen fields is
 * first grown and then shrunk again after consecutive inserts. Rather, the
 * field is allowed to stay larger than necessary, because a large prevlen
 * field implies the ziplist is holding large entries anyway.
 *
 * The pointer "p" points to the first entry that does NOT need to be
 * updated, i.e. consecutive fields MAY need an update. */
/*
 * 当将一个新节点添加到某个节点之前的时候，
 * 如果原节点的 prevlen 不足以保存新节点的长度，
 * 那么就需要对原节点的空间进行扩展（从 1 字节扩展到 5 字节）。
 *
 * 但是，当对原节点进行扩展之后，原节点的下一个节点的 prevlen 可能出现空间不足，
 * 这种情况在多个连续节点的长度都接近 ZIP_BIGLEN 时可能发生。
 *
 * 这个函数就用于处理这种连续扩展动作。
 *
 * 因为节点的长度变小而引起的连续缩小也是可能出现的，
 * 不过，为了避免扩展-缩小-扩展-缩小这样的情况反复出现（flapping，抖动），
 * 我们不处理这种情况，而是任由 prevlen 比所需的长度更长
 *
 * 复杂度：O(N^2)
 *
 * 返回值：更新后的 ziplist
 */
static unsigned char *__ziplistCascadeUpdate(unsigned char *zl, unsigned char *p) {
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), rawlen, rawlensize;
    size_t offset, noffset, extra;
    unsigned char *np;
    zlentry cur, next;

    // 一直更新直到表尾
    while (p[0] != ZIP_END) {
        // 当前节点
        cur = zipEntry(p);
        // 当前节点的长度
        rawlen = cur.headersize + cur.len;
        // 编码当前节点的长度所需的空间大小
        rawlensize = zipPrevEncodeLength(NULL,rawlen);

        /* Abort if there is no next entry. */
        // 已经到达表尾，退出
        if (p[rawlen] == ZIP_END) break;
        // 取出下一节点
        next = zipEntry(p+rawlen);

        /* Abort when "prevlen" has not changed. */
        // 如果下一的 prevlen 等于当前节点的 rawlen
        // 那么说明编码大小无需改变，退出
        if (next.prevrawlen == rawlen) break;

        // 下一节点的长度编码空间不足，进行扩展
        if (next.prevrawlensize < rawlensize) {
            /* The "prevlen" field of "next" needs more bytes to hold
             * the raw length of "cur". */
            offset = p-zl;
            // 需要多添加的长度
            extra = rawlensize-next.prevrawlensize;
            // 重分配，复杂度为 O(N)
            zl = ziplistResize(zl,curlen+extra);
            p = zl+offset;

            /* Current pointer and offset for next element. */
            np = p+rawlen;
            noffset = np-zl;

            /* Update tail offset when next element is not the tail element. */
            if ((zl+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))) != np) {
                ZIPLIST_TAIL_OFFSET(zl) =
                    intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+extra);
            }

            /* Move the tail to the back. */
            // 为获得空间而进行数据移动，复杂度万恶 O(N)
            memmove(np+rawlensize,
                np+next.prevrawlensize,
                curlen-noffset-next.prevrawlensize-1);
            zipPrevEncodeLength(np,rawlen);

            /* Advance the cursor */
            p += rawlen;
            curlen += extra;
        } else {
            // 下一节点的长度编码空间有多余，不进行收缩
            // 只是将被编码的长度写入空间
            if (next.prevrawlensize > rawlensize) {
                /* This would result in shrinking, which we want to avoid.
                 * So, set "rawlen" in the available bytes. */
                zipPrevEncodeLengthForceLarge(p+rawlen,rawlen);
            } else {
                zipPrevEncodeLength(p+rawlen,rawlen);
            }

            // next.prevrawlensize == rawlensize
            /* Stop here, as the raw length of "next" has not changed. */
            break;
        }
    }
    return zl;
}

/* Delete "num" entries, starting at "p". Returns pointer to the ziplist. */
/*
 * 从指针 p 开始，删除 num 个节点
 *
 * 复杂度：O(N^2)
 *
 * 返回值：删除元素后的 ziplist
 */
static unsigned char *__ziplistDelete(unsigned char *zl, unsigned char *p, unsigned int num) {
    unsigned int i, totlen, deleted = 0;
    size_t offset;
    int nextdiff = 0;
    zlentry first, tail;

    // 首个节点
    first = zipEntry(p);
    // 累积起所有删除目标（节点）的编码长度
    // 并移动指针 p 
    // 复杂度 O(N)
    for (i = 0; p[0] != ZIP_END && i < num; i++) {
        p += zipRawEntryLength(p);
        deleted++;
    }

    // 被删除的节点的 byte 总和
    totlen = p-first.p;
    if (totlen > 0) {
        if (p[0] != ZIP_END) {
            /* Storing `prevrawlen` in this entry may increase or decrease the
             * number of bytes required compare to the current `prevrawlen`.
             * There always is room to store this, because it was previously
             * stored by an entry that is now being deleted. */
            // 更新最后一个被删除的节点之后的一个节点，
            // 将它的 prevlan 值设置为 first.prevrawlen ，
            // 也即是被删除的第一个节点的前一个节点的长度
            nextdiff = zipPrevLenByteDiff(p,first.prevrawlen);
            p -= nextdiff;
            zipPrevEncodeLength(p,first.prevrawlen);

            /* Update offset for tail */
            // 更新 ziplist 到表尾的偏移量
            ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))-totlen);

            /* When the tail contains more than one entry, we need to take
             * "nextdiff" in account as well. Otherwise, a change in the
             * size of prevlen doesn't have an effect on the *tail* offset. */
            // 跟新 ziplist 的偏移量，如果有需要的话，算上 nextdiff
            tail = zipEntry(p);
            if (p[tail.headersize+tail.len] != ZIP_END) {
                ZIPLIST_TAIL_OFFSET(zl) =
                   intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+nextdiff);
            }

            /* Move tail to the front of the ziplist */
            // 前移内存中的数据，覆盖原本的被删除数据
            // 复杂度 O(N)
            memmove(first.p,p,
                intrev32ifbe(ZIPLIST_BYTES(zl))-(p-zl)-1);
        } else {
            /* The entire tail was deleted. No need to move memory. */
            // 被删除的是尾节点，无须内存移动，直接更新偏移值就可以了
            ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe((first.p-zl)-first.prevrawlen);
        }

        /* Resize and update length */
        // 调整大小，并更新 ziplist 的长度
        // 复杂度 O(N)
        offset = first.p-zl;
        zl = ziplistResize(zl, intrev32ifbe(ZIPLIST_BYTES(zl))-totlen+nextdiff);
        ZIPLIST_INCR_LENGTH(zl,-deleted);
        p = zl+offset;

        /* When nextdiff != 0, the raw length of the next entry has changed, so
         * we need to cascade the update throughout the ziplist */
        // 层级更新
        if (nextdiff != 0)
            // 复杂度 O(N^2)
            zl = __ziplistCascadeUpdate(zl,p);
    }
    return zl;
}

/* Insert item at "p". */
/*
 * 添加保存给定元素 s 的新节点到地址 p
 *
 * 复杂度：O(N^2)
 *
 * 返回值：删除元素后的 ziplist
 */
static unsigned char *__ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)),
                    reqlen,
                    prevlen = 0;
    size_t offset;
    int nextdiff = 0;
    unsigned char encoding = 0;
    long long value = 123456789; /* initialized to avoid warning. Using a value
                                    that is easy to see if for some reason
                                    we use it uninitialized. */
    zlentry entry, tail;

    /* Find out prevlen for the entry that is inserted. */
    // 如果 p 之后不是没有节点（不是插入到末端）
    // 那么取出节点相关资料，以及 prevlen
    if (p[0] != ZIP_END) {
        entry = zipEntry(p);
        prevlen = entry.prevrawlen;
    } else {
        // 获取列表最后一个节点（表尾）的地址
        unsigned char *ptail = ZIPLIST_ENTRY_TAIL(zl);
        // 如果地址之后不是末端（也即是，列表至少有一个节点）
        if (ptail[0] != ZIP_END) {
            // 保存 ptail 指向的节点的空间长度
            prevlen = zipRawEntryLength(ptail);
        }
    }

    /* See if the entry can be encoded */
    // 查看能否将新值保存为整数
    // 如果可以的话返回 1 ，
    // 并将新值保存到 value ，编码形式保存到 encoding
    if (zipTryEncoding(s,slen,&value,&encoding)) {
        /* 'encoding' is set to the appropriate integer encoding */
        // s 可以保存为整数，那么继续计算保存它所需的空间
        reqlen = zipIntSize(encoding);
    } else {
        /* 'encoding' is untouched, however zipEncodeLength will use the
         * string length to figure out how to encode it. */
        // 不能保存为整数，直接使用字符串长度
        reqlen = slen;
    }
    /* We need space for both the length of the previous entry and
     * the length of the payload. */
    // 计算编码 prevlen 所需的长度
    reqlen += zipPrevEncodeLength(NULL,prevlen);
    // 计算编码 slen 所需的长度
    reqlen += zipEncodeLength(NULL,encoding,slen);

    /* When the insert position is not equal to the tail, we need to
     * make sure that the next entry can hold this entry's length in
     * its prevlen field. */
    // 如果添加的位置不是表尾，那么必须确定后继节点的 prevlen 空间
    // 足以保存新节点的编码长度
    // zipPrevLenByteDiff 的返回值有三种可能：
    // 1）新旧两个节点的编码长度相等，返回 0
    // 2）新节点编码长度 > 旧节点编码长度，返回 5 - 1 = 4
    // 3）旧节点编码长度 > 新编码节点长度，返回 1 - 5 = -4
    nextdiff = (p[0] != ZIP_END) ? zipPrevLenByteDiff(p,reqlen) : 0;

    /* Store offset because a realloc may change the address of zl. */
    // 保存偏移量，因为重分配空间有可能改变 zl 的内存地址
    offset = p-zl;
    // 重分配空间，并更新长度属性和表尾
    // 新空间长度 = 现有长度 + 新节点所需长度 + 编码新节点长度所需的长度差
    // O(N)
    zl = ziplistResize(zl,curlen+reqlen+nextdiff);
    // 更新 p 的指针
    p = zl+offset;

    /* Apply memory move when necessary and update tail offset. */
    // 如果新节点不是添加到列表末端，那么它后面就有其他节点
    // 因此，我们需要移动这部分节点
    if (p[0] != ZIP_END) {
        /* Subtract one because of the ZIP_END bytes */
        // 向右移动移原有数据，为新节点让出空间
        // O(N)
        memmove(p+reqlen,p-nextdiff,curlen-offset-1+nextdiff);

        /* Encode this entry's raw length in the next entry. */
        // 将本节点的长度编码至下一节点
        zipPrevEncodeLength(p+reqlen,reqlen);

        /* Update offset for tail */
        // 更新 ziplist 的表尾偏移量
        ZIPLIST_TAIL_OFFSET(zl) =
            intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+reqlen);

        /* When the tail contains more than one entry, we need to take
         * "nextdiff" in account as well. Otherwise, a change in the
         * size of prevlen doesn't have an effect on the *tail* offset. */
        // 有需要的话，将 nextdiff 也加上到 zltail 上
        tail = zipEntry(p+reqlen);
        if (p[reqlen+tail.headersize+tail.len] != ZIP_END) {
            ZIPLIST_TAIL_OFFSET(zl) =
                intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+nextdiff);
        }
    } else {
        /* This element will be the new tail. */
        // 更新 ziplist 的 zltail 属性，现在新添加节点为表尾节点
        ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(p-zl);
    }

    /* When nextdiff != 0, the raw length of the next entry has changed, so
     * we need to cascade the update throughout the ziplist */
    if (nextdiff != 0) {
        offset = p-zl;
        // O(N^2)
        zl = __ziplistCascadeUpdate(zl,p+reqlen);
        p = zl+offset;
    }

    /* Write the entry */
    // 写入数据到节点

    // 编码上一节点的长度，并向后移动指针
    p += zipPrevEncodeLength(p,prevlen);
    // 编码本节点的长度和类型，并向后移动指针
    p += zipEncodeLength(p,encoding,slen);
    // 写入内容到节点
    if (ZIP_IS_STR(encoding)) {
        memcpy(p,s,slen);
    } else {
        zipSaveInteger(p,value,encoding);
    }
    // 更新节点数量
    ZIPLIST_INCR_LENGTH(zl,1);

    return zl;
}

/*
 * 将新元素插入为列表的表头节点或者表尾节点
 *
 * 复杂度：O(N^2)
 *
 * 返回值：添加操作完成后的 ziplist
 */
unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where) {
    unsigned char *p;
    p = (where == ZIPLIST_HEAD) ? ZIPLIST_ENTRY_HEAD(zl) : ZIPLIST_ENTRY_END(zl);
    return __ziplistInsert(zl,p,s,slen);
}

/* Returns an offset to use for iterating with ziplistNext. When the given
 * index is negative, the list is traversed back to front. When the list
 * doesn't contain an element at the provided index, NULL is returned. */
/*
 * 返回指向当前迭代节点的指针，用于 ziplistNext 进行迭代
 * 当偏移值是负数时，表示迭代是从表尾到表头进行的。
 * 当元素被遍历完时，返回 NULL
 *
 * 复杂度：O(1)
 *
 * 返回值：指向节点的指针
 */
unsigned char *ziplistIndex(unsigned char *zl, int index) {

    unsigned char *p;

    zlentry entry;

    // 向前遍历
    if (index < 0) {
        index = (-index)-1;
        p = ZIPLIST_ENTRY_TAIL(zl);
        // 如果 ziplist 不为空。。。
        if (p[0] != ZIP_END) {
            // 那么根据 entry.prevrawlen ，进行向前迭代
            entry = zipEntry(p);
            while (entry.prevrawlen > 0 && index--) {
                // 后退地址
                p -= entry.prevrawlen;
                entry = zipEntry(p);
            }
        }
    // 向后遍历
    } else {
        p = ZIPLIST_ENTRY_HEAD(zl);
        // 根据 entry.prevrawlen 向后进行迭代
        while (p[0] != ZIP_END && index--) {
            p += zipRawEntryLength(p);
        }
    }

    return (p[0] == ZIP_END || index > 0) ? NULL : p;
}

/* Return pointer to next entry in ziplist.
 *
 * zl is the pointer to the ziplist
 * p is the pointer to the current element
 *
 * The element after 'p' is returned, otherwise NULL if we are at the end. */
/*
 * 返回指向 p 的下一个节点的指针，
 * 如果 p 已经到达表尾，那么返回 NULL 。
 *
 * 复杂度：O(1)
 */
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p) {
    ((void) zl);

    /* "p" could be equal to ZIP_END, caused by ziplistDelete,
     * and we should return NULL. Otherwise, we should return NULL
     * when the *next* element is ZIP_END (there is no next entry). */
    if (p[0] == ZIP_END) {
        return NULL;
    }

    // 指向下一节点，O(1)
    p += zipRawEntryLength(p);
    if (p[0] == ZIP_END) {
        return NULL;
    }

    return p;
}

/* Return pointer to previous entry in ziplist. */
/*
 * 返回 p 的前一个节点
 *
 * 复杂度：O(1)
 */
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p) {
    zlentry entry;

    /* Iterating backwards from ZIP_END should return the tail. When "p" is
     * equal to the first element of the list, we're already at the head,
     * and should return NULL. */
    // 这是表尾
    if (p[0] == ZIP_END) {
        p = ZIPLIST_ENTRY_TAIL(zl);
        return (p[0] == ZIP_END) ? NULL : p;
    // 到达表头，停止
    } else if (p == ZIPLIST_ENTRY_HEAD(zl)) {
        return NULL;
    } else {
        entry = zipEntry(p);
        assert(entry.prevrawlen > 0);
        return p-entry.prevrawlen;
    }
}

/* Get entry pointer to by 'p' and store in either 'e' or 'v' depending
 * on the encoding of the entry. 'e' is always set to NULL to be able
 * to find out whether the string pointer or the integer value was set.
 * Return 0 if 'p' points to the end of the zipmap, 1 otherwise. */
/*
 * 获取 p 所指向的节点，并将相关属性保存至指针
 *
 * 如果节点保存的是字符串值，那么将 sstr 指针指向它，
 * slen 设在为字符串的长度。
 *
 * 如果节点保存的是数字值，那么用 sval 保存它。
 *
 * p 为表尾时返回 0 ，否则返回 1 。
 *
 * 复杂度：O(1)
 */
unsigned int ziplistGet(unsigned char *p, unsigned char **sstr, unsigned int *slen, long long *sval) {
    zlentry entry;
    // 表尾
    if (p == NULL || p[0] == ZIP_END) return 0;
    if (sstr) *sstr = NULL;

    // 获取节点
    entry = zipEntry(p);
    // 字符串
    if (ZIP_IS_STR(entry.encoding)) {
        if (sstr) {
            *slen = entry.len;
            *sstr = p+entry.headersize;
        }
    // 数字值
    } else {
        if (sval) {
            *sval = zipLoadInteger(p+entry.headersize,entry.encoding);
        }
    }
    return 1;
}

/* Insert an entry at "p". */
/*
 * 将节点插入到 p 之后
 *
 * 复杂度：O(N^2)
 *
 * 返回值：添加完成后的 ziplist
 */
unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {
    return __ziplistInsert(zl,p,s,slen);
}

/* Delete a single entry from the ziplist, pointed to by *p.
 * Also update *p in place, to be able to iterate over the
 * ziplist, while deleting entries. */
/*
 * 删除 p 所指向的节点，
 * 并原地更新 p 指针，让它指向被删除节点的后一个节点，
 * 使得可以迭代地进行删除
 * 
 * 复杂度：O(N^2)
 * 
 * 返回值：删除完成后的 ziplist
 */
unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p) {
    size_t offset = *p-zl;
    zl = __ziplistDelete(zl,*p,1);

    /* Store pointer to current element in p, because ziplistDelete will
     * do a realloc which might result in a different "zl"-pointer.
     * When the delete direction is back to front, we might delete the last
     * entry and end up with "p" pointing to ZIP_END, so check this. */
    *p = zl+offset;
    return zl;
}

/* Delete a range of entries from the ziplist. */
/*
 * 定位到索引 index ，并删除这之后的 num 个元素
 *
 * 复杂度：O(N^2)
 * 
 * 返回值：删除完成后的 ziplist
 */
unsigned char *ziplistDeleteRange(unsigned char *zl, unsigned int index, unsigned int num) {
    unsigned char *p = ziplistIndex(zl,index);
    return (p == NULL) ? zl : __ziplistDelete(zl,p,num);
}

/* Compare entry pointer to by 'p' with 'entry'. Return 1 if equal. */
/*
 * 将 p 所指向的节点的属性和 sstr 以及 slen 进行对比，
 * 如果相等则返回 1 。
 *
 * 复杂度：O(N)
 */
unsigned int ziplistCompare(unsigned char *p, unsigned char *sstr, unsigned int slen) {
    zlentry entry;
    unsigned char sencoding;
    long long zval, sval;

    // p 是表尾？
    if (p[0] == ZIP_END) return 0;

    // 获取节点属性
    entry = zipEntry(p);
    // 对比字符串
    if (ZIP_IS_STR(entry.encoding)) {
        /* Raw compare */
        if (entry.len == slen) {
            // O(N)
            return memcmp(p+entry.headersize,sstr,slen) == 0;
        } else {
            return 0;
        }
    // 对比整数
    } else {
        /* Try to compare encoded values. Don't compare encoding because
         * different implementations may encoded integers differently. */
        if (zipTryEncoding(sstr,slen,&sval,&sencoding)) {
          zval = zipLoadInteger(p+entry.headersize,entry.encoding);
          return zval == sval;
        }
    }

    return 0;
}

/* Find pointer to the entry equal to the specified entry. Skip 'skip' entries
 * between every comparison. Returns NULL when the field could not be found. */
/*
 * 根据给定的 vstr 和 vlen ，查找和属性和它们相等的节点
 * 在每次比对之间，跳过 skip 个节点。
 *
 * 复杂度：O(N)
 * 返回值：
 *  查找失败返回 NULL 。
 *  查找成功返回指向目标节点的指针
 */
unsigned char *ziplistFind(unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip) {
    int skipcnt = 0;
    unsigned char vencoding = 0;
    long long vll = 0;

    // 遍历整个列表
    while (p[0] != ZIP_END) {
        unsigned int prevlensize, encoding, lensize, len;
        unsigned char *q;

        // 编码前一个节点的长度所需的空间
        ZIP_DECODE_PREVLENSIZE(p, prevlensize);
        // 当前节点的长度
        ZIP_DECODE_LENGTH(p + prevlensize, encoding, lensize, len);
        // 保存下一个节点的地址
        q = p + prevlensize + lensize;

        if (skipcnt == 0) {
            /* Compare current entry with specified entry */
            // 对比字符串
            if (ZIP_IS_STR(encoding)) {
                if (len == vlen && memcmp(q, vstr, vlen) == 0) {
                    return p;
                }
            // 对比整数
            } else {
                /* Find out if the searched field can be encoded. Note that
                 * we do it only the first time, once done vencoding is set
                 * to non-zero and vll is set to the integer value. */
                // 对传入值进行 decode
                if (vencoding == 0) {
                    if (!zipTryEncoding(vstr, vlen, &vll, &vencoding)) {
                        /* If the entry can't be encoded we set it to
                         * UCHAR_MAX so that we don't retry again the next
                         * time. */
                        vencoding = UCHAR_MAX;
                    }
                    /* Must be non-zero by now */
                    assert(vencoding);
                }

                /* Compare current entry with specified entry, do it only
                 * if vencoding != UCHAR_MAX because if there is no encoding
                 * possible for the field it can't be a valid integer. */
                if (vencoding != UCHAR_MAX) {
                    // 对比
                    long long ll = zipLoadInteger(q, encoding);
                    if (ll == vll) {
                        return p;
                    }
                }
            }

            /* Reset skip count */
            skipcnt = skip;
        } else {
            /* Skip entry */
            skipcnt--;
        }

        /* Move to next entry */
        p = q + len;
    }

    return NULL;
}

/* Return length of ziplist. */
/*
 * 返回 ziplist 的长度
 *
 * 复杂度：O(N)
 */
unsigned int ziplistLen(unsigned char *zl) {
    unsigned int len = 0;
    // 节点的数量 < UINT16_MAX
    if (intrev16ifbe(ZIPLIST_LENGTH(zl)) < UINT16_MAX) {
        // 长度保存在一个 uint16 整数中
        len = intrev16ifbe(ZIPLIST_LENGTH(zl));
    // 节点的数量 >= UINT16_MAX
    } else {
        // 遍历整个 ziplist ，计算长度
        unsigned char *p = zl+ZIPLIST_HEADER_SIZE;
        while (*p != ZIP_END) {
            p += zipRawEntryLength(p);
            len++;
        }

        /* Re-store length if small enough */
        if (len < UINT16_MAX) ZIPLIST_LENGTH(zl) = intrev16ifbe(len);
    }
    return len;
}

/* Return ziplist blob size in bytes. */
/*
 * 返回整个 ziplist 的空间大小
 * 
 * 复杂度：O(1)
 */
size_t ziplistBlobLen(unsigned char *zl) {
    return intrev32ifbe(ZIPLIST_BYTES(zl));
}

void ziplistRepr(unsigned char *zl) {
    unsigned char *p;
    int index = 0;
    zlentry entry;

    printf(
        "{total bytes %d} "
        "{length %u}\n"
        "{tail offset %u}\n",
        intrev32ifbe(ZIPLIST_BYTES(zl)),
        intrev16ifbe(ZIPLIST_LENGTH(zl)),
        intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)));
    p = ZIPLIST_ENTRY_HEAD(zl);
    while(*p != ZIP_END) {
        entry = zipEntry(p);
        printf(
            "{"
                "addr 0x%08lx, "
                "index %2d, "
                "offset %5ld, "
                "rl: %5u, "
                "hs %2u, "
                "pl: %5u, "
                "pls: %2u, "
                "payload %5u"
            "} ",
            (long unsigned)p,
            index,
            (unsigned long) (p-zl),
            entry.headersize+entry.len,
            entry.headersize,
            entry.prevrawlen,
            entry.prevrawlensize,
            entry.len);
        p += entry.headersize;
        if (ZIP_IS_STR(entry.encoding)) {
            if (entry.len > 40) {
                if (fwrite(p,40,1,stdout) == 0) perror("fwrite");
                printf("...");
            } else {
                if (entry.len &&
                    fwrite(p,entry.len,1,stdout) == 0) perror("fwrite");
            }
        } else {
            printf("%lld", (long long) zipLoadInteger(p,entry.encoding));
        }
        printf("\n");
        p += entry.len;
        index++;
    }
    printf("{end}\n\n");
}

#ifdef ZIPLIST_TEST_MAIN
#include <sys/time.h>
#include "adlist.h"
#include "sds.h"

#define debug(f, ...) { if (DEBUG) printf(f, __VA_ARGS__); }

unsigned char *createList() {
    unsigned char *zl = ziplistNew();
    zl = ziplistPush(zl, (unsigned char*)"foo", 3, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char*)"quux", 4, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char*)"hello", 5, ZIPLIST_HEAD);
    zl = ziplistPush(zl, (unsigned char*)"1024", 4, ZIPLIST_TAIL);
    return zl;
}

unsigned char *createIntList() {
    unsigned char *zl = ziplistNew();
    char buf[32];

    sprintf(buf, "100");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "128000");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "-100");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_HEAD);
    sprintf(buf, "4294967296");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_HEAD);
    sprintf(buf, "non integer");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "much much longer non integer");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    return zl;
}

long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

void stress(int pos, int num, int maxsize, int dnum) {
    int i,j,k;
    unsigned char *zl;
    char posstr[2][5] = { "HEAD", "TAIL" };
    long long start;
    for (i = 0; i < maxsize; i+=dnum) {
        zl = ziplistNew();
        for (j = 0; j < i; j++) {
            zl = ziplistPush(zl,(unsigned char*)"quux",4,ZIPLIST_TAIL);
        }

        /* Do num times a push+pop from pos */
        start = usec();
        for (k = 0; k < num; k++) {
            zl = ziplistPush(zl,(unsigned char*)"quux",4,pos);
            zl = ziplistDeleteRange(zl,0,1);
        }
        printf("List size: %8d, bytes: %8d, %dx push+pop (%s): %6lld usec\n",
            i,intrev32ifbe(ZIPLIST_BYTES(zl)),num,posstr[pos],usec()-start);
        zfree(zl);
    }
}

void pop(unsigned char *zl, int where) {
    unsigned char *p, *vstr;
    unsigned int vlen;
    long long vlong;

    p = ziplistIndex(zl,where == ZIPLIST_HEAD ? 0 : -1);
    if (ziplistGet(p,&vstr,&vlen,&vlong)) {
        if (where == ZIPLIST_HEAD)
            printf("Pop head: ");
        else
            printf("Pop tail: ");

        if (vstr)
            if (vlen && fwrite(vstr,vlen,1,stdout) == 0) perror("fwrite");
        else
            printf("%lld", vlong);

        printf("\n");
        ziplistDeleteRange(zl,-1,1);
    } else {
        printf("ERROR: Could not pop\n");
        exit(1);
    }
}

int randstring(char *target, unsigned int min, unsigned int max) {
    int p, len = min+rand()%(max-min+1);
    int minval, maxval;
    switch(rand() % 3) {
    case 0:
        minval = 0;
        maxval = 255;
    break;
    case 1:
        minval = 48;
        maxval = 122;
    break;
    case 2:
        minval = 48;
        maxval = 52;
    break;
    default:
        assert(NULL);
    }

    while(p < len)
        target[p++] = minval+rand()%(maxval-minval+1);
    return len;
}

void verify(unsigned char *zl, zlentry *e) {
    int i;
    int len = ziplistLen(zl);
    zlentry _e;

    for (i = 0; i < len; i++) {
        memset(&e[i], 0, sizeof(zlentry));
        e[i] = zipEntry(ziplistIndex(zl, i));

        memset(&_e, 0, sizeof(zlentry));
        _e = zipEntry(ziplistIndex(zl, -len+i));

        assert(memcmp(&e[i], &_e, sizeof(zlentry)) == 0);
    }
}

int main(int argc, char **argv) {
    unsigned char *zl, *p;
    unsigned char *entry;
    unsigned int elen;
    long long value;

    /* If an argument is given, use it as the random seed. */
    if (argc == 2)
        srand(atoi(argv[1]));

    zl = createIntList();
    ziplistRepr(zl);

    zl = createList();
    ziplistRepr(zl);

    pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    pop(zl,ZIPLIST_HEAD);
    ziplistRepr(zl);

    pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    printf("Get element at index 3:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 3);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index 3\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
    }

    printf("Get element at index 4 (out of range):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 4);
        if (p == NULL) {
            printf("No entry\n");
        } else {
            printf("ERROR: Out of range index should return NULL, returned offset: %ld\n", p-zl);
            return 1;
        }
        printf("\n");
    }

    printf("Get element at index -1 (last element):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index -1\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
    }

    printf("Get element at index -4 (first element):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -4);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index -4\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
    }

    printf("Get element at index -5 (reverse out of range):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -5);
        if (p == NULL) {
            printf("No entry\n");
        } else {
            printf("ERROR: Out of range index should return NULL, returned offset: %ld\n", p-zl);
            return 1;
        }
        printf("\n");
    }

    printf("Iterate list from 0 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 0);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
    }

    printf("Iterate list from 1 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
    }

    printf("Iterate list from 2 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 2);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
    }

    printf("Iterate starting out of range:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 4);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("No entry\n");
        } else {
            printf("ERROR\n");
        }
        printf("\n");
    }

    printf("Iterate from back to front:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistPrev(zl,p);
            printf("\n");
        }
        printf("\n");
    }

    printf("Iterate from back to front, deleting all items:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            zl = ziplistDelete(zl,&p);
            p = ziplistPrev(zl,p);
            printf("\n");
        }
        printf("\n");
    }

    printf("Delete inclusive range 0,0:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 0, 1);
        ziplistRepr(zl);
    }

    printf("Delete inclusive range 0,1:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 0, 2);
        ziplistRepr(zl);
    }

    printf("Delete inclusive range 1,2:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 1, 2);
        ziplistRepr(zl);
    }

    printf("Delete with start index out of range:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 5, 1);
        ziplistRepr(zl);
    }

    printf("Delete with num overflow:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 1, 5);
        ziplistRepr(zl);
    }

    printf("Delete foo while iterating:\n");
    {
        zl = createList();
        p = ziplistIndex(zl,0);
        while (ziplistGet(p,&entry,&elen,&value)) {
            if (entry && strncmp("foo",(char*)entry,elen) == 0) {
                printf("Delete foo\n");
                zl = ziplistDelete(zl,&p);
            } else {
                printf("Entry: ");
                if (entry) {
                    if (elen && fwrite(entry,elen,1,stdout) == 0)
                        perror("fwrite");
                } else {
                    printf("%lld",value);
                }
                p = ziplistNext(zl,p);
                printf("\n");
            }
        }
        printf("\n");
        ziplistRepr(zl);
    }

    printf("Regression test for >255 byte strings:\n");
    {
        char v1[257],v2[257];
        memset(v1,'x',256);
        memset(v2,'y',256);
        zl = ziplistNew();
        zl = ziplistPush(zl,(unsigned char*)v1,strlen(v1),ZIPLIST_TAIL);
        zl = ziplistPush(zl,(unsigned char*)v2,strlen(v2),ZIPLIST_TAIL);

        /* Pop values again and compare their value. */
        p = ziplistIndex(zl,0);
        assert(ziplistGet(p,&entry,&elen,&value));
        assert(strncmp(v1,(char*)entry,elen) == 0);
        p = ziplistIndex(zl,1);
        assert(ziplistGet(p,&entry,&elen,&value));
        assert(strncmp(v2,(char*)entry,elen) == 0);
        printf("SUCCESS\n\n");
    }

    printf("Regression test deleting next to last entries:\n");
    {
        char v[3][257];
        zlentry e[3];
        int i;

        for (i = 0; i < (sizeof(v)/sizeof(v[0])); i++) {
            memset(v[i], 'a' + i, sizeof(v[0]));
        }

        v[0][256] = '\0';
        v[1][  1] = '\0';
        v[2][256] = '\0';

        zl = ziplistNew();
        for (i = 0; i < (sizeof(v)/sizeof(v[0])); i++) {
            zl = ziplistPush(zl, (unsigned char *) v[i], strlen(v[i]), ZIPLIST_TAIL);
        }

        verify(zl, e);

        assert(e[0].prevrawlensize == 1);
        assert(e[1].prevrawlensize == 5);
        assert(e[2].prevrawlensize == 1);

        /* Deleting entry 1 will increase `prevrawlensize` for entry 2 */
        unsigned char *p = e[1].p;
        zl = ziplistDelete(zl, &p);

        verify(zl, e);

        assert(e[0].prevrawlensize == 1);
        assert(e[1].prevrawlensize == 5);

        printf("SUCCESS\n\n");
    }

    printf("Create long list and check indices:\n");
    {
        zl = ziplistNew();
        char buf[32];
        int i,len;
        for (i = 0; i < 1000; i++) {
            len = sprintf(buf,"%d",i);
            zl = ziplistPush(zl,(unsigned char*)buf,len,ZIPLIST_TAIL);
        }
        for (i = 0; i < 1000; i++) {
            p = ziplistIndex(zl,i);
            assert(ziplistGet(p,NULL,NULL,&value));
            assert(i == value);

            p = ziplistIndex(zl,-i-1);
            assert(ziplistGet(p,NULL,NULL,&value));
            assert(999-i == value);
        }
        printf("SUCCESS\n\n");
    }

    printf("Compare strings with ziplist entries:\n");
    {
        zl = createList();
        p = ziplistIndex(zl,0);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl,3);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }
        printf("SUCCESS\n\n");
    }

    printf("Stress with random payloads of different encoding:\n");
    {
        int i,j,len,where;
        unsigned char *p;
        char buf[1024];
        int buflen;
        list *ref;
        listNode *refnode;

        /* Hold temp vars from ziplist */
        unsigned char *sstr;
        unsigned int slen;
        long long sval;

        for (i = 0; i < 20000; i++) {
            zl = ziplistNew();
            ref = listCreate();
            listSetFreeMethod(ref,sdsfree);
            len = rand() % 256;

            /* Create lists */
            for (j = 0; j < len; j++) {
                where = (rand() & 1) ? ZIPLIST_HEAD : ZIPLIST_TAIL;
                if (rand() % 2) {
                    buflen = randstring(buf,1,sizeof(buf)-1);
                } else {
                    switch(rand() % 3) {
                    case 0:
                        buflen = sprintf(buf,"%lld",(0LL + rand()) >> 20);
                        break;
                    case 1:
                        buflen = sprintf(buf,"%lld",(0LL + rand()));
                        break;
                    case 2:
                        buflen = sprintf(buf,"%lld",(0LL + rand()) << 20);
                        break;
                    default:
                        assert(NULL);
                    }
                }

                /* Add to ziplist */
                zl = ziplistPush(zl, (unsigned char*)buf, buflen, where);

                /* Add to reference list */
                if (where == ZIPLIST_HEAD) {
                    listAddNodeHead(ref,sdsnewlen(buf, buflen));
                } else if (where == ZIPLIST_TAIL) {
                    listAddNodeTail(ref,sdsnewlen(buf, buflen));
                } else {
                    assert(NULL);
                }
            }

            assert(listLength(ref) == ziplistLen(zl));
            for (j = 0; j < len; j++) {
                /* Naive way to get elements, but similar to the stresser
                 * executed from the Tcl test suite. */
                p = ziplistIndex(zl,j);
                refnode = listIndex(ref,j);

                assert(ziplistGet(p,&sstr,&slen,&sval));
                if (sstr == NULL) {
                    buflen = sprintf(buf,"%lld",sval);
                } else {
                    buflen = slen;
                    memcpy(buf,sstr,buflen);
                    buf[buflen] = '\0';
                }
                assert(memcmp(buf,listNodeValue(refnode),buflen) == 0);
            }
            zfree(zl);
            listRelease(ref);
        }
        printf("SUCCESS\n\n");
    }

    printf("Stress with variable ziplist size:\n");
    {
        stress(ZIPLIST_HEAD,100000,16384,256);
        stress(ZIPLIST_TAIL,100000,16384,256);
    }

    return 0;
}

#endif
