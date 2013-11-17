/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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

/*-----------------------------------------------------------------------------
 * Sorted set API
 *----------------------------------------------------------------------------*/

/* ZSETs are ordered sets using two data structures to hold the same elements
 * in order to get O(log(N)) INSERT and REMOVE operations into a sorted
 * data structure.
 *
 * Zset 为有序集合，它使用两种数据结构保存同一个对象，
 * 使得可以在有序集合内用 O(log(N)) 复杂度内进行添加和删除操作。
 *
 * The elements are added to an hash table mapping Redis objects to scores.
 * At the same time the elements are added to a skip list mapping scores
 * to Redis objects (so objects are sorted by scores in this "view"). 
 *
 * 哈希表以 Redis 对象为键， score 为值。
 * Skiplist 里同样保持着 Redis 对象和 score 值的映射。
 */

/* This skiplist implementation is almost a C translation of the original
 * algorithm described by William Pugh in "Skip Lists: A Probabilistic
 * Alternative to Balanced Trees", modified in three ways:
 *
 * 这里的 skiplist 实现和 William Pugh 在 "Skip Lists: A Probabilistic
 * Alternative to Balanced Trees" 里描述的差不多，只有三个地方进行了修改：
 *
 * a) this implementation allows for repeated scores.
 *    这个实现允许重复值
 * b) the comparison is not just by key (our 'score') but by satellite data.
 *    不仅对 score 进行比对，还需要对 Redis 对象里的信息进行比对
 * c) there is a back pointer, so it's a doubly linked list with the back
 * pointers being only at "level 1". This allows to traverse the list
 * from tail to head, useful for ZREVRANGE. 
 *    每个节点都带有一个前驱指针，用于从表尾向表头迭代。
 */

#include "redis.h"
#include <math.h>

/*
 * 创建并返回一个跳跃表节点
 *
 * T = O(1)
 */
zskiplistNode *zslCreateNode(int level, double score, robj *obj) {
    // 分配层
    zskiplistNode *zn = zmalloc(sizeof(*zn)+level*sizeof(struct zskiplistLevel));
    // 点数
    zn->score = score;
    // 对象
    zn->obj = obj;

    return zn;
}

/*
 * 创建一个跳跃表
 *
 * T = O(1)
 */
zskiplist *zslCreate(void) {
    int j;
    zskiplist *zsl;

    zsl = zmalloc(sizeof(*zsl));

    zsl->level = 1;
    zsl->length = 0;

    // 初始化头节点， O(1)
    zsl->header = zslCreateNode(ZSKIPLIST_MAXLEVEL,0,NULL);
    // 初始化层指针，O(1)
    for (j = 0; j < ZSKIPLIST_MAXLEVEL; j++) {
        zsl->header->level[j].forward = NULL;
        zsl->header->level[j].span = 0;
    }
    zsl->header->backward = NULL;

    zsl->tail = NULL;

    return zsl;
}

/*
 * 释放跳跃表节点
 *
 * T = O(1)
 */
void zslFreeNode(zskiplistNode *node) {
    decrRefCount(node->obj);
    zfree(node);
}

/*
 * 释放整个跳跃表
 *
 * T = O(N)
 */
void zslFree(zskiplist *zsl) {
     
    zskiplistNode *node = zsl->header->level[0].forward,
                  *next;

    zfree(zsl->header);

    // 遍历删除, O(N)
    while(node) {
        next = node->level[0].forward;
        zslFreeNode(node);
        node = next;
    }

    zfree(zsl);
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. 
 *
 * 返回一个介于 1 和 ZSKIPLIST_MAXLEVEL 之间的随机值，作为节点的层数。
 *
 * 根据幂次定律(power law)，数值越大，函数生成它的几率就越小
 *
 * T = O(N)
 */
int zslRandomLevel(void) {
    int level = 1;
    while ((random()&0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
        level += 1;
    return (level<ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}

/*
 * 将包含给定 score 的对象 obj 添加到 skiplist 里
 *
 * T_worst = O(N), T_average = O(log N)
 */
zskiplistNode *zslInsert(zskiplist *zsl, double score, robj *obj) {

    // 记录寻找元素过程中，每层能到达的最右节点
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;

    // 记录寻找元素过程中，每层所跨越的节点数
    unsigned int rank[ZSKIPLIST_MAXLEVEL];

    int i, level;

    redisAssert(!isnan(score));
    x = zsl->header;
    // 记录沿途访问的节点，并计数 span 等属性
    // 平均 O(log N) ，最坏 O(N)
    for (i = zsl->level-1; i >= 0; i--) {
        /* store rank that is crossed to reach the insert position */
        rank[i] = i == (zsl->level-1) ? 0 : rank[i+1];

        // 右节点不为空
        while (x->level[i].forward &&                   
            // 右节点的 score 比给定 score 小
            (x->level[i].forward->score < score ||      
                // 右节点的 score 相同，但节点的 member 比输入 member 要小
                (x->level[i].forward->score == score && 
                compareStringObjects(x->level[i].forward->obj,obj) < 0))) {
            // 记录跨越了多少个元素
            rank[i] += x->level[i].span;
            // 继续向右前进
            x = x->level[i].forward;
        }
        // 保存访问节点
        update[i] = x;
    }

    /* we assume the key is not already inside, since we allow duplicated
     * scores, and the re-insertion of score and redis object should never
     * happpen since the caller of zslInsert() should test in the hash table
     * if the element is already inside or not. */
    // 因为这个函数不可能处理两个元素的 member 和 score 都相同的情况，
    // 所以直接创建新节点，不用检查存在性

    // 计算新的随机层数
    level = zslRandomLevel();
    // 如果 level 比当前 skiplist 的最大层数还要大
    // 那么更新 zsl->level 参数
    // 并且初始化 update 和 rank 参数在相应的层的数据
    if (level > zsl->level) {
        for (i = zsl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = zsl->header;
            update[i]->level[i].span = zsl->length;
        }
        zsl->level = level;
    }

    // 创建新节点
    x = zslCreateNode(level,score,obj);
    // 根据 update 和 rank 两个数组的资料，初始化新节点
    // 并设置相应的指针
    // O(N)
    for (i = 0; i < level; i++) {
        // 设置指针
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;

        /* update span covered by update[i] as x is inserted here */
        // 设置 span
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    /* increment span for untouched levels */
    // 更新沿途访问节点的 span 值
    for (i = level; i < zsl->level; i++) {
        update[i]->level[i].span++;
    }

    // 设置后退指针
    x->backward = (update[0] == zsl->header) ? NULL : update[0];
    // 设置 x 的前进指针
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else
        // 这个是新的表尾节点
        zsl->tail = x;

    // 更新跳跃表节点数量
    zsl->length++;

    return x;
}

/* Internal function used by zslDelete, zslDeleteByScore and zslDeleteByRank */
/*
 * 节点删除函数
 *
 * T = O(N)
 */
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update) {
    int i;

    // 修改相应的指针和 span , O(N)
    for (i = 0; i < zsl->level; i++) {
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            update[i]->level[i].span -= 1;
        }
    }

    // 处理表头和表尾节点
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else {
        zsl->tail = x->backward;
    }

    // 收缩 level 的值, O(N)
    while(zsl->level > 1 && zsl->header->level[zsl->level-1].forward == NULL)
        zsl->level--;

    zsl->length--;
}

/* Delete an element with matching score/object from the skiplist. */
/*
 * 从 skiplist 中删除和给定 obj 以及给定 score 匹配的元素
 *
 * T_worst = O(N), T_average = O(log N)
 */
int zslDelete(zskiplist *zsl, double score, robj *obj) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    x = zsl->header;
    // 遍历所有层，记录删除节点后需要被修改的节点到 update 数组
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                compareStringObjects(x->level[i].forward->obj,obj) < 0)))
            x = x->level[i].forward;
        update[i] = x;
    }
    /* We may have multiple elements with the same score, what we need
     * is to find the element with both the right score and object. */
    // 因为多个不同的 member 可能有相同的 score 
    // 所以要确保 x 的 member 和 score 都匹配时，才进行删除
    x = x->level[0].forward;
    if (x && score == x->score && equalStringObjects(x->obj,obj)) {
        zslDeleteNode(zsl, x, update);
        zslFreeNode(x);
        return 1;
    } else {
        return 0; /* not found */
    }
    return 0; /* not found */
}

/*
 * 检查 value 是否属于 spec 指定的范围内
 *
 * T = O(1)
 */
static int zslValueGteMin(double value, zrangespec *spec) {
    return spec->minex ? (value > spec->min) : (value >= spec->min);
}

/*
 * 检查 value 是否属于 spec 指定的范围内
 *
 * T = O(1)
 */
static int zslValueLteMax(double value, zrangespec *spec) {
    return spec->maxex ? (value < spec->max) : (value <= spec->max);
}

/* Returns if there is a part of the zset is in range. */
/*
 * 检查 zset 中的元素是否在给定范围之内
 *
 * T = O(1)
 */
int zslIsInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;

    // 如果 zset 的最大节点的 score 比范围的最小值要小
    // 那么 zset 不在范围之内
    x = zsl->tail;
    if (x == NULL || !zslValueGteMin(x->score,range))
        return 0;

    // 如果 zset 的最小节点的 score 比范围的最大值要大
    // 那么 zset 不在范围之内
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslValueLteMax(x->score,range))
        return 0;

    // 在范围内
    return 1;
}

/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
/*
 * 找到跳跃表中第一个符合给定范围的元素
 *
 * T_worst = O(N) , T_average = O(log N)
 */
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!zslIsInRange(zsl,&range)) return NULL;

    // 找到第一个 score 值大于给定范围最小值的节点
    // O(N)
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward &&
            !zslValueGteMin(x->level[i].forward->score,&range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    x = x->level[0].forward;
    redisAssert(x != NULL);

    /* Check if score <= max. */
    // O(1)
    if (!zslValueLteMax(x->score,&range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
/*
 * 找到跳跃表中最后一个符合给定范围的元素
 *
 * T_worst = O(N) , T_average = O(log N)
 */
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!zslIsInRange(zsl,&range)) return NULL;

    // O(N)
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward &&
            zslValueLteMax(x->level[i].forward->score,&range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    redisAssert(x != NULL);

    /* Check if score >= min. */
    if (!zslValueGteMin(x->score,&range)) return NULL;
    return x;
}

/* Delete all the elements with score between min and max from the skiplist.
 * Min and mx are inclusive, so a score >= min || score <= max is deleted.
 * Note that this function takes the reference to the hash table view of the
 * sorted set, in order to remove the elements from the hash table too. */
 /*
  * 删除给定范围内的 score 的元素。
  *
  * T = O(N^2)
  */
unsigned long zslDeleteRangeByScore(zskiplist *zsl, zrangespec range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    // 记录沿途的节点
    // O(N)
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (range.minex ?
            x->level[i].forward->score <= range.min :
            x->level[i].forward->score < range.min))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    // 一直向右删除，直到到达 range 的底为止
    // O(N^2)
    while (x && (range.maxex ? x->score < range.max : x->score <= range.max)) {
        // 保存后继指针
        zskiplistNode *next = x->level[0].forward;
        // 在跳跃表中删除, O(N)
        zslDeleteNode(zsl,x,update);
        // 在字典中删除，O(1)
        dictDelete(dict,x->obj);
        // 释放
        zslFreeNode(x);

        removed++;

        x = next;
    }

    return removed;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
/*
 * 删除给定排序范围内的所有节点
 *
 * T = O(N^2)
 */
unsigned long zslDeleteRangeByRank(zskiplist *zsl, unsigned int start, unsigned int end, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;
    int i;

    // 通过计算 rank ，移动到删除开始的地方
    // O(N)
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    // 算上 start 节点
    traversed++;
    // 从 start 开始，删除直到到达索引 end ，或者末尾
    // O(N^2)
    x = x->level[0].forward;
    while (x && traversed <= end) {
        // 保存后一节点的指针
        zskiplistNode *next = x->level[0].forward;
        // 删除 skiplist 节点, O(N)
        zslDeleteNode(zsl,x,update);
        // 删除 dict 节点, O(1)
        dictDelete(dict,x->obj);
        // 删除节点
        zslFreeNode(x);
        // 删除计数
        removed++;
        traversed++;
        x = next;
    }
    return removed;
}

/* Find the rank for an element by both score and key.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element. */
 /*
  * 返回目标元素在有序集中的 rank 
  *
  * 如果元素不存在于有序集，那么返回 0 。
  *
  * T = O(N)
  */
unsigned long zslGetRank(zskiplist *zsl, double score, robj *o) {
    zskiplistNode *x;
    unsigned long rank = 0;
    int i;

    x = zsl->header;
    // 遍历 ziplist ，并累积沿途的 span 到 rank ，找到目标元素时返回 rank
    // O(N)
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                compareStringObjects(x->level[i].forward->obj,o) <= 0))) {
            // 累积
            rank += x->level[i].span;
            // 前进
            x = x->level[i].forward;
        }

        /* x might be equal to zsl->header, so test if obj is non-NULL */
        // 找到目标元素
        if (x->obj && equalStringObjects(x->obj,o)) {
            return rank;
        }
    }
    return 0;
}

/* Finds an element by its rank. The rank argument needs to be 1-based. */
/*
 * 根据给定的 rank 查找元素
 *
 * T = O(N)
 */
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank) {
    zskiplistNode *x;
    unsigned long traversed = 0;
    int i;

    // 沿着指针前进，直到累积的步数 traversed 等于 rank 为止
    // O(N)
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank)
        {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank) {
            return x;
        }
    }

    // 没找到
    return NULL;
}

/* Populate the rangespec according to the objects min and max. */
/*
 * 根据 min 和 max 对象，将 range 值保存到 spec 上。
 *
 * T = O(1)
 */
static int zslParseRange(robj *min, robj *max, zrangespec *spec) {
    char *eptr;
    spec->minex = spec->maxex = 0;

    /* Parse the min-max interval. If one of the values is prefixed
     * by the "(" character, it's considered "open". For instance
     * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */
    if (min->encoding == REDIS_ENCODING_INT) {
        spec->min = (long)min->ptr;
    } else {
        if (((char*)min->ptr)[0] == '(') {
            spec->min = strtod((char*)min->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return REDIS_ERR;
            spec->minex = 1;
        } else {
            spec->min = strtod((char*)min->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return REDIS_ERR;
        }
    }
    if (max->encoding == REDIS_ENCODING_INT) {
        spec->max = (long)max->ptr;
    } else {
        if (((char*)max->ptr)[0] == '(') {
            spec->max = strtod((char*)max->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return REDIS_ERR;
            spec->maxex = 1;
        } else {
            spec->max = strtod((char*)max->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return REDIS_ERR;
        }
    }

    return REDIS_OK;
}

/*-----------------------------------------------------------------------------
 * Ziplist-backed sorted set API
 *----------------------------------------------------------------------------*/

/*
 * 取出 sptr 所指向的 ziplist 节点的 score 值
 *
 * T = O(1)
 */
double zzlGetScore(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    char buf[128];
    double score;

    redisAssert(sptr != NULL);
    redisAssert(ziplistGet(sptr,&vstr,&vlen,&vlong));
    
    if (vstr) {
        // 字符串值
        // 用在这里表示 score 是一个非常大的整数
        // （超过 long long 类型）
        // 或者一个浮点数
        memcpy(buf,vstr,vlen);
        buf[vlen] = '\0';
        score = strtod(buf,NULL);
    } else {
        // 整数值
        score = vlong;
    }

    return score;
}

/* Compare element in sorted set with given element. */
/*
 * 在 eptr 节点保存的值和 cstr 之间进行对比
 *
 * T = O(N)
 */
int zzlCompareElements(unsigned char *eptr, unsigned char *cstr, unsigned int clen) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    unsigned char vbuf[32];
    int minlen, cmp;

    redisAssert(ziplistGet(eptr,&vstr,&vlen,&vlong));
    if (vstr == NULL) {
        /* Store string representation of long long in buf. */
        // 如果节点保存的是整数值，
        // 那么将它转换为字符串表示
        vlen = ll2string((char*)vbuf,sizeof(vbuf),vlong);
        vstr = vbuf;
    }

    // 对比
    // 小优化（只对比长度较短的字符串的长度）
    minlen = (vlen < clen) ? vlen : clen;   
    cmp = memcmp(vstr,cstr,minlen);
    if (cmp == 0) return vlen-clen;
    return cmp;
}

/*
 * 返回 ziplist 表示的有序集的长度
 *
 * T = O(N)
 */
unsigned int zzlLength(unsigned char *zl) {
    // 每个有序集用两个 ziplist 节点表示
    // O(N)
    return ziplistLen(zl)/2;
}

/* Move to next entry based on the values in eptr and sptr. Both are set to
 * NULL when there is no next entry. */
 /*
  * 移动指针指向有序集的下个节点
  *
  * 其中 eptr 指向下个节点的 member 域，
  * sptr 指向下个节点的 score 域。
  *
  * 当整个 ziplist 遍历完时，返回 NULL
  *
  * T = O(1)
  */
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    redisAssert(*eptr != NULL && *sptr != NULL);

    // 指向下一节点的 member 域
    _eptr = ziplistNext(zl,*sptr);
    if (_eptr != NULL) {
        // 指向下一节点的 score 域
        _sptr = ziplistNext(zl,_eptr);
        redisAssert(_sptr != NULL);
    } else {
        /* No next entry. */
        _sptr = NULL;
    }

    // 更新指针
    *eptr = _eptr;
    *sptr = _sptr;
}

/* Move to the previous entry based on the values in eptr and sptr. Both are
 * set to NULL when there is no next entry. */
/*
 * 移动到 eptr 和 sptr 所指向的节点的前一个节点，
 * 并将更新后的位置保存回 eptr 和 sptr 。
 *
 * 如果已经到达尽头，将两个指针都设为 NULL 
 *
 * T = O(1)
 */
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    redisAssert(*eptr != NULL && *sptr != NULL);

    // 指向前一节点的 score 域
    _sptr = ziplistPrev(zl,*eptr);
    if (_sptr != NULL) {
        // 指向前一节点的 memeber 域
        _eptr = ziplistPrev(zl,_sptr);
        redisAssert(_eptr != NULL);
    } else {
        /* No previous entry. */
        _eptr = NULL;
    }

    // 更新指针
    *eptr = _eptr;
    *sptr = _sptr;
}

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInRange and zzlLastInRange. */
/*
 * 检查有序集的 score 值是否在给定的 range 之内
 *
 * T = O(1)
 */
int zzlIsInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *p;
    double score;

    /* Test for ranges that will always be empty. */
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;

    // 取出有序集中最小的 score 值
    p = ziplistIndex(zl,-1); /* Last score. */
    if (p == NULL) return 0; /* Empty sorted set */
    score = zzlGetScore(p);
    // 如果 score 值不位于给定边界之内，返回 0
    if (!zslValueGteMin(score,range))
        return 0;

    // 取出有序集中最大的 score 值
    p = ziplistIndex(zl,1); /* First score. */
    redisAssert(p != NULL);
    score = zzlGetScore(p);
    // 如果 score 值不位于给定边界之内，返回 0
    if (!zslValueLteMax(score,range))
        return 0;

    return 1;
}

/* Find pointer to the first element contained in the specified range.
 * Returns NULL when no element is contained in the range. */
/*
 * 返回第一个 score 值在给定范围内的节点
 *
 * 如果没有节点的 score 值在给定范围，返回 NULL 。
 *
 * T = O(N)
 */
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec range) {
    // 从表头开始遍历
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;
    double score;

    /* If everything is out of range, return early. */
    if (!zzlIsInRange(zl,&range)) return NULL;

    // 从表头向表尾遍历
    while (eptr != NULL) {
        sptr = ziplistNext(zl,eptr);
        redisAssert(sptr != NULL);

        // 获取 score 值
        score = zzlGetScore(sptr);
        // score 值在范围之内？
        if (zslValueGteMin(score,&range)) {
            /* Check if score <= max. */
            if (zslValueLteMax(score,&range))
                return eptr;
            return NULL;
        }

        /* Move to next element. */
        // 后移指针
        eptr = ziplistNext(zl,sptr);
    }

    return NULL;
}

/* Find pointer to the last element contained in the specified range.
 * Returns NULL when no element is contained in the range. */
/*
 * 返回 score 值在给定范围内的最后一个节点
 *
 * 没有元素包含它时，返回 NULL
 *
 * T = O(N)
 */
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec range) {
    // 从表尾开始遍历
    unsigned char *eptr = ziplistIndex(zl,-2), *sptr;
    double score;

    /* If everything is out of range, return early. */
    if (!zzlIsInRange(zl,&range)) return NULL;

    // 在有序的 ziplist 里从表尾到表头遍历
    while (eptr != NULL) {
        sptr = ziplistNext(zl,eptr);
        redisAssert(sptr != NULL);

        // 获取节点的 score 值
        score = zzlGetScore(sptr);
        // score 在给定的范围之内？
        if (zslValueLteMax(score,&range)) {
            /* Check if score >= min. */
            if (zslValueGteMin(score,&range))
                return eptr;
            return NULL;
        }

        /* Move to previous element by moving to the score of previous element.
         * When this returns NULL, we know there also is no element. */
        // 前移指针
        sptr = ziplistPrev(zl,eptr);
        if (sptr != NULL)
            redisAssert((eptr = ziplistPrev(zl,sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

/*
 * 在 ziplist 里查找给定元素 ele ，如果找到了，
 * 将元素的点数保存到 score ，并返回该元素在 ziplist 的指针。
 *
 * T = O(N^2)
 */
unsigned char *zzlFind(unsigned char *zl, robj *ele, double *score) {

    // 迭代器
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;

    // 解码
    ele = getDecodedObject(ele);

    // 遍历整个 ziplist ， O(N^2)
    while (eptr != NULL) {
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(NULL,ele,sptr != NULL);

        // 对比元素 ele 的值和 eptr 所保存的值
        // O(N)
        if (ziplistCompare(eptr,ele->ptr,sdslen(ele->ptr))) {
            /* Matching element, pull out score. */
            // 将匹配元素的指针保存到 score 里
            if (score != NULL) *score = zzlGetScore(sptr);

            decrRefCount(ele);

            // 返回指针
            return eptr;
        }

        /* Move to next element. */
        eptr = ziplistNext(zl,sptr);
    }

    decrRefCount(ele);
    return NULL;
}

/* Delete (element,score) pair from ziplist. Use local copy of eptr because we
 * don't want to modify the one given as argument. */
/*
 * 从 ziplist 中删除 element-score 对。
 * 使用一个副本保存 eptr 的值。
 *
 * T = O(N^2)
 */
unsigned char *zzlDelete(unsigned char *zl, unsigned char *eptr) {
    unsigned char *p = eptr;

    /* TODO: add function to ziplist API to delete N elements from offset. */
    // 删除 member 域 ，O(N^2)
    zl = ziplistDelete(zl,&p);
    // 删除 score 域 ，O(N^2)
    zl = ziplistDelete(zl,&p);

    return zl;
}

/*
 * 将有序集节点保存到 eptr 所指向的地方
 *
 * T = O(N^2)
 */
unsigned char *zzlInsertAt(unsigned char *zl, unsigned char *eptr, robj *ele, double score) {
    unsigned char *sptr;
    char scorebuf[128];
    int scorelen;
    size_t offset;

    redisAssertWithInfo(NULL,ele,ele->encoding == REDIS_ENCODING_RAW);
    // 将 score 值转换为字符串
    scorelen = d2string(scorebuf,sizeof(scorebuf),score);
    if (eptr == NULL) {
        // 插入到 ziplist 的最后, O(N^2)
        // ziplist 的第一个节点保存有序集的 member
        zl = ziplistPush(zl,ele->ptr,sdslen(ele->ptr),ZIPLIST_TAIL);
        // ziplist 的第二个节点保存有序集的 score
        zl = ziplistPush(zl,(unsigned char*)scorebuf,scorelen,ZIPLIST_TAIL);
    } else {
        // 插入到给定位置, O(N^2)
        /* Keep offset relative to zl, as it might be re-allocated. */
        // 记录 ziplist 的相对偏移量（而不是指针），避免内存重分配之后位置丢失
        offset = eptr-zl;
        // 保存 member
        zl = ziplistInsert(zl,eptr,ele->ptr,sdslen(ele->ptr));
        eptr = zl+offset;

        /* Insert score after the element. */
        redisAssertWithInfo(NULL,ele,(sptr = ziplistNext(zl,eptr)) != NULL);
        // 保存 score
        zl = ziplistInsert(zl,sptr,(unsigned char*)scorebuf,scorelen);
    }

    return zl;
}

/* Insert (element,score) pair in ziplist. This function assumes the element is
 * not yet present in the list. */
/*
 * 将 ele 成员和它的分值 score 添加到 ziplist 里面
 *
 * ziplist 里的各个节点按 score 值从小到大排列
 *
 * 这个函数假设 elem 不存在于有序集
 *
 * T = O(N^2)
 */
unsigned char *zzlInsert(unsigned char *zl, robj *ele, double score) {
    // 指向 ziplist 第一个节点（也即是有序集的 member 域）
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;
    double s;

    // 解码值
    ele = getDecodedObject(ele);
    // 遍历整个 ziplist
    while (eptr != NULL) {
        // 指向 score 域
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(NULL,ele,sptr != NULL);
        // 取出 score 值
        s = zzlGetScore(sptr);

        if (s > score) {
            /* First element with score larger than score for element to be
             * inserted. This means we should take its spot in the list to
             * maintain ordering. */
            // 遇到第一个 score 值比输入 score 大的节点
            // 将新节点插入在这个节点的前面，
            // 让节点在 ziplist 里根据 score 从小到大排列
            // O(N^2)
            zl = zzlInsertAt(zl,eptr,ele,score);
            break;
        } else if (s == score) {
            /* Ensure lexicographical ordering for elements. */
            // 如果输入 score 和节点的 score 相同
            // 那么根据 member 的字符串位置来决定新节点的插入位置
            if (zzlCompareElements(eptr,ele->ptr,sdslen(ele->ptr)) > 0) {
                // O(N^2)
                zl = zzlInsertAt(zl,eptr,ele,score);
                break;
            }
        }

        /* Move to next element. */
        // 输入 score 比节点的 score 值要大
        // 移动到下一个节点
        eptr = ziplistNext(zl,sptr);
    }

    /* Push on tail of list when it was not yet inserted. */
    // 如果有序集里目前没有一个节点的 score 值比输入 score 大
    // 那么将新节点添加到 ziplist 的最后
    if (eptr == NULL)
        // O(N^2)
        zl = zzlInsertAt(zl,NULL,ele,score);

    decrRefCount(ele);
    return zl;
}

/*
 * 删除给定 score 范围内的节点
 *
 * T = O(N^3)
 */
unsigned char *zzlDeleteRangeByScore(unsigned char *zl, zrangespec range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    double score;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    // 按 range 定位起始节点的位置
    eptr = zzlFirstInRange(zl,range);
    if (eptr == NULL) return zl;

    /* When the tail of the ziplist is deleted, eptr will point to the sentinel
     * byte and ziplistNext will return NULL. */
    // 一直进行删除，直到碰到 score 值比 range->max 更大的节点为止
    // O(N^3)
    while ((sptr = ziplistNext(zl,eptr)) != NULL) {
        score = zzlGetScore(sptr);
        if (zslValueLteMax(score,&range)) {
            /* Delete both the element and the score. */
            // O(N^2)
            zl = ziplistDelete(zl,&eptr);
            // O(N^2)
            zl = ziplistDelete(zl,&eptr);
            num++;
        } else {
            /* No longer in range. */
            break;
        }
    }

    if (deleted != NULL) *deleted = num;
    return zl;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
/*
 * 删除给定排列区间内的所有节点
 *
 * T = O(N^2)
 */
unsigned char *zzlDeleteRangeByRank(unsigned char *zl, unsigned int start, unsigned int end, unsigned long *deleted) {
    // 计算删除的节点数量
    unsigned int num = (end-start)+1;
    if (deleted) *deleted = num;
    // 删除
    zl = ziplistDeleteRange(zl,2*(start-1),2*num);
    return zl;
}

/*-----------------------------------------------------------------------------
 * Common sorted set API
 *----------------------------------------------------------------------------*/

/*
 * 返回有序集的元素个数
 *
 * T = O(N)
 */
unsigned int zsetLength(robj *zobj) {
    int length = -1;
    // O(N)
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        length = zzlLength(zobj->ptr);
    // O(1)
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        length = ((zset*)zobj->ptr)->zsl->length;
    } else {
        redisPanic("Unknown sorted set encoding");
    }
    return length;
}

/*
 * 将给定的 zobj 转换成给定编码
 *
 * T = O(N^3)
 */
void zsetConvert(robj *zobj, int encoding) {
    zset *zs;
    zskiplistNode *node, *next;
    robj *ele;
    double score;

    // 编码相同，无须转换
    if (zobj->encoding == encoding) return;

    // 将 ziplist 编码转换成 skiplist 编码
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        if (encoding != REDIS_ENCODING_SKIPLIST)
            redisPanic("Unknown target encoding");

        // 创建新 zset
        zs = zmalloc(sizeof(*zs));
        zs->dict = dictCreate(&zsetDictType,NULL);
        zs->zsl = zslCreate();

        // 指向第一个节点的 member 域
        eptr = ziplistIndex(zl,0);
        redisAssertWithInfo(NULL,zobj,eptr != NULL);
        // 指向第一个节点的 score 域
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(NULL,zobj,sptr != NULL);

        // 遍历整个 ziplist ，将它的 member 和 score 添加到 zset
        // O(N^2)
        while (eptr != NULL) {
            // 取出 score 值
            score = zzlGetScore(sptr);
            // 取出 member 值
            redisAssertWithInfo(NULL,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));

            // 为 member 值创建 robj 对象
            if (vstr == NULL)
                ele = createStringObjectFromLongLong(vlong);
            else
                ele = createStringObject((char*)vstr,vlen);

            /* Has incremented refcount since it was just created. */
            // 将 score 和 member（这里的ele）添加到 skiplist
            // O(N)
            node = zslInsert(zs->zsl,score,ele);
            // 将 member 作为键， score 作为值，保存到字典
            // O(1)
            redisAssertWithInfo(NULL,zobj,dictAdd(zs->dict,ele,&node->score) == DICT_OK);
            incrRefCount(ele); /* Added to dictionary. */

            // 前进至下个节点
            zzlNext(zl,&eptr,&sptr);
        }

        zfree(zobj->ptr);
        zobj->ptr = zs;
        zobj->encoding = REDIS_ENCODING_SKIPLIST;

    // 将 skiplist 转换为 ziplist
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        unsigned char *zl = ziplistNew();

        if (encoding != REDIS_ENCODING_ZIPLIST)
            redisPanic("Unknown target encoding");

        /* Approach similar to zslFree(), since we want to free the skiplist at
         * the same time as creating the ziplist. */
        zs = zobj->ptr;
        // 释放整个字典
        dictRelease(zs->dict);
        // 指向首个节点
        node = zs->zsl->header->level[0].forward;
        // 释放 zset 表头
        zfree(zs->zsl->header);
        zfree(zs->zsl);

        // 将所有元素保存到 ziplist , O(N^3)
        while (node) {
            // 取出解码后的 member
            ele = getDecodedObject(node->obj);
            // 插入 member 和 score 到 ziplist, O(N^2)
            zl = zzlInsertAt(zl,NULL,ele,node->score);
            decrRefCount(ele);

            next = node->level[0].forward;
            zslFreeNode(node);
            node = next;
        }

        zfree(zs);
        zobj->ptr = zl;
        zobj->encoding = REDIS_ENCODING_ZIPLIST;
    } else {
        redisPanic("Unknown sorted set encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Sorted set commands 
 *----------------------------------------------------------------------------*/

/* This generic command implements both ZADD and ZINCRBY. */
/*
 * 多态添加操作
 *
 * ZADD 和 ZINCRBY 的底层实现
 *
 * T = O(N^4)
 */
void zaddGenericCommand(redisClient *c, int incr) {
    static char *nanerr = "resulting score is not a number (NaN)";
    robj *key = c->argv[1];
    robj *ele;
    robj *zobj;
    robj *curobj;
    double score = 0, *scores, curscore = 0.0;
    int j, elements = (c->argc-2)/2;
    int added = 0;

    // 参数 member - score 对，直接报错
    if (c->argc % 2) {
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Start parsing all the scores, we need to emit any syntax error
     * before executing additions to the sorted set, as the command should
     * either execute fully or nothing at all. */
    // parse 所有输入的 score 
    // 如果发现错误，立即报错并返回
    scores = zmalloc(sizeof(double)*elements);
    for (j = 0; j < elements; j++) {
        if (getDoubleFromObjectOrReply(c,c->argv[2+j*2],&scores[j],NULL)
            != REDIS_OK)
        {
            zfree(scores);
            return;
        }
    }

    /* Lookup the key and create the sorted set if does not exist. */
    // 按 key 查找排序集合，如果 key 为空就创建一个
    zobj = lookupKeyWrite(c->db,key);
    // 对象不存在，创建有序集
    if (zobj == NULL) {

        // 创建 skiplist 编码的 zset
        if (server.zset_max_ziplist_entries == 0 ||
            server.zset_max_ziplist_value < sdslen(c->argv[3]->ptr))
        {
            zobj = createZsetObject();
        // 创建 ziplist 编码的 zset
        } else {
            zobj = createZsetZiplistObject();
        }

        // 添加新有序集到 db
        dbAdd(c->db,key,zobj);
    } else {
        // 对已存在对象进行类型检查
        if (zobj->type != REDIS_ZSET) {
            addReply(c,shared.wrongtypeerr);
            zfree(scores);
            return;
        }
    }

    // 遍历所有元素，将它们加入到有序集
    // O(N^4)
    for (j = 0; j < elements; j++) {
        score = scores[j];

        // 添加元素到 ziplist 编码的有序集, O(N^3)
        if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
            unsigned char *eptr;

            /* Prefer non-encoded element when dealing with ziplists. */
            // 获取元素
            ele = c->argv[3+j*2];
            // 如果元素存在，那么取出它
            // O(N)
            if ((eptr = zzlFind(zobj->ptr,ele,&curscore)) != NULL) {
                // zincrby 操作，追加 incr 到元素的值
                if (incr) {
                    score += curscore;
                    // 检查溢出
                    if (isnan(score)) {
                        addReplyError(c,nanerr);
                        /* Don't need to check if the sorted set is empty
                         * because we know it has at least one element. */
                        zfree(scores);
                        return;
                    }
                }

                /* Remove and re-insert when score changed. */
                // 如果两个元素点数不同，那么用新元素替换旧元素
                if (score != curscore) {
                    // 删除旧元素-点数, O(N^2)
                    zobj->ptr = zzlDelete(zobj->ptr,eptr);
                    // 插入新元素-点数, O(N^2)
                    zobj->ptr = zzlInsert(zobj->ptr,ele,score);

                    signalModifiedKey(c->db,key);
                    server.dirty++;
                }
            // 元素不存在
            } else {
                /* Optimize: check if the element is too large or the list
                 * becomes too long *before* executing zzlInsert. */
                // 添加元素到 ziplist
                // O(N^2)
                zobj->ptr = zzlInsert(zobj->ptr,ele,score);

                // 如果有需要，将 ziplist 转换为 skiplist 编码
                if (zzlLength(zobj->ptr) > server.zset_max_ziplist_entries)
                    // O(N^3)
                    zsetConvert(zobj,REDIS_ENCODING_SKIPLIST);
                if (sdslen(ele->ptr) > server.zset_max_ziplist_value)
                    // O(N^3)
                    zsetConvert(zobj,REDIS_ENCODING_SKIPLIST);

                signalModifiedKey(c->db,key);
                server.dirty++;

                // 这是 incrby 还是 zadd 操作？
                if (!incr) added++;
            }
        // 添加元素到 skiplist
        } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
            zset *zs = zobj->ptr;
            zskiplistNode *znode;
            dictEntry *de;
    
            // 编码元素
            ele = c->argv[3+j*2] = tryObjectEncoding(c->argv[3+j*2]);

            // 在字典中查找元素, O(1)
            de = dictFind(zs->dict,ele);

            // 元素存在（更新 score）
            if (de != NULL) {
                // 当前 member
                curobj = dictGetKey(de);
                // 当前 score
                curscore = *(double*)dictGetVal(de);

                // INCRBY 操作
                if (incr) {
                    score += curscore;
                    // 检查溢出
                    if (isnan(score)) {
                        addReplyError(c,nanerr);
                        /* Don't need to check if the sorted set is empty
                         * because we know it has at least one element. */
                        zfree(scores);
                        return;
                    }
                }

                /* Remove and re-insert when score changed. We can safely
                 * delete the key object from the skiplist, since the
                 * dictionary still has a reference to it. */
                // 新旧 score 值不同，先将旧元素（和分值）删除，再重新添加到 ziplist
                if (score != curscore) {
                    // 删除 zset 旧节点, O(N)
                    redisAssertWithInfo(c,curobj,zslDelete(zs->zsl,curscore,curobj));

                    // 添加带新点数的节点到 zset, O(N)
                    znode = zslInsert(zs->zsl,score,curobj);

                    incrRefCount(curobj); /* Re-inserted in skiplist. */

                    // 更新字典保存的元素的分值
                    dictGetVal(de) = &znode->score; /* Update score ptr. */

                    signalModifiedKey(c->db,key);
                    server.dirty++;
                }
            // 元素不存在（添加操作）
            } else {

                // 添加到 zset, O(N)
                znode = zslInsert(zs->zsl,score,ele);
                incrRefCount(ele); /* Inserted in skiplist. */

                // 添加到 dict , O(1)
                redisAssertWithInfo(c,NULL,dictAdd(zs->dict,ele,&znode->score) == DICT_OK);
                incrRefCount(ele); /* Added to dictionary. */

                signalModifiedKey(c->db,key);
                server.dirty++;

                // 添加操作
                if (!incr) added++;
            }
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    }

    zfree(scores);

    if (incr) /* ZINCRBY */
        addReplyDouble(c,score);
    else /* ZADD */
        addReplyLongLong(c,added);
}

void zaddCommand(redisClient *c) {
    zaddGenericCommand(c,0);
}

void zincrbyCommand(redisClient *c) {
    zaddGenericCommand(c,1);
}

/*
 * 多态元素删除函数
 *
 * T = O(N^3)
 */
void zremCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    int deleted = 0, j;

    // 查找对象，检查类型
    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    // ziplist
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *eptr;

        // O(N^3)
        for (j = 2; j < c->argc; j++) {
            // 如果元素存在，那么对它进行删除, O(N^2)
            if ((eptr = zzlFind(zobj->ptr,c->argv[j],NULL)) != NULL) {
                deleted++;
                // O(N^2)
                zobj->ptr = zzlDelete(zobj->ptr,eptr);

                // 如果集合为空，那么删除它
                if (zzlLength(zobj->ptr) == 0) {
                    dbDelete(c->db,key);
                    break;
                }
            }
        }
    // 跳跃表
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        dictEntry *de;
        double score;

        // O(N^2)
        for (j = 2; j < c->argc; j++) {
            // 取出元素 O(1)
            de = dictFind(zs->dict,c->argv[j]);
            if (de != NULL) {
                deleted++;

                /* Delete from the skiplist */
                // 删除 score 域
                score = *(double*)dictGetVal(de);
                // 从 skiplist 中删除元素
                // O(N)
                redisAssertWithInfo(c,c->argv[j],zslDelete(zs->zsl,score,c->argv[j]));

                /* Delete from the hash table */
                // 从字典中删除元素
                // O(1)
                dictDelete(zs->dict,c->argv[j]);
                // O(N)
                if (htNeedsResize(zs->dict)) dictResize(zs->dict);
                if (dictSize(zs->dict) == 0) {
                    dbDelete(c->db,key);
                    break;
                }
            }
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    if (deleted) {
        signalModifiedKey(c->db,key);
        server.dirty += deleted;
    }
    addReplyLongLong(c,deleted);
}

/*
 * 多态地移除给定 score 范围内的元素
 *
 * T = O(N^2)
 */
void zremrangebyscoreCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zrangespec range;
    unsigned long deleted;

    /* Parse the range arguments. */
    // 解释输入范围
    if (zslParseRange(c->argv[2],c->argv[3],&range) != REDIS_OK) {
        addReplyError(c,"min or max is not a float");
        return;
    }

    // 查找 key ，检查类型
    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    // ziplist
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        // O(N^3)
        zobj->ptr = zzlDeleteRangeByScore(zobj->ptr,range,&deleted);
        // 删除空 ziplist
        if (zzlLength(zobj->ptr) == 0) dbDelete(c->db,key);
    // skiplist
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        // O(N^2)
        deleted = zslDeleteRangeByScore(zs->zsl,range,zs->dict);
        // 紧缩空间
        if (htNeedsResize(zs->dict)) dictResize(zs->dict);
        // 删除空字典
        if (dictSize(zs->dict) == 0) dbDelete(c->db,key);
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    if (deleted) signalModifiedKey(c->db,key);

    server.dirty += deleted;
    addReplyLongLong(c,deleted);
}

/*
 * 删除给定排序范围内的所有元素
 *
 * T = O(N^2)
 */
void zremrangebyrankCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    long start;
    long end;
    int llen;
    unsigned long deleted;

    // 读入 start 和 end 参数
    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK)) return;

    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    /* Sanitize indexes. */
    llen = zsetLength(zobj);
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        addReply(c,shared.czero);
        return;
    }
    if (end >= llen) end = llen-1;

    // ziplist
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        /* Correct for 1-based rank. */
        // O(N^2)
        zobj->ptr = zzlDeleteRangeByRank(zobj->ptr,start+1,end+1,&deleted);
        if (zzlLength(zobj->ptr) == 0) dbDelete(c->db,key);
    // skiplist
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;

        /* Correct for 1-based rank. */
        // O(N^2)
        deleted = zslDeleteRangeByRank(zs->zsl,start+1,end+1,zs->dict);
        if (htNeedsResize(zs->dict)) dictResize(zs->dict);
        if (dictSize(zs->dict) == 0) dbDelete(c->db,key);
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    if (deleted) signalModifiedKey(c->db,key);
    server.dirty += deleted;
    addReplyLongLong(c,deleted);
}

/*
 * 迭代器结构
 */
typedef struct {
    // 迭代目标
    robj *subject;
    // 类型：可以是集合或有序集
    int type; /* Set, sorted set */
    // 编码
    int encoding;
    // 权重
    double weight;

    union {
        // 集合迭代器
        /* Set iterators. */
        union _iterset {
            struct {
                intset *is;
                int ii;
            } is;
            struct {
                dict *dict;
                dictIterator *di;
                dictEntry *de;
            } ht;
        } set;

        /* Sorted set iterators. */
        // 有序集迭代器
        union _iterzset {
            // ziplist 编码
            struct {
                unsigned char *zl;
                unsigned char *eptr, *sptr;
            } zl;
            // zset 编码
            struct {
                zset *zs;
                zskiplistNode *node;
            } sl;
        } zset;
    } iter;
} zsetopsrc;


/* Use dirty flags for pointers that need to be cleaned up in the next
 * iteration over the zsetopval. The dirty flag for the long long value is
 * special, since long long values don't need cleanup. Instead, it means that
 * we already checked that "ell" holds a long long, or tried to convert another
 * representation into a long long value. When this was successful,
 * OPVAL_VALID_LL is set as well. */
#define OPVAL_DIRTY_ROBJ 1
#define OPVAL_DIRTY_LL 2
#define OPVAL_VALID_LL 4

/* Store value retrieved from the iterator. */
/*
 * 保存从迭代器取得的值
 */
typedef struct {
    int flags;
    unsigned char _buf[32]; /* Private buffer. */
    // 可能保存 member 的几个类型
    robj *ele;
    unsigned char *estr;
    unsigned int elen;
    long long ell;
    // score 值
    double score;
} zsetopval;

typedef union _iterset iterset;
typedef union _iterzset iterzset;

/*
 * 初始化迭代器
 */
void zuiInitIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == REDIS_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == REDIS_ENCODING_INTSET) {
            it->is.is = op->subject->ptr;
            it->is.ii = 0;
        } else if (op->encoding == REDIS_ENCODING_HT) {
            it->ht.dict = op->subject->ptr;
            it->ht.di = dictGetIterator(op->subject->ptr);
            it->ht.de = dictNext(it->ht.di);
        } else {
            redisPanic("Unknown set encoding");
        }
    } else if (op->type == REDIS_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == REDIS_ENCODING_ZIPLIST) {
            it->zl.zl = op->subject->ptr;
            it->zl.eptr = ziplistIndex(it->zl.zl,0);
            if (it->zl.eptr != NULL) {
                it->zl.sptr = ziplistNext(it->zl.zl,it->zl.eptr);
                redisAssert(it->zl.sptr != NULL);
            }
        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
            it->sl.zs = op->subject->ptr;
            it->sl.node = it->sl.zs->zsl->header->level[0].forward;
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } else {
        redisPanic("Unsupported type");
    }
}

/*
 * 清空迭代器
 */
void zuiClearIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == REDIS_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == REDIS_ENCODING_INTSET) {
            REDIS_NOTUSED(it); /* skip */
        } else if (op->encoding == REDIS_ENCODING_HT) {
            dictReleaseIterator(it->ht.di);
        } else {
            redisPanic("Unknown set encoding");
        }
    } else if (op->type == REDIS_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == REDIS_ENCODING_ZIPLIST) {
            REDIS_NOTUSED(it); /* skip */
        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
            REDIS_NOTUSED(it); /* skip */
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } else {
        redisPanic("Unsupported type");
    }
}

/*
 * 返回迭代对象的元素数量
 */
int zuiLength(zsetopsrc *op) {
    if (op->subject == NULL)
        return 0;

    if (op->type == REDIS_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == REDIS_ENCODING_INTSET) {
            return intsetLen(it->is.is);
        } else if (op->encoding == REDIS_ENCODING_HT) {
            return dictSize(it->ht.dict);
        } else {
            redisPanic("Unknown set encoding");
        }
    } else if (op->type == REDIS_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == REDIS_ENCODING_ZIPLIST) {
            return zzlLength(it->zl.zl);
        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
            return it->sl.zs->zsl->length;
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } else {
        redisPanic("Unsupported type");
    }
}

/* Check if the current value is valid. If so, store it in the passed structure
 * and move to the next element. If not valid, this means we have reached the
 * end of the structure and can abort. */
/*
 * 从迭代器中取出下一个元素，并将它保存到 val ，然后返回 1 。
 *
 * 当没有下一个元素时，返回 0 。
 *
 * T = O(N)
 */
int zuiNext(zsetopsrc *op, zsetopval *val) {
    if (op->subject == NULL)
        return 0;

    if (val->flags & OPVAL_DIRTY_ROBJ)
        decrRefCount(val->ele);

    // 清零
    memset(val,0,sizeof(zsetopval));

    // 输入是集合
    if (op->type == REDIS_SET) {
        iterset *it = &op->iter.set;
        // intset 编码
        if (op->encoding == REDIS_ENCODING_INTSET) {
            int64_t ell;

            // O(1)
            if (!intsetGet(it->is.is,it->is.ii,&ell))
                return 0;
            // 取出member
            val->ell = ell;
            // 集合元素没有 score ，为它设置一个默认 score
            val->score = 1.0;

            /* Move to next element. */
            it->is.ii++;
        // ht 编码
        } else if (op->encoding == REDIS_ENCODING_HT) {
            if (it->ht.de == NULL)
                return 0;
            // 取出member
            // O(1)
            val->ele = dictGetKey(it->ht.de);
            // 集合元素没有 score ，为它设置一个默认 score
            val->score = 1.0;

            /* Move to next element. */
            it->ht.de = dictNext(it->ht.di);
        } else {
            redisPanic("Unknown set encoding");
        }

    // 输入是有序集
    } else if (op->type == REDIS_ZSET) {
        iterzset *it = &op->iter.zset;
        // ziplist 编码, O(N)
        if (op->encoding == REDIS_ENCODING_ZIPLIST) {
            /* No need to check both, but better be explicit. */
            if (it->zl.eptr == NULL || it->zl.sptr == NULL)
                return 0;
            // 取出 member
            redisAssert(ziplistGet(it->zl.eptr,&val->estr,&val->elen,&val->ell));
            // 取出 score
            val->score = zzlGetScore(it->zl.sptr);

            /* Move to next element. */
            zzlNext(it->zl.zl,&it->zl.eptr,&it->zl.sptr);
        // skiplist 编码, O(N)
        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
            if (it->sl.node == NULL)
                return 0;
            // 取出 member
            val->ele = it->sl.node->obj;
            // 取出 score
            val->score = it->sl.node->score;

            /* Move to next element. */
            it->sl.node = it->sl.node->level[0].forward;
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } else {
        redisPanic("Unsupported type");
    }


    return 1;
}

/*
 * 从 val 里取出并返回整数值
 *
 * T = O(1)
 */
int zuiLongLongFromValue(zsetopval *val) {
    if (!(val->flags & OPVAL_DIRTY_LL)) {
        val->flags |= OPVAL_DIRTY_LL;

        // 输入为集合时使用...
        if (val->ele != NULL) {
            // 从 intset 里取出
            if (val->ele->encoding == REDIS_ENCODING_INT) {
                val->ell = (long)val->ele->ptr;
                val->flags |= OPVAL_VALID_LL;
            // 从 sds 里取出
            } else if (val->ele->encoding == REDIS_ENCODING_RAW) {
                if (string2ll(val->ele->ptr,sdslen(val->ele->ptr),&val->ell))
                    val->flags |= OPVAL_VALID_LL;
            } else {
                redisPanic("Unsupported element encoding");
            }

        // 输入为有序集时使用
        // 从字符串中取出
        } else if (val->estr != NULL) {
            if (string2ll((char*)val->estr,val->elen,&val->ell))
                val->flags |= OPVAL_VALID_LL;
        } else {
            /* The long long was already set, flag as valid. */
            val->flags |= OPVAL_VALID_LL;
        }
    }
    return val->flags & OPVAL_VALID_LL;
}

/*
 * 将 zsetopval 里的结果取出，保存到 robj 对象里，然后返回
 *
 * T = O(1)
 */
robj *zuiObjectFromValue(zsetopval *val) {
    if (val->ele == NULL) {
        if (val->estr != NULL) {
            // member 为整数
            val->ele = createStringObject((char*)val->estr,val->elen);
        } else {
            // member 为整数值
            val->ele = createStringObjectFromLongLong(val->ell);
        }
        val->flags |= OPVAL_DIRTY_ROBJ;
    }
    // member 为对象
    return val->ele;
}

int zuiBufferFromValue(zsetopval *val) {
    if (val->estr == NULL) {
        if (val->ele != NULL) {
            if (val->ele->encoding == REDIS_ENCODING_INT) {
                val->elen = ll2string((char*)val->_buf,sizeof(val->_buf),(long)val->ele->ptr);
                val->estr = val->_buf;
            } else if (val->ele->encoding == REDIS_ENCODING_RAW) {
                val->elen = sdslen(val->ele->ptr);
                val->estr = val->ele->ptr;
            } else {
                redisPanic("Unsupported element encoding");
            }
        } else {
            val->elen = ll2string((char*)val->_buf,sizeof(val->_buf),val->ell);
            val->estr = val->_buf;
        }
    }
    return 1;
}

/* Find value pointed to by val in the source pointer to by op. When found,
 * return 1 and store its score in target. Return 0 otherwise. */
/*
 * 在 op 中查找 member 和 score 值。
 *
 * 找到返回 1 ，否则返回 0 。
 *
 * T = O(N)
 */
int zuiFind(zsetopsrc *op, zsetopval *val, double *score) {
    if (op->subject == NULL)
        return 0;

    if (op->type == REDIS_SET) {
        iterset *it = &op->iter.set;

        if (op->encoding == REDIS_ENCODING_INTSET) {
            // O(lg N)
            if (zuiLongLongFromValue(val) && intsetFind(it->is.is,val->ell)) {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }
        } else if (op->encoding == REDIS_ENCODING_HT) {
            zuiObjectFromValue(val);
            // O(1)
            if (dictFind(it->ht.dict,val->ele) != NULL) {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }
        } else {
            redisPanic("Unknown set encoding");
        }
    } else if (op->type == REDIS_ZSET) {
        iterzset *it = &op->iter.zset;
        zuiObjectFromValue(val);

        if (op->encoding == REDIS_ENCODING_ZIPLIST) {
            // O(N)
            if (zzlFind(it->zl.zl,val->ele,score) != NULL) {
                /* Score is already set by zzlFind. */
                return 1;
            } else {
                return 0;
            }
        } else if (op->encoding == REDIS_ENCODING_SKIPLIST) {
            dictEntry *de;
            // O(1)
            if ((de = dictFind(it->sl.zs->dict,val->ele)) != NULL) {
                *score = *(double*)dictGetVal(de);
                return 1;
            } else {
                return 0;
            }
        } else {
            redisPanic("Unknown sorted set encoding");
        }
    } else {
        redisPanic("Unsupported type");
    }
}

int zuiCompareByCardinality(const void *s1, const void *s2) {
    return zuiLength((zsetopsrc*)s1) - zuiLength((zsetopsrc*)s2);
}

#define REDIS_AGGR_SUM 1
#define REDIS_AGGR_MIN 2
#define REDIS_AGGR_MAX 3
#define zunionInterDictValue(_e) (dictGetVal(_e) == NULL ? 1.0 : *(double*)dictGetVal(_e))

/*
 * 根据 aggregate 参数所指定的模式，聚合 *target 和 val 两个值。
 */
inline static void zunionInterAggregate(double *target, double val, int aggregate) {
    if (aggregate == REDIS_AGGR_SUM) {
        *target = *target + val;
        /* The result of adding two doubles is NaN when one variable
         * is +inf and the other is -inf. When these numbers are added,
         * we maintain the convention of the result being 0.0. */
        if (isnan(*target)) *target = 0.0;
    } else if (aggregate == REDIS_AGGR_MIN) {
        *target = val < *target ? val : *target;
    } else if (aggregate == REDIS_AGGR_MAX) {
        *target = val > *target ? val : *target;
    } else {
        /* safety net */
        redisPanic("Unknown ZUNION/INTER aggregate type");
    }
}

/*
 * ZUNIONSTORE 和 ZINTERSTORE 两个命令的底层实现
 *
 * T = O(N^4)
 */
void zunionInterGenericCommand(redisClient *c, robj *dstkey, int op) {
    int i, j;
    long setnum;
    int aggregate = REDIS_AGGR_SUM;
    zsetopsrc *src;
    zsetopval zval;
    robj *tmp;
    unsigned int maxelelen = 0;
    robj *dstobj;
    zset *dstzset;
    zskiplistNode *znode;
    int touched = 0;

    /* expect setnum input keys to be given */
    // 取出 setnum 参数
    if ((getLongFromObjectOrReply(c, c->argv[2], &setnum, NULL) != REDIS_OK))
        return;

    if (setnum < 1) {
        addReplyError(c,
            "at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE");
        return;
    }

    /* test if the expected number of keys would overflow */
    if (setnum > c->argc-3) {
        addReply(c,shared.syntaxerr);
        return;
    }

    /* read keys to be used for input */
    // 保存所有 key , O(N)
    src = zcalloc(sizeof(zsetopsrc) * setnum);
    for (i = 0, j = 3; i < setnum; i++, j++) {
        // 取出 key
        robj *obj = lookupKeyWrite(c->db,c->argv[j]);
        if (obj != NULL) {
            // key 可以是 sorted set 或者 set
            if (obj->type != REDIS_ZSET && obj->type != REDIS_SET) {
                zfree(src);
                addReply(c,shared.wrongtypeerr);
                return;
            }

            // 设置
            src[i].subject = obj;
            src[i].type = obj->type;
            src[i].encoding = obj->encoding;
        } else {
            src[i].subject = NULL;
        }

        /* Default all weights to 1. */
        src[i].weight = 1.0;
    }

    /* parse optional extra arguments */
    // 分析附加参数, O(N)
    if (j < c->argc) {
        int remaining = c->argc - j;

        // O(N)
        while (remaining) {
            // 读入所有 weight 参数, O(N)
            if (remaining >= (setnum + 1) && !strcasecmp(c->argv[j]->ptr,"weights")) {
                j++; remaining--;
                for (i = 0; i < setnum; i++, j++, remaining--) {
                    // 将 weight 保存到 src 数组中
                    if (getDoubleFromObjectOrReply(c,c->argv[j],&src[i].weight,
                            "weight value is not a float") != REDIS_OK)
                    {
                        zfree(src);
                        return;
                    }
                }
            // 读取所有 aggregate 参数, O(N)
            } else if (remaining >= 2 && !strcasecmp(c->argv[j]->ptr,"aggregate")) {
                j++; remaining--;
                if (!strcasecmp(c->argv[j]->ptr,"sum")) {
                    aggregate = REDIS_AGGR_SUM;
                } else if (!strcasecmp(c->argv[j]->ptr,"min")) {
                    aggregate = REDIS_AGGR_MIN;
                } else if (!strcasecmp(c->argv[j]->ptr,"max")) {
                    aggregate = REDIS_AGGR_MAX;
                } else {
                    zfree(src);
                    addReply(c,shared.syntaxerr);
                    return;
                }
                j++; remaining--;
            } else {
                zfree(src);
                addReply(c,shared.syntaxerr);
                return;
            }
        }
    }

    // 为所有 key 创建迭代器
    for (i = 0; i < setnum; i++)
        zuiInitIterator(&src[i]);

    /* sort sets from the smallest to largest, this will improve our
     * algorithm's performance */
    // 将所有集合按基数从小到大排列，提升算法性能
    qsort(src,setnum,sizeof(zsetopsrc),zuiCompareByCardinality);

    // 结果集合对象
    dstobj = createZsetObject();
    dstzset = dstobj->ptr;
    // 初始化 zval 变量
    memset(&zval, 0, sizeof(zval));

    // INTER 操作, O(N^3)
    if (op == REDIS_OP_INTER) {
        /* Skip everything if the smallest input is empty. */
        // 如果最小集合为空集，那么跳出
        // （小优化，如果输入里有至少一个空集，那么结果必将是空集）
        if (zuiLength(&src[0]) > 0) {
            /* Precondition: as src[0] is non-empty and the inputs are ordered
             * by size, all src[i > 0] are non-empty too. */
            // 取出第一个集合的元素
            // O(N^3)
            while (zuiNext(&src[0],&zval)) {
                double score, value;

                // 根据 weight 计算新的 score 值
                score = src[0].weight * zval.score;
                if (isnan(score)) score = 0;

                // 遍历所有输入集合，计算交集元素，并对元素的 score 值进行聚合
                // O(N^2)
                for (j = 1; j < setnum; j++) {
                    /* It is not safe to access the zset we are
                     * iterating, so explicitly check for equal object. */
                    // 如果同一个集合出现了两次，那么将同一个元素计算多聚合一次
                    if (src[j].subject == src[0].subject) {
                        value = zval.score*src[j].weight;
                        // O(1)
                        zunionInterAggregate(&score,value,aggregate);
                    // 查找集合中是否有相同元素
                    // 如果有就进行聚合, O(N)
                    } else if (zuiFind(&src[j],&zval,&value)) {
                        value *= src[j].weight;
                        // O(1)
                        zunionInterAggregate(&score,value,aggregate);
                    // 没有交集元素，跳出
                    } else {
                        break;
                    }
                }

                /* Only continue when present in every input. */
                // 如果前面的交集计算没有跳出，那么执行如下操作
                // O(N)
                if (j == setnum) {
                    // 取出 member
                    tmp = zuiObjectFromValue(&zval);
                    // 添加到 skiplist, O(N)
                    znode = zslInsert(dstzset->zsl,score,tmp);
                    incrRefCount(tmp); /* added to skiplist */
                    // 添加到字典, O(1)
                    dictAdd(dstzset->dict,tmp,&znode->score);
                    incrRefCount(tmp); /* added to dictionary */

                    if (tmp->encoding == REDIS_ENCODING_RAW)
                        if (sdslen(tmp->ptr) > maxelelen)
                            maxelelen = sdslen(tmp->ptr);
                }
            }
        }

    // ZUNIONSTORE 操作， O(N^4)
    } else if (op == REDIS_OP_UNION) {
        // 遍历所有集合
        // O(N^4)
        for (i = 0; i < setnum; i++) {
            if (zuiLength(&src[i]) == 0)
                continue;

            // 取出集合中的所有元素
            // O(N^3)
            while (zuiNext(&src[i],&zval)) {
                double score, value;

                /* Skip key when already processed */
                // 如果该元素已经存在于结果集，那么结束循环
                if (dictFind(dstzset->dict,zuiObjectFromValue(&zval)) != NULL)
                    continue;

                /* Initialize score */
                // 根据 weight 计算 score
                score = src[i].weight * zval.score;
                if (isnan(score)) score = 0;

                /* Because the inputs are sorted by size, it's only possible
                 * for sets at larger indices to hold this element. */
                // 遍历当前集合之后的所有集合，对 member 进行聚合
                // O(N^2)
                for (j = (i+1); j < setnum; j++) {
                    /* It is not safe to access the zset we are
                     * iterating, so explicitly check for equal object. */
                    // 两个集合相同, O(1)
                    if(src[j].subject == src[i].subject) {
                        value = zval.score*src[j].weight;
                        zunionInterAggregate(&score,value,aggregate);
                    // 查找元素, O(N)
                    } else if (zuiFind(&src[j],&zval,&value)) {
                        value *= src[j].weight;
                        zunionInterAggregate(&score,value,aggregate);
                    }
                }

                // 将结果保存到 zset 对象里
                tmp = zuiObjectFromValue(&zval);
                znode = zslInsert(dstzset->zsl,score,tmp);
                incrRefCount(zval.ele); /* added to skiplist */
                dictAdd(dstzset->dict,tmp,&znode->score);
                incrRefCount(zval.ele); /* added to dictionary */

                if (tmp->encoding == REDIS_ENCODING_RAW)
                    if (sdslen(tmp->ptr) > maxelelen)
                        maxelelen = sdslen(tmp->ptr);
            }
        }
    } else {
        redisPanic("Unknown operator");
    }

    // 清除所有迭代器
    for (i = 0; i < setnum; i++)
        zuiClearIterator(&src[i]);

    // 删除旧的 dstkey 
    if (dbDelete(c->db,dstkey)) {
        signalModifiedKey(c->db,dstkey);
        touched = 1;
        server.dirty++;
    }

    // 保存聚合结果到 dstkey
    if (dstzset->zsl->length) {
        /* Convert to ziplist when in limits. */
        if (dstzset->zsl->length <= server.zset_max_ziplist_entries &&
            maxelelen <= server.zset_max_ziplist_value)
                zsetConvert(dstobj,REDIS_ENCODING_ZIPLIST);

        dbAdd(c->db,dstkey,dstobj);
        addReplyLongLong(c,zsetLength(dstobj));
        if (!touched) signalModifiedKey(c->db,dstkey);
        server.dirty++;
    } else {
        decrRefCount(dstobj);
        addReply(c,shared.czero);
    }
    zfree(src);
}

void zunionstoreCommand(redisClient *c) {
    zunionInterGenericCommand(c,c->argv[1], REDIS_OP_UNION);
}

void zinterstoreCommand(redisClient *c) {
    zunionInterGenericCommand(c,c->argv[1], REDIS_OP_INTER);
}

/*
 * T = O(N)
 */
void zrangeGenericCommand(redisClient *c, int reverse) {
    robj *key = c->argv[1];
    robj *zobj;
    int withscores = 0;
    long start;
    long end;
    int llen;
    int rangelen;

    // 取出 start 和 end 参数
    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != REDIS_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != REDIS_OK)) return;

    // 取出 withscores 参数
    if (c->argc == 5 && !strcasecmp(c->argv[4]->ptr,"withscores")) {
        withscores = 1;
    } else if (c->argc >= 5) {
        addReply(c,shared.syntaxerr);
        return;
    }

    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptymultibulk)) == NULL
         || checkType(c,zobj,REDIS_ZSET)) return;

    /* Sanitize indexes. */
    llen = zsetLength(zobj);
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        addReply(c,shared.emptymultibulk);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    addReplyMultiBulkLen(c, withscores ? (rangelen*2) : rangelen);

    // ziplist 编码, O(N)
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        // 决定迭代的方向
        // 并指向第一个 member
        // O(1)
        if (reverse)
            eptr = ziplistIndex(zl,-2-(2*start));
        else
            eptr = ziplistIndex(zl,2*start);

        redisAssertWithInfo(c,zobj,eptr != NULL);
        // 指向第一个 score
        sptr = ziplistNext(zl,eptr);

        // 取出元素, O(N)
        while (rangelen--) {
            // 元素不为空？
            redisAssertWithInfo(c,zobj,eptr != NULL && sptr != NULL);
            // 取出 member 
            redisAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));
            if (vstr == NULL)
                addReplyBulkLongLong(c,vlong);
            else
                addReplyBulkCBuffer(c,vstr,vlen);

            // 取出 score
            if (withscores)
                addReplyDouble(c,zzlGetScore(sptr));

            // 移动指针到下一个节点
            if (reverse)
                zzlPrev(zl,&eptr,&sptr);
            else
                zzlNext(zl,&eptr,&sptr);
        }

    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        robj *ele;

        /* Check if starting point is trivial, before doing log(N) lookup. */
        // 决定起始节点, O(N)
        if (reverse) {
            ln = zsl->tail;
            if (start > 0)
                // O(N)
                ln = zslGetElementByRank(zsl,llen-start);
        } else {
            ln = zsl->header->level[0].forward;
            if (start > 0)
                // O(N)
                ln = zslGetElementByRank(zsl,start+1);
        }

        // O(N)
        while(rangelen--) {
            redisAssertWithInfo(c,zobj,ln != NULL);
            // 返回 member
            ele = ln->obj;
            addReplyBulk(c,ele);
            // 返回 score
            if (withscores)
                addReplyDouble(c,ln->score);
            // O(1)
            ln = reverse ? ln->backward : ln->level[0].forward;
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }
}

void zrangeCommand(redisClient *c) {
    zrangeGenericCommand(c,0);
}

void zrevrangeCommand(redisClient *c) {
    zrangeGenericCommand(c,1);
}

/* This command implements ZRANGEBYSCORE, ZREVRANGEBYSCORE. */
/*
 * T = O(N)
 */
void genericZrangebyscoreCommand(redisClient *c, int reverse) {
    zrangespec range;
    robj *key = c->argv[1];
    robj *zobj;
    long offset = 0, limit = -1;
    int withscores = 0;
    unsigned long rangelen = 0;
    void *replylen = NULL;
    int minidx, maxidx;

    /* Parse the range arguments. */
    if (reverse) {
        /* Range is given as [max,min] */
        maxidx = 2; minidx = 3;
    } else {
        /* Range is given as [min,max] */
        minidx = 2; maxidx = 3;
    }

    if (zslParseRange(c->argv[minidx],c->argv[maxidx],&range) != REDIS_OK) {
        addReplyError(c,"min or max is not a float");
        return;
    }

    /* Parse optional extra arguments. Note that ZCOUNT will exactly have
     * 4 arguments, so we'll never enter the following code path. */
    // O(1)
    if (c->argc > 4) {
        int remaining = c->argc - 4;
        int pos = 4;

        // 读入参数, O(N)
        while (remaining) {
            // withscores 参数
            if (remaining >= 1 && !strcasecmp(c->argv[pos]->ptr,"withscores")) {
                pos++; remaining--;
                withscores = 1;
            // limit 参数
            } else if (remaining >= 3 && !strcasecmp(c->argv[pos]->ptr,"limit")) {
                // offset 和 limit 参数
                if ((getLongFromObjectOrReply(c, c->argv[pos+1], &offset, NULL) != REDIS_OK) ||
                    (getLongFromObjectOrReply(c, c->argv[pos+2], &limit, NULL) != REDIS_OK)) return;
                pos += 3; remaining -= 3;
            } else {
                addReply(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* Ok, lookup the key and get the range */
    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptymultibulk)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    // O(N)
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        double score;

        /* If reversed, get the last node in range as starting point. */
        // O(N)
        if (reverse) {
            eptr = zzlLastInRange(zl,range);
        } else {
            eptr = zzlFirstInRange(zl,range);
        }

        /* No "first" element in the specified interval. */
        if (eptr == NULL) {
            addReply(c, shared.emptymultibulk);
            return;
        }

        /* Get score pointer for the first element. */
        redisAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        // O(N)
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        // O(N)
        while (eptr && limit--) {
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            // 超出范围，跳出
            if (reverse) {
                if (!zslValueGteMin(score,&range)) break;
            } else {
                if (!zslValueLteMax(score,&range)) break;
            }

            /* We know the element exists, so ziplistGet should always succeed */
            // 取出 member
            redisAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));

            rangelen++;
            if (vstr == NULL) {
                addReplyBulkLongLong(c,vlong);
            } else {
                addReplyBulkCBuffer(c,vstr,vlen);
            }

            // 取出 score
            if (withscores) {
                addReplyDouble(c,score);
            }

            /* Move to next node */
            // 移动到下一节点
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        // O(N)
        if (reverse) {
            ln = zslLastInRange(zsl,range);
        } else {
            ln = zslFirstInRange(zsl,range);
        }

        /* No "first" element in the specified interval. */
        if (ln == NULL) {
            addReply(c, shared.emptymultibulk);
            return;
        }

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addDeferredMultiBulkLength(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        // O(N)
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        // O(N)
        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            // 超出范围， 跳出
            if (reverse) {
                if (!zslValueGteMin(ln->score,&range)) break;
            } else {
                if (!zslValueLteMax(ln->score,&range)) break;
            }

            rangelen++;
            // 取出 member
            addReplyBulk(c,ln->obj);

            // 取出 score
            if (withscores) {
                addReplyDouble(c,ln->score);
            }

            /* Move to next node */
            // 移动到下一节点
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    if (withscores) {
        rangelen *= 2;
    }

    setDeferredMultiBulkLength(c, replylen, rangelen);
}

void zrangebyscoreCommand(redisClient *c) {
    genericZrangebyscoreCommand(c,0);
}

void zrevrangebyscoreCommand(redisClient *c) {
    genericZrangebyscoreCommand(c,1);
}

/*
 * 返回有序集在给定范围内的元素
 *
 * T = O(N)
 */
void zcountCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zrangespec range;
    int count = 0;

    /* Parse the range arguments */
    // range 参数
    if (zslParseRange(c->argv[2],c->argv[3],&range) != REDIS_OK) {
        addReplyError(c,"min or max is not a float");
        return;
    }

    /* Lookup the sorted set */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, REDIS_ZSET)) return;

    // O(N)
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        double score;

        /* Use the first element in range as the starting point */
        // 取出第一个符合范围的 member
        eptr = zzlFirstInRange(zl,range);

        /* No "first" element */
        // 没有符合范围的节点
        if (eptr == NULL) {
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        // 指向 score 域
        sptr = ziplistNext(zl,eptr);
        // 取出 score 值
        score = zzlGetScore(sptr);
        redisAssertWithInfo(c,zobj,zslValueLteMax(score,&range));

        /* Iterate over elements in range */
        // O(N)
        while (eptr) {
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            if (!zslValueLteMax(score,&range)) {
                break;
            } else {
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        // O(N)
        zn = zslFirstInRange(zsl, range);

        /* Use rank of first element, if any, to determine preliminary count */
        if (zn != NULL) {
            // O(N)
            rank = zslGetRank(zsl, zn->score, zn->obj);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            zn = zslLastInRange(zsl, range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->obj);
                // 找出第一个和最后一个符合范围的节点
                // 将它们的 rank 相减就是范围内的节点的数量
                count -= (zsl->length - rank);
            }
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }

    addReplyLongLong(c, count);
}

/*
 * 返回有序集的基数
 *
 * T = O(N)
 */
void zcardCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;

    if ((zobj = lookupKeyReadOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    addReplyLongLong(c,zsetLength(zobj));
}

/*
 * 找出给定元素的 score 值
 *
 * T = O(N)
 */
void zscoreCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *zobj;
    double score;

    if ((zobj = lookupKeyReadOrReply(c,key,shared.nullbulk)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;

    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        // O(N)
        if (zzlFind(zobj->ptr,c->argv[2],&score) != NULL)
            addReplyDouble(c,score);
        else
            addReply(c,shared.nullbulk);
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        dictEntry *de;

        // O(1)
        c->argv[2] = tryObjectEncoding(c->argv[2]);
        de = dictFind(zs->dict,c->argv[2]);
        if (de != NULL) {
            score = *(double*)dictGetVal(de);
            addReplyDouble(c,score);
        } else {
            addReply(c,shared.nullbulk);
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }
}

/*
 * 返回给定元素在有序集中的排位
 *
 * T = O(N)
 */
void zrankGenericCommand(redisClient *c, int reverse) {
    robj *key = c->argv[1];
    robj *ele = c->argv[2];
    robj *zobj;
    unsigned long llen;
    unsigned long rank;

    if ((zobj = lookupKeyReadOrReply(c,key,shared.nullbulk)) == NULL ||
        checkType(c,zobj,REDIS_ZSET)) return;
    llen = zsetLength(zobj);

    redisAssertWithInfo(c,ele,ele->encoding == REDIS_ENCODING_RAW);
    if (zobj->encoding == REDIS_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        eptr = ziplistIndex(zl,0);
        redisAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);
        redisAssertWithInfo(c,zobj,sptr != NULL);

        // 遍历指针，一路计算越过的节点数量
        rank = 1;
        while(eptr != NULL) {
            if (ziplistCompare(eptr,ele->ptr,sdslen(ele->ptr)))
                break;
            rank++;
            zzlNext(zl,&eptr,&sptr);
        }

        if (eptr != NULL) {
            if (reverse)
                addReplyLongLong(c,llen-rank);
            else
                addReplyLongLong(c,rank-1);
        } else {
            addReply(c,shared.nullbulk);
        }
    } else if (zobj->encoding == REDIS_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        dictEntry *de;
        double score;

        // O(1)
        ele = c->argv[2] = tryObjectEncoding(c->argv[2]);
        de = dictFind(zs->dict,ele);
        if (de != NULL) {
            score = *(double*)dictGetVal(de);
            // 查找元素在跳跃表中的位置 O(N)
            rank = zslGetRank(zsl,score,ele);
            redisAssertWithInfo(c,ele,rank); /* Existing elements always have a rank. */
            if (reverse)
                addReplyLongLong(c,llen-rank);
            else
                addReplyLongLong(c,rank-1);
        } else {
            addReply(c,shared.nullbulk);
        }
    } else {
        redisPanic("Unknown sorted set encoding");
    }
}

void zrankCommand(redisClient *c) {
    zrankGenericCommand(c, 0);
}

void zrevrankCommand(redisClient *c) {
    zrankGenericCommand(c, 1);
}
