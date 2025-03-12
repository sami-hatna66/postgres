#include "postgres.h"
#include "storage/buf_internals.h"

/*
 * buffer tag / buffer index pair
*/
typedef struct 
{
	int tag;
	int bufferDescIndex;  // buf_id
} S3RingBufferEntry;

/*
 * Data structure for S3-FIFO probationary and main queues
*/
typedef struct 
{
	slock_t ring_buffer_lock;
	int head;
	int tail;
	int currentSize;
	int maxSize;
	S3RingBufferEntry queue[FLEXIBLE_ARRAY_MEMBER];
} S3RingBuffer;

/*
 * Data structure for returning the result of querying a page's reference and usage count
*/
typedef struct 
{
	int refCount;
	int usageCount;
} ReferenceUsagePair;

/*
 * Checks if ring buffer is full
*/
static inline bool S3RingBuffer_IsFull(S3RingBuffer* rb) {
	return rb->currentSize == rb->maxSize;
}

/*
 * Checks if ring buffer is empty
*/
static inline bool S3RingBuffer_IsEmpty(S3RingBuffer* rb) {
	return rb->currentSize == 0;
}

/*
 * Adds a new element to the ring buffer head
 * If an item has to be evicted to make space, returns that item
 * Else returns {-1, -1}
 * If resultPtr is not null, assign resultPtr to newly inserted entry
*/
static S3RingBufferEntry S3RingBuffer_Enqueue(S3RingBuffer* rb, S3RingBufferEntry newEntry, S3RingBufferEntry** resultPtr) {
	S3RingBufferEntry victim = {.tag = -1, .bufferDescIndex = -1};

	if (S3RingBuffer_IsFull(rb)) {
		victim.tag = rb->queue[rb->tail].tag;
		victim.bufferDescIndex = rb->queue[rb->tail].bufferDescIndex;
		rb->tail = (rb->tail + 1) % rb->maxSize;
	} else {
		rb->currentSize++;
	}

	rb->queue[rb->head] = newEntry;

	if (resultPtr != NULL) {
		*resultPtr = &(rb->queue[rb->head]);
	}

	rb->head = (rb->head + 1) % rb->maxSize;

	return victim;
}

/*
 * Pops and returns an element from the ring buffer tail
 * If ring buffer is empty, return {-1, -1}
*/
static S3RingBufferEntry S3RingBuffer_Dequeue(S3RingBuffer* rb) {
    S3RingBufferEntry dequeued = {.tag = -1, .bufferDescIndex = -1};

    if (S3RingBuffer_IsEmpty(rb)) {
        return dequeued;
    }

    dequeued.tag = rb->queue[rb->tail].tag;
    dequeued.bufferDescIndex = rb->queue[rb->tail].bufferDescIndex;

    rb->tail = (rb->tail + 1) % rb->maxSize;

    rb->currentSize--;

    return dequeued;
}

typedef enum {
	RB_BY_TAG,
	RB_BY_INDEX
} RingBufferOperationMode;

/*
 * Search ringbuffer for buffer index or tag (denoted by parameter mode)
 * Returns index of result in rb->queue or -1 if not found
*/
static int S3RingBuffer_Search(S3RingBuffer* rb, int target, RingBufferOperationMode mode) {
	if (S3RingBuffer_IsEmpty(rb)) return -1;

	int count = rb->currentSize;
	int idx = rb->head;
	while (count > 0) {
		if ((mode == RB_BY_TAG && rb->queue[idx].tag == target) ||
			(mode == RB_BY_INDEX && rb->queue[idx].bufferDescIndex == target)) {
			return idx;
		}
		idx = (idx + 1) % rb->maxSize;
		count--;
	}
	return -1;
}

/*
 * Deletes entry from ring buffer based on buffer index or tag (denoted by parameter mode)
 * Returns true if item was present and deleted, returns false if index not found
*/
static bool S3RingBuffer_Delete(S3RingBuffer* rb, int target, RingBufferOperationMode mode) {
	if (S3RingBuffer_IsEmpty(rb)) return false;

	int idx = S3RingBuffer_Search(rb, target, mode);
	if (idx == -1) return false;

	int current = idx;
	int next = (idx + 1) % rb->maxSize;
	while (current != rb->head) {
		rb->queue[current] = rb->queue[next];
		current = next;
		next = (next + 1) % rb->maxSize;
	}

	rb->head = (rb->head + rb->maxSize - 1) % rb->maxSize;
	rb->currentSize--;

    // Handle edge case of single element
	if (rb->currentSize == 0) {
		rb->head = 0;
		rb->tail = 0;
	}

	return true;
}

/*
 * Get reference and usage count of the page at idx
 * Returns pair of values as a ReferenceUsagePair struct
*/
static ReferenceUsagePair GetRefUsageCount(int idx) {
	BufferDesc* newBuf = GetBufferDescriptor(idx);
	uint32 newBufState = LockBufHdr(newBuf);
	ReferenceUsagePair result;
	result.refCount = BUF_STATE_GET_REFCOUNT(newBufState);
	result.usageCount = BUF_STATE_GET_USAGECOUNT(newBufState);
	UnlockBufHdr(newBuf, newBufState);
	return result;
}

/*
 * Decrement the usage count of the page at idx
*/
static void DecrementUsageCount(int idx) {
	BufferDesc* newBuf = GetBufferDescriptor(idx);
	uint32 newBufState = LockBufHdr(newBuf);
	if (BUF_STATE_GET_USAGECOUNT(newBufState) > 0) newBufState -= BUF_USAGECOUNT_ONE;
	UnlockBufHdr(newBuf, newBufState);
}

/*
 * Set the usage count of the page at idx to zero
*/
static void ZeroUsageCount(int idx) {
	BufferDesc* newBuf = GetBufferDescriptor(idx);
	uint32 newBufState = LockBufHdr(newBuf);
	while (BUF_STATE_GET_USAGECOUNT(newBufState) > 0) {
		newBufState -= BUF_USAGECOUNT_ONE;
	}
	UnlockBufHdr(newBuf, newBufState);
}

/*
 * Performs FIFO-Reinsertion on the given ring buffer, 
 * using the given tag and idx as the first to insert
 * Returns the buffer descriptor index held by the evicted entry
 * If resultPtr is not null, assign resultPtr to newly inserted firstEntry
*/
static int FifoReinsert(S3RingBuffer* rb, S3RingBufferEntry firstEntry, S3RingBufferEntry** resultPtr) {	
	S3RingBufferEntry newEntry = firstEntry;
	int bufferSize = rb->maxSize;
	bool isFirstIter = true;
	while (bufferSize > 0) {
		S3RingBufferEntry enqueueResult = S3RingBuffer_Enqueue(rb, newEntry, isFirstIter ? resultPtr : NULL);
		if (newEntry.bufferDescIndex != -1) DecrementUsageCount(newEntry.bufferDescIndex);
		if (enqueueResult.tag == -1 && enqueueResult.bufferDescIndex == -1) break;
		isFirstIter = false;
		
		ReferenceUsagePair metrics;
		if (enqueueResult.bufferDescIndex != -1) {
			metrics = GetRefUsageCount(enqueueResult.bufferDescIndex);
		} else if (enqueueResult.tag == firstEntry.tag) {
			// reinsert if not yet assigned an index (edge case)
			metrics.refCount = 1;
			metrics.usageCount = 1;
		}

		if (metrics.refCount > 0 || metrics.usageCount > 0) {
			newEntry = enqueueResult;
		} else {
			newEntry = enqueueResult;
			break;
		}
		bufferSize--;
	}

	if (newEntry.bufferDescIndex == -1) elog(ERROR, "S3-FIFO: No unpinned buffers available");

	return newEntry.bufferDescIndex;
}




/*
 * Ghost ring buffer is used for the ghost queue which only
 * needs to store buffer tags, not indexes
*/
typedef struct 
{
	slock_t ring_buffer_lock;
	int head;
	int tail;
	int currentSize;
	int maxSize;
	int queue[FLEXIBLE_ARRAY_MEMBER];
} S3GhostRingBuffer;

static inline bool GhostRingBuffer_IsFull(S3GhostRingBuffer* rb) {
    return rb->currentSize == rb->maxSize;
}

static inline bool GhostRingBuffer_IsEmpty(S3GhostRingBuffer* rb) {
    return rb->currentSize == 0;
}

static inline int GhostRingBuffer_Search(S3GhostRingBuffer* rb, int target) {
    if (GhostRingBuffer_IsEmpty(rb)) return -1;

    int count = rb->currentSize;
    int idx = rb->tail;
    while (count > 0) {
        if (rb->queue[idx] == target) return idx;
        idx = (idx + 1) % rb->maxSize;
        count--;
    }
    return -1;
}

static bool GhostRingBuffer_Delete(S3GhostRingBuffer* rb, int target) {
    if (GhostRingBuffer_IsEmpty(rb)) return false;

    int idx = GhostRingBuffer_Search(rb, target);
    if (idx == -1) return false;

    int current = idx;
    int next = (idx + 1) % rb->maxSize;
    while (current != rb->head) {
        rb->queue[current] = rb->queue[next];
        current = next;
        next = (next + 1) % rb->maxSize;
    }

    rb->head = (rb->head + rb->maxSize - 1) % rb->maxSize;
    rb->currentSize--;

    if (rb->currentSize == 0) {
        rb->head = 0;
        rb->tail = 0;
    }

    return true;
}

static int GhostRingBuffer_Enqueue(S3GhostRingBuffer* rb, int value) {
    int victim = -1;

    if (GhostRingBuffer_IsFull(rb)) {
        victim = rb->queue[rb->tail];
        rb->tail = (rb->tail + 1) % rb->maxSize;
    } else {
        rb->currentSize++;
    }

    rb->queue[rb->head] = value;
    rb->head = (rb->head + 1) % rb->maxSize;

    return victim;
}