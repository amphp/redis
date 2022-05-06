# Mutex Design

This file explains the algorithm design behind the lock implementation in this package.

## Goals

 - Mutual exclusion: Only one client can hold the lock at any given time
 - Liveness / deadlock free: Another client may eventually acquire the lock, even if the client holding the lock crashes or becomes unresponsive, e.g. due to a network partition
 - Fairness: The clients are given the locks in the same order as they requested them to prevent possible starvation

## Limitations

 - Unlike the [Redlock](https://redis.io/topics/distlock) algorithm, this algorithm is designed to work on a single Redis instance only
 - To guarantee the liveness property, another client might eventually acquire the lock while the original client holding the lock is still alive but unresponsive, leading to two or more clients concurrently owning the lock

## Implementation

 - `lock:<key>` holds the token of the current lock holder, the token ensures the lock can be freed safely
 - `lock-queue:<key>` holds a list of waiting client tokens
 - `lock-notify:<token>` is used for notifications to waiting clients, which execute `blpop` and will pick up any value pushed to such a key
 - `lock:<key>` expires after `<lock-time>`, so a stale client holding a lock won't hold it indefinitely

### Locking

The following operations are assumed to be executed atomically, i.e. either via transactions or Redis scripting.

 - If key `lock:123` is empty
    - If key `lock-queue:123` is empty
        - Set key `lock:123` to value `<token>` with a TTL of `<lock-time>`
        - Return success
    - Else
        - If key `lock-queue:123` does not contain `<token>`
            - Execute `rpush lock-queue:123 <token>`
        - Remove head `<queued>` of key `lock-queue:123`
        - Set key `lock:123` to value `<queued>` with a TTL of `<lock-time>`
        - If `<queued> == <token>`
            - Return success
        - Else
            - Push a dummy value to key `lock-notify:<queued>`
            - Execute `del lock-notify:<token>`
            - Execute `blpop lock-notify:<token> <lock-time>`
            - Restart from beginning
 - Else
    - Execute `del lock-notify:<token>`
    - Execute `blpop lock-notify:<token> <lock-time>`
    - Restart from beginning

### Unlocking

The following operations are assumed to be executed atomically, i.e. either via transactions or Redis scripting.

 - If key `lock:123` has value `<token>`
    - Delete key `lock:123`
    - If key `lock-queue:123` is empty
        - Return success
    - Else
        - Push a dummy value to key `lock-notify:<queued>`
        - Return success
 - Else
    - Return success
