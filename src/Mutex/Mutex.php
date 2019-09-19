<?php

namespace Amp\Redis\Mutex;

use Amp\Loop;
use Amp\Promise;
use Amp\Redis\QueryExecutorFactory;
use Amp\Redis\Redis;
use function Amp\call;
use function Amp\delay;

/**
 * Mutex can be used to create locks for mutual exclusion in distributed clients.
 *
 * @author Niklas Keller <me@kelunik.com>
 */
final class Mutex
{
    private const LOCK = <<<LOCK
local lock = KEYS[1]
local queue = KEYS[2]

local token = ARGV[1]
local ttl = ARGV[2]

if redis.call("exists", lock) == 0 then
    if redis.call("llen", queue) == 0 then
        redis.call("set", lock, token, "px", ttl)
        return 1
    else
        local queued_tokens = redis.call("lrange", queue, 0, -1)
        local push = 1
        local position = 0
        
        for i=1,#queued_tokens do
            if queued_tokens[i] == token then
                push = 0
                position = i
                break
            end
        end

        if push == 1 then
            redis.call("rpush", queue, token)
        end

        local queued = redis.call("lpop", queue)
        redis.call("set", lock, queued, "px", ttl)
        if queued == token then
            return 2
        else
            return -1 - position
        end
    end
else
    if redis.call("get", lock) == token then
        redis.call("set", lock, token, "px", ttl)
        return 1
    end

    local queued_tokens = redis.call("lrange", queue, 0, -1)
    for i=1,#queued_tokens do
        if queued_tokens[i] == token then
            return -1 - i
        end
    end

    redis.call("rpush", queue, token)

    return -1 - redis.call("llen", queue)
end
LOCK;

    private const UNLOCK = <<<UNLOCK
local lock = KEYS[1]
local queue = KEYS[2]

local token = ARGV[1]

if redis.call("get", lock) == token then
    redis.call("del", lock)
    if redis.call("llen", queue) == 0 then
        return 1
    else
        return 2
    end
else
    return 3
end
UNLOCK;

    private const RENEW = <<<RENEW
for i=1,#KEYS do
    if redis.call("get", KEYS[i]) == ARGV[i + 1] then
        redis.call("pexpire", KEYS[i], ARGV[1])
    end
end
RENEW;

    /** @var MutexOptions */
    private $options;
    /** @var Redis */
    private $sharedConnection;
    /** @var array[] */
    private $locks;
    /** @var string */
    private $watcher;

    private $numberOfLocks = 0;
    private $numberOfAttempts = 0;

    /**
     * Constructs a new Mutex instance. A single instance can be used to create as many locks as you need.
     *
     * @param QueryExecutorFactory $queryExecutorFactory
     * @param MutexOptions|null    $options
     */
    public function __construct(QueryExecutorFactory $queryExecutorFactory, ?MutexOptions $options = null)
    {
        $this->options = $options ?? new MutexOptions;
        $this->sharedConnection = new Redis($queryExecutorFactory->createQueryExecutor());
    }

    public function __destruct()
    {
        if (isset($this->watcher)) {
            Loop::cancel($this->watcher);
        }
    }

    /**
     * Tries to acquire a lock.
     *
     * If acquiring a lock fails, it uses a blocking connection waiting for the current client holding the lock to free
     * it. If the other client crashes or doesn't free the lock, the returned promise will fail, because once having
     * entered the blocking mode, it doesn't try to acquire a lock until another client frees the current lock. It can't
     * react to key expires. You can call this method once again if you absolutely need it, but usually, it should only
     * be required if another client misbehaves or crashes, which is clearly a bug then.
     *
     * @param string $id specific lock ID (every lock has its own ID).
     * @param string $token unique token (only has to be unique within other locking attempts with the same lock ID).
     *
     * @return Promise Fails if lock couldn't be acquired, otherwise resolves to `true`.
     */
    public function lock(string $id, string $token): Promise
    {
        return call(function () use ($id, $token) {
            $this->numberOfLocks++;

            do {
                $this->numberOfAttempts++;

                $result = yield $this->sharedConnection->eval(
                    self::LOCK,
                    ["lock:{$id}", "queue:{$id}"],
                    [$token, $this->options->getLockExpiration()]
                );

                if ($result < 1) {
                    // A negative integer as reply means we're still in the queue and indicates the queue position.
                    // Making the timing dependent on the queue position greatly reduces CPU usage and locking attempts.
                    yield delay(5 + \min((-$result - 1) * 10, 300));
                }
            } while ($result < 1);

            if (empty($this->locks)) {
                $this->createRenewWatcher();
            }

            $this->locks[$id . ' @ ' . $token] = [$id, $token];

            return true;
        });
    }

    /**
     * Unlocks a previously acquired lock.
     *
     * You need to provide the same parameters for this method as you did for {@link lock()}.
     *
     * @param string $id specific lock ID.
     * @param string $token unique token provided during {@link lock()}.
     *
     * @return Promise Fails if lock couldn't be unlocked, otherwise resolves normally.
     */
    public function unlock(string $id, string $token): Promise
    {
        return call(function () use ($id, $token) {
            // Unset before unlocking, as we don't want to renew the lock anymore
            // If something goes wrong, the lock will simply expire
            unset($this->locks[$id . ' @ ' . $token]);

            if (empty($this->locks) && $this->watcher !== null) {
                Loop::cancel($this->watcher);
                $this->watcher = null;
            }

            yield $this->sharedConnection->eval(
                self::UNLOCK,
                ["lock:{$id}", "queue:{$id}"],
                [$token]
            );
        });
    }

    public function getNumberOfAttempts(): int
    {
        return $this->numberOfAttempts;
    }

    public function getNumberOfLocks(): int
    {
        return $this->numberOfLocks;
    }

    public function resetStatistics(): void
    {
        $this->numberOfAttempts = 0;
        $this->numberOfLocks = 0;
    }

    private function createRenewWatcher(): void
    {
        $this->watcher = Loop::repeat($this->options->getLockRenewInterval(), function () {
            if (empty($this->locks)) {
                return;
            }

            $lockKeys = [];
            $lockTokens = [$this->options->getLockExpiration()];

            foreach ($this->locks as [$lockKey, $lockToken]) {
                $lockKeys[] = $lockKey;
                $lockTokens[] = $lockToken;
            }

            $this->sharedConnection->eval(self::RENEW, $lockKeys, $lockTokens);
        });
    }
}
