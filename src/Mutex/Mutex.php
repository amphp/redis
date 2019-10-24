<?php

namespace Amp\Redis\Mutex;

use Amp\Loop;
use Amp\Promise;
use Amp\Redis\QueryExecutorFactory;
use Amp\Redis\Redis;
use Amp\Redis\RedisException;
use Amp\Sync\KeyedMutex;
use Amp\Sync\Lock;
use Psr\Log\LoggerInterface as PsrLogger;
use Psr\Log\NullLogger;
use function Amp\call;
use function Amp\delay;

/**
 * Mutex can be used to create locks for mutual exclusion in distributed clients.
 *
 * @author Niklas Keller <me@kelunik.com>
 */
final class Mutex implements KeyedMutex
{
    private const LOCK = <<<LOCK
local lock = KEYS[1]
local queue = KEYS[2]

local token = ARGV[1]
local ttl = ARGV[2]
local queueTtl = ARGV[3]

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
            redis.call("pexpire", queue, queueTtl)

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
            redis.call("pexpire", queue, queueTtl)

            return -1 - i
        end
    end

    redis.call("rpush", queue, token)
    redis.call("pexpire", queue, queueTtl)

    return -1 - redis.call("llen", queue)
end
LOCK;

    private const UNLOCK = <<<UNLOCK
local lock = KEYS[1]
local token = ARGV[1]

if redis.call("get", lock) == token then
    redis.call("del", lock)
    return 1
else
    return 2
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
    /** @var PsrLogger */
    private $logger;

    private $numberOfLocks = 0;
    private $numberOfAttempts = 0;

    /**
     * Constructs a new Mutex instance. A single instance can be used to create as many locks as you need.
     *
     * @param QueryExecutorFactory $queryExecutorFactory
     * @param MutexOptions|null    $options
     */
    public function __construct(
        QueryExecutorFactory $queryExecutorFactory,
        ?MutexOptions $options = null,
        ?PsrLogger $logger = null
    ) {
        $this->options = $options ?? new MutexOptions;
        $this->sharedConnection = new Redis($queryExecutorFactory->createQueryExecutor());
        $this->logger = $logger ?? new NullLogger;
    }

    public function __destruct()
    {
        if (isset($this->watcher)) {
            Loop::cancel($this->watcher);
        }
    }

    /**
     * Acquires a lock.
     *
     * If directly acquiring a lock fails, the client is placed in a queue and reattempts to lock the key. If a client
     * crashes or doesn't free the lock while not renewing it, the lock will expire and the next client in the queue
     * will be able to acquire it.
     *
     * @param string $key Lock key.
     *
     * @return Promise<Lock> Resolves to an instance of `Lock`.
     */
    public function acquire(string $key): Promise
    {
        return call(function () use ($key) {
            $this->numberOfLocks++;

            $token = \base64_encode(\random_bytes(16));
            $prefix = $this->options->getKeyPrefix();
            $timeLimit = \microtime(true) * 1000 + $this->options->getLockTimeout();
            $attempts = 0;

            do {
                $attempts++;
                $this->numberOfAttempts++;

                $result = yield $this->sharedConnection->eval(
                    self::LOCK,
                    ["{$prefix}lock:{$key}", "{$prefix}lock-queue:{$key}"],
                    [$token, $this->options->getLockExpiration(), $this->options->getLockExpiration() + $this->options->getLockTimeout()]
                );

                if ($result < 1) {
                    if ($attempts > 2 && \microtime(true) * 1000 > $timeLimit) {
                        // In very rare cases we might not get the lock, but are at the head of the queue and another
                        // client moves us into the lock position. Deleting the token from the queue and afterwards
                        // unlocking solves this. No yield required, because we use the same connection.
                        $this->sharedConnection->getList("{$prefix}lock-queue:{$key}")->remove($token);
                        Promise\rethrow($this->unlock($key, $token));

                        throw new LockException('Failed to acquire lock for ' . $key . ' within ' . $this->options->getLockTimeout() . ' ms');
                    }

                    // A negative integer as reply means we're still in the queue and indicates the queue position.
                    // Making the timing dependent on the queue position greatly reduces CPU usage and locking attempts.
                    yield delay(5 + \min((-$result - 1) * 10, 300));
                }
            } while ($result < 1);

            if (empty($this->locks)) {
                $this->createRenewWatcher();
            }

            $this->locks[$key . ' @ ' . $token] = [$key, $token];

            return new Lock(0, function () use ($key, $token) {
                Promise\rethrow($this->unlock($key, $token));
            });
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

    /**
     * Unlocks a previously acquired lock.
     *
     * @param string $key Lock key.
     * @param string $token Unique token generated during {@link lock()}.
     *
     * @return Promise Fails if lock couldn't be unlocked, otherwise resolves normally.
     */
    private function unlock(string $key, string $token): Promise
    {
        return call(function () use ($key, $token) {
            // Unset before unlocking, as we don't want to renew the lock anymore
            // If something goes wrong, the lock will simply expire
            unset($this->locks[$key . ' @ ' . $token]);

            if (empty($this->locks) && $this->watcher !== null) {
                Loop::cancel($this->watcher);
                $this->watcher = null;
            }

            $prefix = $this->options->getKeyPrefix();

            for ($attempt = 0; $attempt < 2; $attempt++) {
                try {
                    $result = yield $this->sharedConnection->eval(
                        self::UNLOCK,
                        ["{$prefix}lock:{$key}"],
                        [$token]
                    );

                    if ($result === 2) {
                        $this->logger->warning('Lock was already expired when unlocked', [
                            'key' => $key,
                        ]);
                    }

                    break;
                } catch (RedisException $e) {
                    $this->logger->error('Unlock operation failed on attempt ' . ($attempt + 1), [
                        'exception' => $e,
                    ]);
                }
            }
        });
    }

    private function createRenewWatcher(): void
    {
        $this->watcher = Loop::repeat($this->options->getLockRenewInterval(), function () {
            \assert(!empty($this->locks));

            $keys = [];
            $arguments = [$this->options->getLockExpiration()];

            $prefix = $this->options->getKeyPrefix();

            foreach ($this->locks as [$key, $token]) {
                $keys[] = "{$prefix}lock:{$key}";
                $arguments[] = $token;
            }

            try {
                yield $this->sharedConnection->eval(self::RENEW, $keys, $arguments);
            } catch (RedisException $e) {
                $this->logger->error('Renew operation failed, locks might expire', [
                    'exception' => $e,
                ]);
            }
        });
    }
}
