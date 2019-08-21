<?php

namespace Amp\Redis\Mutex;

use Amp\Failure;
use Amp\Loop;
use Amp\Promise;
use Amp\Redis\Client;
use function Amp\call;
use function Amp\Promise\all;

/**
 * Mutex can be used to create locks for mutual exclusion in distributed clients.
 *
 * @author Niklas Keller <me@kelunik.com>
 */
final class Mutex
{
    private const LOCK = <<<LOCK
if redis.call("llen",KEYS[1]) > 0 and redis.call("ttl",KEYS[1]) >= 0 then
    return redis.call("lindex",KEYS[1],0) == ARGV[1]
elseif redis.call("ttl",KEYS[1]) == -1 then
    redis.call("pexpire",KEYS[1],ARGV[2])
    return 0
else
    redis.call("del",KEYS[1])
    redis.call("lpush",KEYS[1],ARGV[1])
    redis.call("pexpire",KEYS[1],ARGV[2])
    return 1
end
LOCK;

    private const TOKEN = <<<TOKEN
if redis.call("lindex",KEYS[1],0) == "%" then
    redis.call("del",KEYS[1])
    redis.call("lpush",KEYS[1],ARGV[1])
    redis.call("pexpire",KEYS[1],ARGV[2])
    return 1
else
    return {err="Redis lock error"}
end
TOKEN;

    private const UNLOCK = <<<UNLOCK
if redis.call("lindex",KEYS[1],0) == ARGV[1] then
    redis.call("del",KEYS[1])
    redis.call("lpush",KEYS[2],"%")
    redis.call("pexpire",KEYS[2],ARGV[2])
    return redis.call("llen",KEYS[2])
end
UNLOCK;

    private const RENEW = <<<RENEW
if redis.call("lindex",KEYS[1],0) == ARGV[1] then
    return redis.call("pexpire",KEYS[1],ARGV[2])
else
    return 0
end
RENEW;

    /** @var string */
    private $uri;
    /** @var array */
    private $options;
    /** @var Client */
    private $std;
    /** @var array */
    private $busyConnectionMap;
    /** @var Client[] */
    private $busyConnections;
    /** @var Client[] */
    private $readyConnections;
    /** @var int */
    private $maxConnections;
    /** @var int */
    private $ttl;
    /** @var int */
    private $timeout;
    /** @var string */
    private $watcher;

    /**
     * Constructs a new Mutex instance. A single instance can be used to create as many locks as you need.
     *
     * @param string     $uri URI of the Redis server instance, e.g. tcp://localhost:6379
     * @param array      $options {
     *      General options for this instance.
     *
     * @type string|null $password password for the Redis server
     * @type int         $max_connections maximum of concurrent Redis connections waiting for a lock with blocking
     *     commands
     * @type int timeout timeout for blocking lock wait
     * @type int         $ttl key ttl for created locks and lock renews
     * }
     */
    public function __construct(string $uri, array $options = [])
    {
        $this->uri = $uri;
        $this->options = $options;

        $this->std = new Client($uri);
        $this->maxConnections = $options["max_connections"] ?? 0;
        $this->ttl = $options["ttl"] ?? 1000;
        $this->timeout = (int) (($options["timeout"] ?? 1000) / 1000);
        $this->readyConnections = [];
        $this->busyConnections = [];
        $this->busyConnectionMap = [];

        $readyConnections = &$this->readyConnections;
        $this->watcher = Loop::repeat(5000, static function () use (&$readyConnections) {
            $now = \time();
            $unused = $now - 60;

            foreach ($readyConnections as $key => [$time, $connection]) {
                if ($time > $unused) {
                    break;
                }

                unset($readyConnections[$key]);
                $connection->close();
            }
        });

        Loop::unreference($this->watcher);
    }

    public function __destruct()
    {
        $this->shutdown();
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
            $result = yield $this->std->eval(self::LOCK, ["lock:{$id}", "queue:{$id}"], [$token, $this->ttl]);

            if ($result) {
                return true;
            }

            yield $this->std->expire("queue:{$id}", $this->timeout * 2, true);

            $connection = $this->getReadyConnection();

            try {
                $result = yield $connection->brPoplPush("queue:{$id}", "lock:{$id}", $this->timeout);

                if ($result === null) {
                    return new Failure(new LockException);
                }

                return $this->std->eval(self::TOKEN, ["lock:{$id}"], [$token, $this->ttl]);
            } finally {
                $hash = spl_object_hash($connection);
                $key = $this->busyConnectionMap[$hash] ?? null;
                unset($this->busyConnections[$key], $this->busyConnectionMap[$hash]);
                $this->readyConnections[] = [time(), $connection];
            }
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
     * @return Promise promise fails if lock couldn't be acquired, otherwise resolves normally.
     */
    public function unlock(string $id, string $token): Promise
    {
        return $this->std->eval(self::UNLOCK, ["lock:{$id}", "queue:{$id}"], [$token, 2 * $this->timeout]);
    }

    /**
     * Renews a lock to extend its validity.
     *
     * Without renewing a lock, other clients may detect this client as stale and acquire a lock.
     *
     * @param string $id specific lock ID.
     * @param string $token unique token provided during {@link lock()}.
     *
     * @return Promise promise fails if lock couldn't be renewed, otherwise resolves normally.
     */
    public function renew(string $id, string $token): Promise
    {
        return $this->std->eval(self::RENEW, ["lock:{$id}"], [$token, $this->ttl]);
    }

    /**
     * Shut down the mutex client.
     *
     * Be sure to release all locks you acquired before, so other clients will be able to acquire them.
     *
     * @return Promise
     */
    public function shutdown(): Promise
    {
        Loop::cancel($this->watcher);

        $promises = [$this->std->close()];

        foreach ($this->busyConnections as $connection) {
            $promises[] = $connection->close();
        }

        foreach ($this->readyConnections as [$time, $connection]) {
            $promises[] = $connection->close();
        }

        return all($promises);
    }

    /**
     * Gets the instance's TTL set in the constructor's {@code $options}.
     *
     * @return int TTL in milliseconds.
     */
    public function getTtl(): int
    {
        return $this->ttl;
    }

    /**
     * Gets the instance's timeout set in the constructor's {@code $options}.
     *
     * @return int timeout in milliseconds.
     */
    public function getTimeout(): int
    {
        return $this->timeout;
    }

    /**
     * Get a ready connection instance.
     *
     * If possible, uses the most recent connection that's no longer used, so older connections will be closed when not
     * used anymore. If there's no connection available, it creates a new one as long as the max. connections limit
     * allows it.
     *
     * @return Client ready connection to be used for blocking commands.
     *
     * @throws ConnectionLimitException if there's no ready connection and no new connection could be created because of
     * a max. connections limit.
     */
    protected function getReadyConnection(): Client
    {
        $connection = array_pop($this->readyConnections);
        $connection = $connection[1] ?? null;

        if (!$connection) {
            if ($this->maxConnections && count($this->busyConnections) + 1 === $this->maxConnections) {
                throw new ConnectionLimitException;
            }

            $connection = new Client(
                $this->uri
            );
        }

        $this->busyConnections[] = $connection;
        end($this->busyConnections);

        $hash = spl_object_hash($connection);
        $this->busyConnectionMap[$hash] = key($this->busyConnections);

        return $connection;
    }
}
