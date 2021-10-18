<?php /** @noinspection DuplicatedCode */

namespace Amp\Redis;

use Amp\Iterator;
use Amp\Producer;
use Amp\Promise;
use function Amp\call;

final class Redis
{
    /** @var QueryExecutor */
    private $queryExecutor;

    /** @var string[] */
    private $evalCache = [];

    public function __construct(QueryExecutor $queryExecutor)
    {
        $this->queryExecutor = $queryExecutor;
    }

    public function getHyperLogLog(string $key): RedisHyperLogLog
    {
        return new RedisHyperLogLog($this->queryExecutor, $key);
    }

    public function getList(string $key): RedisList
    {
        return new RedisList($this->queryExecutor, $key);
    }

    public function getMap(string $key): RedisMap
    {
        return new RedisMap($this->queryExecutor, $key);
    }

    public function getSet(string $key): RedisSet
    {
        return new RedisSet($this->queryExecutor, $key);
    }

    public function getSortedSet(string $key): RedisSortedSet
    {
        return new RedisSortedSet($this->queryExecutor, $key);
    }

    /**
     * @param string $arg
     * @param string ...$args
     *
     * @return Promise
     */
    public function query(string $arg, string ...$args): Promise
    {
        return $this->queryExecutor->execute(\array_merge([$arg], $args));
    }

    /**
     * @param string $key
     * @param string ...$keys
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/del
     */
    public function delete(string $key, string ...$keys): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['del', $key], $keys));
    }

    /**
     * @param string $key
     *
     * @return Promise<string>
     *
     * @link https://redis.io/commands/dump
     */
    public function dump(string $key): Promise
    {
        return $this->queryExecutor->execute(['dump', $key]);
    }

    /**
     * @param string $key
     *
     * @return Promise<bool>
     *
     * @link https://redis.io/commands/exists
     */
    public function has(string $key): Promise
    {
        return $this->queryExecutor->execute(['exists', $key], toBool);
    }

    /**
     * @param string $key
     * @param int    $seconds
     *
     * @return Promise<bool>
     *
     * @link https://redis.io/commands/expire
     */
    public function expireIn(string $key, int $seconds): Promise
    {
        return $this->queryExecutor->execute(['expire', $key, $seconds], toBool);
    }

    /**
     * @param string $key
     * @param int    $millis
     *
     * @return Promise<bool>
     *
     * @link https://redis.io/commands/pexpire
     */
    public function expireInMillis(string $key, int $millis): Promise
    {
        return $this->queryExecutor->execute(['pexpire', $key, $millis], toBool);
    }

    /**
     * @param string $key
     * @param int    $timestamp
     *
     * @return Promise<bool>
     *
     * @link https://redis.io/commands/expireat
     */
    public function expireAt(string $key, int $timestamp): Promise
    {
        return $this->queryExecutor->execute(['expireat', $key, $timestamp], toBool);
    }

    /**
     * @param string $key
     * @param int    $timestamp
     *
     * @return Promise<bool>
     *
     * @link https://redis.io/commands/pexpireat
     */
    public function expireAtMillis(string $key, int $timestamp): Promise
    {
        return $this->queryExecutor->execute(['pexpireat', $key, $timestamp], toBool);
    }

    /**
     * @param string $pattern
     *
     * @return Promise<array>
     *
     * @link https://redis.io/commands/keys
     *
     * @see Redis::scan()
     */
    public function getKeys(string $pattern = '*'): Promise
    {
        return $this->queryExecutor->execute(['keys', $pattern]);
    }

    /**
     * @param string $key
     * @param int    $db
     *
     * @return Promise<bool>
     *
     * @link https://redis.io/commands/move
     */
    public function move(string $key, int $db): Promise
    {
        return $this->queryExecutor->execute(['move', $key, $db], toBool);
    }

    /**
     * @param string $key
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/object
     */
    public function getObjectRefcount(string $key): Promise
    {
        return $this->queryExecutor->execute(['object', 'refcount', $key]);
    }

    /**
     * @param string $key
     *
     * @return Promise<string>
     *
     * @link https://redis.io/commands/object
     */
    public function getObjectEncoding(string $key): Promise
    {
        return $this->queryExecutor->execute(['object', 'encoding', $key]);
    }

    /**
     * @param string $key
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/object
     */
    public function getObjectIdletime(string $key): Promise
    {
        return $this->queryExecutor->execute(['object', 'idletime', $key]);
    }

    /**
     * @param string $key
     *
     * @return Promise<bool>
     *
     * @link https://redis.io/commands/persist
     */
    public function persist(string $key): Promise
    {
        return $this->queryExecutor->execute(['persist', $key], toBool);
    }

    /**
     * @return Promise<string|null>
     *
     * @link https://redis.io/commands/randomkey
     */
    public function getRandomKey(): Promise
    {
        return $this->queryExecutor->execute(['randomkey']);
    }

    /**
     * @param string $key
     * @param string $newKey
     *
     * @return Promise<void>
     *
     * @link https://redis.io/commands/rename
     */
    public function rename(string $key, string $newKey): Promise
    {
        return $this->queryExecutor->execute(['rename', $key, $newKey], toNull);
    }

    /**
     * @param string $key
     * @param string $newKey
     *
     * @return Promise<void>
     *
     * @link https://redis.io/commands/renamenx
     */
    public function renameWithoutOverwrite(string $key, string $newKey): Promise
    {
        return $this->queryExecutor->execute(['renamenx', $key, $newKey], toNull);
    }

    /**
     * @param string $key
     * @param string $serializedValue
     * @param int    $ttl
     *
     * @return Promise<void>
     *
     * @link https://redis.io/commands/restore
     */
    public function restore(string $key, string $serializedValue, int $ttl = 0): Promise
    {
        return $this->queryExecutor->execute(['restore', $key, $ttl, $serializedValue], toNull);
    }

    /**
     * @param string $pattern
     * @param int    $count
     *
     * @return Iterator<string>
     *
     * @link https://redis.io/commands/scan
     */
    public function scan(?string $pattern = null, ?int $count = null): Iterator
    {
        return new Producer(function (callable $emit) use ($pattern, $count) {
            $cursor = 0;

            do {
                $query = ['SCAN', $cursor];

                if ($pattern !== null) {
                    $query[] = 'MATCH';
                    $query[] = $pattern;
                }

                if ($count !== null) {
                    $query[] = 'COUNT';
                    $query[] = $count;
                }

                [$cursor, $keys] = yield $this->queryExecutor->execute($query);

                foreach ($keys as $key) {
                    yield $emit($key);
                }
            } while ($cursor !== '0');
        });
    }

    /**
     * @param string $key
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/ttl
     */
    public function getTtl(string $key): Promise
    {
        return $this->queryExecutor->execute(['ttl', $key]);
    }

    /**
     * @param string $key
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/pttl
     */
    public function getTtlInMillis(string $key): Promise
    {
        return $this->queryExecutor->execute(['pttl', $key]);
    }

    /**
     * @param string $key
     *
     * @return Promise<string>
     *
     * @link https://redis.io/commands/type
     */
    public function getType(string $key): Promise
    {
        return $this->queryExecutor->execute(['type', $key]);
    }

    /**
     * @param string $key
     * @param string $value
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/append
     */
    public function append(string $key, string $value): Promise
    {
        return $this->queryExecutor->execute(['append', $key, $value]);
    }

    /**
     * @param string   $key
     * @param int|null $start
     * @param int|null $end
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/bitcount
     */
    public function countBits(string $key, ?int $start = null, ?int $end = null): Promise
    {
        $cmd = ['bitcount', $key];

        if (isset($start, $end)) {
            $cmd[] = $start;
            $cmd[] = $end;
        } elseif (isset($start) || isset($end)) {
            throw \Error('Start and end must both be set or unset in countBits(), got start = ' . $start . ' and end = ' . $end);
        }

        return $this->queryExecutor->execute($cmd);
    }

    /**
     * @param string $destination
     * @param string $key
     * @param string ...$keys
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/bitop
     */
    public function storeBitwiseAnd(string $destination, string $key, string ...$keys): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['bitop', 'and', $destination, $key], $keys));
    }

    /**
     * @param string $destination
     * @param string $key
     * @param string ...$keys
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/bitop
     */
    public function storeBitwiseOr(string $destination, string $key, string ...$keys): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['bitop', 'or', $destination, $key], $keys));
    }

    /**
     * @param string $destination
     * @param string $key
     * @param string ...$keys
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/bitop
     */
    public function storeBitwiseXor(string $destination, string $key, string ...$keys): Promise
    {
        return $this->queryExecutor->execute(\array_merge(['bitop', 'xor', $destination, $key], $keys));
    }

    /**
     * @param string $destination
     * @param string $key
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/bitop
     */
    public function storeBitwiseNot(string $destination, string $key): Promise
    {
        return $this->queryExecutor->execute(['bitop', 'not', $destination, $key]);
    }

    /**
     * @param string $key
     * @param bool   $bit
     * @param int    $start
     * @param int    $end
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/bitpos
     */
    public function getBitPosition(string $key, bool $bit, ?int $start = null, ?int $end = null): Promise
    {
        $payload = ['bitpos', $key, $bit ? 1 : 0];

        if ($start !== null) {
            $payload[] = $start;

            if ($end !== null) {
                $payload[] = $end;
            }
        }

        return $this->queryExecutor->execute($payload);
    }

    /**
     * @param string $key
     * @param int    $decrement
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/decrby
     */
    public function decrement(string $key, int $decrement = 1): Promise
    {
        if ($decrement === 1) {
            return $this->queryExecutor->execute(['decr', $key]);
        }

        return $this->queryExecutor->execute(['decrby', $key, $decrement]);
    }

    /**
     * @param string $key
     *
     * @return Promise<string|null>
     *
     * @link https://redis.io/commands/get
     */
    public function get(string $key): Promise
    {
        return $this->queryExecutor->execute(['get', $key]);
    }

    /**
     * @param string $key
     * @param int    $offset
     *
     * @return Promise<bool>
     *
     * @link https://redis.io/commands/getbit
     */
    public function getBit(string $key, int $offset): Promise
    {
        return $this->queryExecutor->execute(['getbit', $key, $offset], toBool);
    }

    /**
     * @param string $key
     * @param int    $start
     * @param int    $end
     *
     * @return Promise<string>
     *
     * @link https://redis.io/commands/getrange
     */
    public function getRange(string $key, int $start = 0, int $end = -1): Promise
    {
        return $this->queryExecutor->execute(['getrange', $key, $start, $end]);
    }

    /**
     * @param string $key
     * @param string $value
     *
     * @return Promise<string>
     *
     * @link https://redis.io/commands/getset
     */
    public function getAndSet(string $key, string $value): Promise
    {
        return $this->queryExecutor->execute(['getset', $key, $value]);
    }

    /**
     * @param string $key
     * @param int    $increment
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/incrby
     */
    public function increment(string $key, int $increment = 1): Promise
    {
        if ($increment === 1) {
            return $this->queryExecutor->execute(['incr', $key]);
        }

        return $this->queryExecutor->execute(['incrby', $key, $increment]);
    }

    /**
     * @param string $key
     * @param float  $increment
     *
     * @return Promise<float>
     *
     * @link https://redis.io/commands/incrbyfloat
     */
    public function incrementByFloat(string $key, float $increment): Promise
    {
        return $this->queryExecutor->execute(['incrbyfloat', $key, $increment], toFloat);
    }

    /**
     * @param string $key
     * @param string ...$keys
     *
     * @return Promise<array<string|null>>
     *
     * @link https://redis.io/commands/mget
     */
    public function getMultiple(string $key, string ...$keys): Promise
    {
        \array_unshift($keys , $key);
        $query = \array_merge(['mget'], $keys);

        return $this->queryExecutor->execute($query, static function ($response) use ($keys) {
            return \array_combine($keys, $response);
        });
    }

    /**
     * @param array $data
     *
     * @return Promise<void>
     *
     * @link https://redis.io/commands/mset
     */
    public function setMultiple(array $data): Promise
    {
        $payload = ['mset'];

        foreach ($data as $key => $value) {
            $payload[] = $key;
            $payload[] = $value;
        }

        return $this->queryExecutor->execute($payload, toNull);
    }

    /**
     * @param array $data
     *
     * @return Promise<void>
     *
     * @link https://redis.io/commands/msetnx
     */
    public function setMultipleWithoutOverwrite(array $data): Promise
    {
        $payload = ['msetnx'];

        foreach ($data as $key => $value) {
            $payload[] = $key;
            $payload[] = $value;
        }

        return $this->queryExecutor->execute($payload, toNull);
    }

    /**
     * @param string $key
     * @param string $value
     *
     * @return Promise<bool>
     *
     * @link https://redis.io/commands/setnx
     */
    public function setWithoutOverwrite(string $key, string $value): Promise
    {
        return $this->queryExecutor->execute(['setnx', $key, $value], toBool);
    }

    /**
     * @param string $key
     * @param string $value
     *
     * @return Promise<bool>
     */
    public function set(string $key, string $value, SetOptions $options = null): Promise
    {
        $query = ['set', $key, $value];

        if ($options !== null) {
            $query = \array_merge($query, $options->toQuery());
        }

        return $this->queryExecutor->execute($query, toBool);
    }

    /**
     * @param string $key
     * @param int    $offset
     * @param bool   $value
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/setbit
     */
    public function setBit(string $key, int $offset, bool $value): Promise
    {
        return $this->queryExecutor->execute(['setbit', $key, $offset, (int) $value]);
    }

    /**
     * @param string $key
     * @param int    $offset
     * @param mixed  $value
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/setrange
     */
    public function setRange(string $key, int $offset, string $value): Promise
    {
        return $this->queryExecutor->execute(['setrange', $key, $offset, $value]);
    }

    /**
     * @param string $key
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/strlen
     */
    public function getLength(string $key): Promise
    {
        return $this->queryExecutor->execute(['strlen', $key]);
    }

    /**
     * @param string $channel
     * @param string $message
     *
     * @return Promise<int>
     *
     * @link https://redis.io/commands/publish
     */
    public function publish(string $channel, string $message): Promise
    {
        return $this->queryExecutor->execute(['publish', $channel, $message]);
    }

    /**
     * @param string $pattern
     *
     * @return Promise<array>
     *
     * @link https://redis.io/commands/pubsub
     */
    public function getActiveChannels(?string $pattern = null): Promise
    {
        $payload = ['pubsub', 'channels'];

        if ($pattern !== null) {
            $payload[] = $pattern;
        }

        return $this->queryExecutor->execute($payload);
    }

    /**
     * @param string ...$channels
     *
     * @return Promise<int[]>
     *
     * @link https://redis.io/commands/pubsub
     */
    public function getNumberOfSubscriptions(string ...$channels): Promise
    {
        $query = \array_merge(['pubsub', 'numsub'], $channels);

        return $this->queryExecutor->execute($query, static function ($response) {
            $result = [];

            for ($i = 0, $count = \count($response); $i < $count; $i += 2) {
                $result[$response[$i]] = $response[$i + 1];
            }

            return $result;
        });
    }

    /**
     * @return Promise<int>
     *
     * @link https://redis.io/commands/pubsub
     */
    public function getNumberOfPatternSubscriptions(): Promise
    {
        return $this->queryExecutor->execute(['pubsub', 'numpat']);
    }

    /**
     * @return Promise<void>
     *
     * @link https://redis.io/commands/ping
     */
    public function ping(): Promise
    {
        return $this->queryExecutor->execute(['ping'], toNull);
    }

    /**
     * @return Promise<void>
     *
     * @link https://redis.io/commands/quit
     */
    public function quit(): Promise
    {
        return $this->queryExecutor->execute(['quit'], toNull);
    }

    /**
     * @return Promise<void>
     *
     * @link https://redis.io/commands/bgrewriteaof
     */
    public function rewriteAofAsync(): Promise
    {
        return $this->queryExecutor->execute(['bgrewriteaof'], toNull);
    }

    /**
     * @return Promise<void>
     *
     * @link https://redis.io/commands/bgsave
     */
    public function saveAsync(): Promise
    {
        return $this->queryExecutor->execute(['bgsave'], toNull);
    }

    /**
     * @return Promise<string>
     *
     * @link https://redis.io/commands/client-getname
     */
    public function getName(): Promise
    {
        return $this->queryExecutor->execute(['client', 'getname']);
    }

    /**
     * @param int $timeInMillis
     *
     * @return Promise<void>
     *
     * @link https://redis.io/commands/client-pause
     */
    public function pauseMillis(int $timeInMillis): Promise
    {
        return $this->queryExecutor->execute(['client', 'pause', $timeInMillis], toNull);
    }

    /**
     * @param string $name
     *
     * @return Promise<void>
     *
     * @link https://redis.io/commands/client-setname
     */
    public function setName(string $name): Promise
    {
        return $this->queryExecutor->execute(['client', 'setname', $name], toNull);
    }

    /**
     * @param string $parameter
     *
     * @return Promise<array>
     *
     * @link https://redis.io/commands/config-get
     */
    public function getConfig(string $parameter): Promise
    {
        return $this->queryExecutor->execute(['config', 'get', $parameter]);
    }

    /**
     * @return Promise<void>
     *
     * @link https://redis.io/commands/config-resetstat
     */
    public function resetStatistics(): Promise
    {
        return $this->queryExecutor->execute(['config', 'resetstat'], toNull);
    }

    /**
     * @return Promise<void>
     *
     * @link https://redis.io/commands/config-rewrite
     */
    public function rewriteConfig(): Promise
    {
        return $this->queryExecutor->execute(['config', 'rewrite'], toNull);
    }

    /**
     * @param string $parameter
     * @param string $value
     *
     * @return Promise<void>
     *
     * @link https://redis.io/commands/config-set
     */
    public function setConfig(string $parameter, string $value): Promise
    {
        return $this->queryExecutor->execute(['config', 'set', $parameter, $value], toNull);
    }

    /**
     * @return Promise<int>
     *
     * @link https://redis.io/commands/dbsize
     */
    public function getDatabaseSize(): Promise
    {
        return $this->queryExecutor->execute(['dbsize']);
    }

    /**
     * @return Promise<void>
     *
     * @link https://redis.io/commands/flushall
     */
    public function flushAll(): Promise
    {
        return $this->queryExecutor->execute(['flushall'], toNull);
    }

    /**
     * @return Promise<void>
     *
     * @link https://redis.io/commands/flushdb
     */
    public function flushDatabase(): Promise
    {
        return $this->queryExecutor->execute(['flushdb'], toNull);
    }

    /**
     * @return Promise<int>
     *
     * @link https://redis.io/commands/lastsave
     */
    public function getLastSave(): Promise
    {
        return $this->queryExecutor->execute(['lastsave']);
    }

    /**
     * @return Promise<array>
     *
     * @link https://redis.io/commands/role
     */
    public function getRole(): Promise
    {
        return $this->queryExecutor->execute(['role']);
    }

    /**
     * @return Promise<void>
     *
     * @link https://redis.io/commands/save
     */
    public function save(): Promise
    {
        return $this->queryExecutor->execute(['save'], toNull);
    }

    /**
     * @return Promise<string>
     *
     * @link https://redis.io/commands/shutdown
     */
    public function shutdownWithSave(): Promise
    {
        return $this->queryExecutor->execute(['shutdown', 'save']);
    }

    /**
     * @return Promise<string>
     *
     * @link https://redis.io/commands/shutdown
     */
    public function shutdownWithoutSave(): Promise
    {
        return $this->queryExecutor->execute(['shutdown', 'nosave']);
    }

    /**
     * @return Promise<string>
     *
     * @link https://redis.io/commands/shutdown
     */
    public function shutdown(): Promise
    {
        return $this->queryExecutor->execute(['shutdown']);
    }

    /**
     * @param string $host
     * @param int    $port
     *
     * @return Promise<void>
     *
     * @link https://redis.io/commands/slaveof
     */
    public function enableReplication(string $host, int $port): Promise
    {
        return $this->queryExecutor->execute(['slaveof', $host, $port], toNull);
    }

    /**
     * @return Promise<void>
     *
     * @link https://redis.io/commands/slaveof
     */
    public function disableReplication(): Promise
    {
        return $this->queryExecutor->execute(['slaveof', 'no', 'one'], toNull);
    }

    /**
     * @param int $count
     *
     * @return Promise<array>
     *
     * @link https://redis.io/commands/slowlog
     */
    public function getSlowlog(?int $count = null): Promise
    {
        $payload = ['slowlog', 'get'];

        if ($count !== null) {
            $payload[] = $count;
        }

        return $this->queryExecutor->execute($payload);
    }

    /**
     * @return Promise<int>
     *
     * @link https://redis.io/commands/slowlog
     */
    public function getSlowlogLength(): Promise
    {
        return $this->queryExecutor->execute(['slowlog', 'len']);
    }

    /**
     * @return Promise<void>
     *
     * @link https://redis.io/commands/slowlog
     */
    public function resetSlowlog(): Promise
    {
        return $this->queryExecutor->execute(['slowlog', 'reset'], toNull);
    }

    /**
     * @return Promise<array>
     *
     * @link https://redis.io/commands/time
     */
    public function getTime(): Promise
    {
        return $this->queryExecutor->execute(['time']);
    }

    /**
     * @return Promise<bool>
     *
     * @link https://redis.io/commands/script-exists
     */
    public function hasScript(string $sha1): Promise
    {
        return $this->queryExecutor->execute(['script', 'exists', $sha1], static function (array $array) {
            return (bool) $array[0];
        });
    }

    /**
     * @return Promise<void>
     *
     * @link https://redis.io/commands/script-flush
     */
    public function flushScripts(): Promise
    {
        $this->evalCache = []; // same as internal redis behavior

        return $this->queryExecutor->execute(['script', 'flush'], toNull);
    }

    /**
     * @return Promise<void>
     *
     * @link https://redis.io/commands/script-kill
     */
    public function killScript(): Promise
    {
        return $this->queryExecutor->execute(['script', 'kill'], toNull);
    }

    /**
     * @param string $script
     *
     * @return Promise<string>
     *
     * @link https://redis.io/commands/script-load
     */
    public function loadScript(string $script): Promise
    {
        return $this->queryExecutor->execute(['script', 'load', $script]);
    }

    /**
     * @param string $text
     *
     * @return Promise<string>
     *
     * @link https://redis.io/commands/echo
     */
    public function echo(string $text): Promise
    {
        return $this->queryExecutor->execute(['echo', $text]);
    }

    /**
     * @param string   $script
     * @param string[] $keys
     * @param string[] $args
     *
     * @return Promise<mixed>
     *
     * @link https://redis.io/commands/eval
     */
    public function eval(string $script, array $keys = [], array $args = []): Promise
    {
        return call(function () use ($script, $keys, $args) {
            try {
                $sha1 = $this->evalCache[$script] ?? ($this->evalCache[$script] = \sha1($script));
                $query = \array_merge(['evalsha', $sha1, \count($keys)], $keys, $args);

                return yield $this->queryExecutor->execute($query);
            } catch (QueryException $e) {
                if (\strtok($e->getMessage(), ' ') === 'NOSCRIPT') {
                    $query = \array_merge(['eval', $script, \count($keys)], $keys, $args);
                    return $this->queryExecutor->execute($query);
                }

                throw $e;
            }
        });
    }

    public function select(int $database): Promise
    {
        return $this->queryExecutor->execute(['select', $database], toNull);
    }
}
