<?php declare(strict_types=1);
/** @noinspection DuplicatedCode */

namespace Amp\Redis;

final class Redis
{
    /** @var string[] */
    private array $evalCache = [];

    public function __construct(
        private readonly RedisClient $client
    ) {
    }

    public function getHyperLogLog(string $key): RedisHyperLogLog
    {
        return new RedisHyperLogLog($this->client, $key);
    }

    public function getList(string $key): RedisList
    {
        return new RedisList($this->client, $key);
    }

    public function getMap(string $key): RedisMap
    {
        return new RedisMap($this->client, $key);
    }

    public function getSet(string $key): RedisSet
    {
        return new RedisSet($this->client, $key);
    }

    public function getSortedSet(string $key): RedisSortedSet
    {
        return new RedisSortedSet($this->client, $key);
    }

    public function query(string $arg, string ...$args): mixed
    {
        return $this->client->execute($arg, ...$args);
    }

    /**
     * @link https://redis.io/commands/del
     */
    public function delete(string $key, string ...$keys): int
    {
        return $this->client->execute('del', $key, ...$keys);
    }

    /**
     * @link https://redis.io/commands/dump
     */
    public function dump(string $key): string
    {
        return $this->client->execute('dump', $key);
    }

    /**
     * @link https://redis.io/commands/exists
     */
    public function has(string $key): bool
    {
        return (bool) $this->client->execute('exists', $key);
    }

    /**
     * @link https://redis.io/commands/expire
     */
    public function expireIn(string $key, int $seconds): bool
    {
        return (bool) $this->client->execute('expire', $key, $seconds);
    }

    /**
     * @link https://redis.io/commands/pexpire
     */
    public function expireInMillis(string $key, int $millis): bool
    {
        return (bool) $this->client->execute('pexpire', $key, $millis);
    }

    /**
     * @link https://redis.io/commands/expireat
     */
    public function expireAt(string $key, int $timestamp): bool
    {
        return (bool) $this->client->execute('expireat', $key, $timestamp);
    }

    /**
     * @link https://redis.io/commands/pexpireat
     */
    public function expireAtMillis(string $key, int $timestamp): bool
    {
        return (bool) $this->client->execute('pexpireat', $key, $timestamp);
    }

    /**
     * @link https://redis.io/commands/keys
     *
     * @see Redis::scan()
     */
    public function getKeys(string $pattern = '*'): array
    {
        return $this->client->execute('keys', $pattern);
    }

    /**
     * @link https://redis.io/commands/move
     */
    public function move(string $key, int $db): bool
    {
        return (bool) $this->client->execute('move', $key, $db);
    }

    /**
     * @link https://redis.io/commands/object
     */
    public function getObjectRefcount(string $key): int
    {
        return $this->client->execute('object', 'refcount', $key);
    }

    /**
     * @link https://redis.io/commands/object
     */
    public function getObjectEncoding(string $key): string
    {
        return $this->client->execute('object', 'encoding', $key);
    }

    /**
     * @link https://redis.io/commands/object
     */
    public function getObjectIdletime(string $key): int
    {
        return $this->client->execute('object', 'idletime', $key);
    }

    /**
     * @link https://redis.io/commands/persist
     */
    public function persist(string $key): bool
    {
        return (bool) $this->client->execute('persist', $key);
    }

    /**
     * @link https://redis.io/commands/randomkey
     */
    public function getRandomKey(): ?string
    {
        return $this->client->execute('randomkey');
    }

    /**
     * @link https://redis.io/commands/rename
     */
    public function rename(string $key, string $newKey): void
    {
        $this->client->execute('rename', $key, $newKey);
    }

    /**
     * @link https://redis.io/commands/renamenx
     */
    public function renameWithoutOverwrite(string $key, string $newKey): void
    {
        $this->client->execute('renamenx', $key, $newKey);
    }

    /**
     * @link https://redis.io/commands/restore
     */
    public function restore(string $key, string $serializedValue, int $ttl = 0): void
    {
        $this->client->execute('restore', $key, $ttl, $serializedValue);
    }

    /**
     * @return \Traversable<string>
     *
     * @link https://redis.io/commands/scan
     */
    public function scan(?string $pattern = null, ?int $count = null): \Traversable
    {
        $cursor = 0;

        do {
            $query = [$cursor];

            if ($pattern !== null) {
                $query[] = 'MATCH';
                $query[] = $pattern;
            }

            if ($count !== null) {
                $query[] = 'COUNT';
                $query[] = $count;
            }

            [$cursor, $keys] = $this->client->execute('SCAN', ...$query);

            foreach ($keys as $key) {
                yield $key;
            }
        } while ($cursor !== '0');
    }

    /**
     * @link https://redis.io/commands/ttl
     */
    public function getTtl(string $key): int
    {
        return $this->client->execute('ttl', $key);
    }

    /**
     * @link https://redis.io/commands/pttl
     */
    public function getTtlInMillis(string $key): int
    {
        return $this->client->execute('pttl', $key);
    }

    /**
     * @link https://redis.io/commands/type
     */
    public function getType(string $key): string
    {
        return $this->client->execute('type', $key);
    }

    /**
     * @link https://redis.io/commands/append
     */
    public function append(string $key, string $value): int
    {
        return $this->client->execute('append', $key, $value);
    }

    /**
     * @link https://redis.io/commands/bitcount
     */
    public function countBits(string $key, ?int $start = null, ?int $end = null): int
    {
        $cmd = [$key];

        if (isset($start, $end)) {
            $cmd[] = $start;
            $cmd[] = $end;
        } elseif (isset($start) || isset($end)) {
            throw new \Error('Start and end must both be set or unset in countBits(), got start = ' . $start . ' and end = ' . $end);
        }

        return $this->client->execute('bitcount', ...$cmd);
    }

    /**
     * @link https://redis.io/commands/bitop
     */
    public function storeBitwiseAnd(string $destination, string $key, string ...$keys): int
    {
        return $this->client->execute('bitop', 'and', $destination, $key, ...$keys);
    }

    /**
     * @link https://redis.io/commands/bitop
     */
    public function storeBitwiseOr(string $destination, string $key, string ...$keys): int
    {
        return $this->client->execute('bitop', 'or', $destination, $key, ...$keys);
    }

    /**
     * @link https://redis.io/commands/bitop
     */
    public function storeBitwiseXor(string $destination, string $key, string ...$keys): int
    {
        return $this->client->execute('bitop', 'xor', $destination, $key, ...$keys);
    }

    /**
     * @link https://redis.io/commands/bitop
     */
    public function storeBitwiseNot(string $destination, string $key): int
    {
        return $this->client->execute('bitop', 'not', $destination, $key);
    }

    /**
     * @link https://redis.io/commands/bitpos
     */
    public function getBitPosition(string $key, bool $bit, ?int $start = null, ?int $end = null): int
    {
        $payload = [$key, $bit ? 1 : 0];

        if ($start !== null) {
            $payload[] = $start;

            if ($end !== null) {
                $payload[] = $end;
            }
        }

        return $this->client->execute('bitpos', ...$payload);
    }

    /**
     * @link https://redis.io/commands/decrby
     */
    public function decrement(string $key, int $decrement = 1): int
    {
        if ($decrement === 1) {
            return $this->client->execute('decr', $key);
        }

        return $this->client->execute('decrby', $key, $decrement);
    }

    /**
     * @link https://redis.io/commands/get
     */
    public function get(string $key): ?string
    {
        return $this->client->execute('get', $key);
    }

    /**
     * @link https://redis.io/commands/getbit
     */
    public function getBit(string $key, int $offset): bool
    {
        return (bool) $this->client->execute('getbit', $key, $offset);
    }

    /**
     * @link https://redis.io/commands/getrange
     */
    public function getRange(string $key, int $start = 0, int $end = -1): string
    {
        return $this->client->execute('getrange', $key, $start, $end);
    }

    /**
     * @link https://redis.io/commands/getset
     */
    public function getAndSet(string $key, string $value): string
    {
        return $this->client->execute('getset', $key, $value);
    }

    /**
     * @link https://redis.io/commands/incrby
     */
    public function increment(string $key, int $increment = 1): int
    {
        if ($increment === 1) {
            return $this->client->execute('incr', $key);
        }

        return $this->client->execute('incrby', $key, $increment);
    }

    /**
     * @link https://redis.io/commands/incrbyfloat
     */
    public function incrementByFloat(string $key, float $increment): float
    {
        return (float) $this->client->execute('incrbyfloat', $key, $increment);
    }

    /**
     * @return array<string|null>
     *
     * @link https://redis.io/commands/mget
     */
    public function getMultiple(string $key, string ...$keys): array
    {
        return \array_combine($keys, $this->client->execute('mget', $key, ...$keys));
    }

    /**
     * @link https://redis.io/commands/mset
     *
     * @param array<string, int|float|string> $data
     */
    public function setMultiple(array $data): void
    {
        $payload = [];

        foreach ($data as $key => $value) {
            $payload[] = $key;
            $payload[] = $value;
        }

        $this->client->execute('mset', ...$payload);
    }

    /**
     * @link https://redis.io/commands/msetnx
     *
     * @param array<string, int|float|string> $data
     */
    public function setMultipleWithoutOverwrite(array $data): void
    {
        $payload = [];

        foreach ($data as $key => $value) {
            $payload[] = $key;
            $payload[] = $value;
        }

        $this->client->execute('msetnx', ...$payload);
    }

    /**
     * @link https://redis.io/commands/setnx
     */
    public function setWithoutOverwrite(string $key, string $value): bool
    {
        return (bool) $this->client->execute('setnx', $key, $value);
    }

    /**
     * @link https://redis.io/commands/set/
     */
    public function set(string $key, string $value, ?RedisSetOptions $options = null): bool
    {
        $options ??= new RedisSetOptions();
        return (bool) $this->client->execute('set', $key, $value, ...$options->toQuery());
    }

    /**
     * @link https://redis.io/commands/setbit
     */
    public function setBit(string $key, int $offset, bool $value): int
    {
        return $this->client->execute('setbit', $key, $offset, (int) $value);
    }

    /**
     * @param mixed  $value
     *
     * @link https://redis.io/commands/setrange
     */
    public function setRange(string $key, int $offset, string $value): int
    {
        return $this->client->execute('setrange', $key, $offset, $value);
    }

    /**
     * @link https://redis.io/commands/strlen
     */
    public function getLength(string $key): int
    {
        return $this->client->execute('strlen', $key);
    }

    /**
     * @link https://redis.io/commands/publish
     */
    public function publish(string $channel, string $message): int
    {
        return $this->client->execute('publish', $channel, $message);
    }

    /**
     * @param string $pattern
     *
     * @link https://redis.io/commands/pubsub
     */
    public function getActiveChannels(?string $pattern = null): array
    {
        $payload = ['channels'];

        if ($pattern !== null) {
            $payload[] = $pattern;
        }

        return $this->client->execute('pubsub', ...$payload);
    }

    /**
     * @return int[]
     *
     * @link https://redis.io/commands/pubsub
     */
    public function getNumberOfSubscriptions(string ...$channels): array
    {
        return Internal\toMap($this->client->execute('pubsub', 'numsub', ...$channels));
    }

    /**
     * @link https://redis.io/commands/pubsub
     */
    public function getNumberOfPatternSubscriptions(): int
    {
        return $this->client->execute('pubsub', 'numpat');
    }

    /**
     * @link https://redis.io/commands/ping
     */
    public function ping(): void
    {
        $this->client->execute('ping');
    }

    /**
     * @link https://redis.io/commands/quit
     */
    public function quit(): void
    {
        $this->client->execute('quit');
    }

    /**
     * @link https://redis.io/commands/bgrewriteaof
     */
    public function rewriteAofAsync(): void
    {
        $this->client->execute('bgrewriteaof');
    }

    /**
     * @link https://redis.io/commands/bgsave
     */
    public function saveAsync(): void
    {
        $this->client->execute('bgsave');
    }

    /**
     * @link https://redis.io/commands/client-getname
     */
    public function getName(): ?string
    {
        return $this->client->execute('client', 'getname');
    }

    /**
     * @link https://redis.io/commands/client-pause
     */
    public function pauseMillis(int $timeInMillis): void
    {
        $this->client->execute('client', 'pause', $timeInMillis);
    }

    /**
     * @link https://redis.io/commands/client-setname
     */
    public function setName(string $name): void
    {
        $this->client->execute('client', 'setname', $name);
    }

    /**
     * @link https://redis.io/commands/config-get
     */
    public function getConfig(string $parameter): array
    {
        return $this->client->execute('config', 'get', $parameter);
    }

    /**
     * @link https://redis.io/commands/config-resetstat
     */
    public function resetStatistics(): void
    {
        $this->client->execute('config', 'resetstat');
    }

    /**
     * @link https://redis.io/commands/config-rewrite
     */
    public function rewriteConfig(): void
    {
        $this->client->execute('config', 'rewrite');
    }

    /**
     * @link https://redis.io/commands/config-set
     */
    public function setConfig(string $parameter, string $value): void
    {
        $this->client->execute('config', 'set', $parameter, $value);
    }

    /**
     * @link https://redis.io/commands/dbsize
     */
    public function getDatabaseSize(): int
    {
        return $this->client->execute('dbsize');
    }

    /**
     * @link https://redis.io/commands/flushall
     */
    public function flushAll(): void
    {
        $this->client->execute('flushall');
    }

    /**
     * @link https://redis.io/commands/flushdb
     */
    public function flushDatabase(): void
    {
        $this->client->execute('flushdb');
    }

    /**
     * @link https://redis.io/commands/lastsave
     */
    public function getLastSave(): int
    {
        return $this->client->execute('lastsave');
    }

    /**
     * @link https://redis.io/commands/role
     */
    public function getRole(): array
    {
        return $this->client->execute('role');
    }

    /**
     * @link https://redis.io/commands/save
     */
    public function save(): void
    {
        $this->client->execute('save');
    }

    /**
     * @link https://redis.io/commands/shutdown
     */
    public function shutdownWithSave(): string
    {
        return $this->client->execute('shutdown', 'save');
    }

    /**
     * @link https://redis.io/commands/shutdown
     */
    public function shutdownWithoutSave(): string
    {
        return $this->client->execute('shutdown', 'nosave');
    }

    /**
     * @link https://redis.io/commands/shutdown
     */
    public function shutdown(): string
    {
        return $this->client->execute('shutdown');
    }

    /**
     * @link https://redis.io/commands/slaveof
     */
    public function enableReplication(string $host, int $port): void
    {
        $this->client->execute('slaveof', $host, $port);
    }

    /**
     * @link https://redis.io/commands/slaveof
     */
    public function disableReplication(): void
    {
        $this->client->execute('slaveof', 'no', 'one');
    }

    /**
     * @param int $count
     *
     * @link https://redis.io/commands/slowlog
     */
    public function getSlowlog(?int $count = null): array
    {
        $payload = ['get'];

        if ($count !== null) {
            $payload[] = $count;
        }

        return $this->client->execute('slowlog', ...$payload);
    }

    /**
     * @link https://redis.io/commands/slowlog
     */
    public function getSlowlogLength(): int
    {
        return $this->client->execute('slowlog', 'len');
    }

    /**
     * @link https://redis.io/commands/slowlog
     */
    public function resetSlowlog(): void
    {
        $this->client->execute('slowlog', 'reset');
    }

    /**
     * @link https://redis.io/commands/time
     */
    public function getTime(): array
    {
        return $this->client->execute('time');
    }

    /**
     * @link https://redis.io/commands/script-exists
     */
    public function hasScript(string $sha1): bool
    {
        $array = $this->client->execute('script', 'exists', $sha1);
        return (bool) ($array[0] ?? false);
    }

    /**
     * @link https://redis.io/commands/script-flush
     */
    public function flushScripts(): void
    {
        $this->evalCache = []; // same as internal redis behavior

        $this->client->execute('script', 'flush');
    }

    /**
     * @link https://redis.io/commands/script-kill
     */
    public function killScript(): void
    {
        $this->client->execute('script', 'kill');
    }

    /**
     * @link https://redis.io/commands/script-load
     */
    public function loadScript(string $script): string
    {
        return $this->client->execute('script', 'load', $script);
    }

    /**
     * @link https://redis.io/commands/echo
     */
    public function echo(string $text): string
    {
        return $this->client->execute('echo', $text);
    }

    /**
     * @param array<array-key, string> $keys
     * @param array<array-key, int|float|string> $args
     *
     * @link https://redis.io/commands/eval
     */
    public function eval(string $script, array $keys = [], array $args = []): mixed
    {
        try {
            $sha1 = $this->evalCache[$script] ?? ($this->evalCache[$script] = \sha1($script));
            return $this->client->execute('evalsha', $sha1, \count($keys), ...$keys, ...$args);
        } catch (QueryException $e) {
            if (\strtok($e->getMessage(), ' ') === 'NOSCRIPT') {
                return $this->client->execute('eval', $script, \count($keys), ...$keys, ...$args);
            }

            throw $e;
        }
    }

    public function select(int $database): void
    {
        $this->client->execute('select', $database);
    }
}
