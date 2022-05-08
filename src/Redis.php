<?php /** @noinspection DuplicatedCode */

namespace Amp\Redis;

final class Redis
{
    /** @var string[] */
    private array $evalCache = [];

    public function __construct(
        private readonly QueryExecutor $queryExecutor
    ) {
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

    public function query(string $arg, string ...$args): mixed
    {
        return $this->queryExecutor->execute(\array_merge([$arg], $args));
    }

    /**
     * @link https://redis.io/commands/del
     */
    public function delete(string $key, string ...$keys): int
    {
        return $this->queryExecutor->execute(\array_merge(['del', $key], $keys));
    }

    /**
     * @link https://redis.io/commands/dump
     */
    public function dump(string $key): string
    {
        return $this->queryExecutor->execute(['dump', $key]);
    }

    /**
     * @link https://redis.io/commands/exists
     */
    public function has(string $key): bool
    {
        return $this->queryExecutor->execute(['exists', $key], toBool(...));
    }

    /**
     * @link https://redis.io/commands/expire
     */
    public function expireIn(string $key, int $seconds): bool
    {
        return $this->queryExecutor->execute(['expire', $key, $seconds], toBool(...));
    }

    /**
     * @link https://redis.io/commands/pexpire
     */
    public function expireInMillis(string $key, int $millis): bool
    {
        return $this->queryExecutor->execute(['pexpire', $key, $millis], toBool(...));
    }

    /**
     * @link https://redis.io/commands/expireat
     */
    public function expireAt(string $key, int $timestamp): bool
    {
        return $this->queryExecutor->execute(['expireat', $key, $timestamp], toBool(...));
    }

    /**
     * @link https://redis.io/commands/pexpireat
     */
    public function expireAtMillis(string $key, int $timestamp): bool
    {
        return $this->queryExecutor->execute(['pexpireat', $key, $timestamp], toBool(...));
    }

    /**
     * @link https://redis.io/commands/keys
     *
     * @see Redis::scan()
     */
    public function getKeys(string $pattern = '*'): array
    {
        return $this->queryExecutor->execute(['keys', $pattern]);
    }

    /**
     * @link https://redis.io/commands/move
     */
    public function move(string $key, int $db): bool
    {
        return $this->queryExecutor->execute(['move', $key, $db], toBool(...));
    }

    /**
     * @link https://redis.io/commands/object
     */
    public function getObjectRefcount(string $key): int
    {
        return $this->queryExecutor->execute(['object', 'refcount', $key]);
    }

    /**
     * @link https://redis.io/commands/object
     */
    public function getObjectEncoding(string $key): string
    {
        return $this->queryExecutor->execute(['object', 'encoding', $key]);
    }

    /**
     * @link https://redis.io/commands/object
     */
    public function getObjectIdletime(string $key): int
    {
        return $this->queryExecutor->execute(['object', 'idletime', $key]);
    }

    /**
     * @link https://redis.io/commands/persist
     */
    public function persist(string $key): bool
    {
        return $this->queryExecutor->execute(['persist', $key], toBool(...));
    }

    /**
     * @link https://redis.io/commands/randomkey
     */
    public function getRandomKey(): ?string
    {
        return $this->queryExecutor->execute(['randomkey']);
    }

    /**
     * @link https://redis.io/commands/rename
     */
    public function rename(string $key, string $newKey): void
    {
        $this->queryExecutor->execute(['rename', $key, $newKey], toNull(...));
    }

    /**
     * @link https://redis.io/commands/renamenx
     */
    public function renameWithoutOverwrite(string $key, string $newKey): void
    {
        $this->queryExecutor->execute(['renamenx', $key, $newKey], toNull(...));
    }

    /**
     * @link https://redis.io/commands/restore
     */
    public function restore(string $key, string $serializedValue, int $ttl = 0): void
    {
        $this->queryExecutor->execute(['restore', $key, $ttl, $serializedValue], toNull(...));
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
            $query = ['SCAN', $cursor];

            if ($pattern !== null) {
                $query[] = 'MATCH';
                $query[] = $pattern;
            }

            if ($count !== null) {
                $query[] = 'COUNT';
                $query[] = $count;
            }

            [$cursor, $keys] = $this->queryExecutor->execute($query);

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
        return $this->queryExecutor->execute(['ttl', $key]);
    }

    /**
     * @link https://redis.io/commands/pttl
     */
    public function getTtlInMillis(string $key): int
    {
        return $this->queryExecutor->execute(['pttl', $key]);
    }

    /**
     * @link https://redis.io/commands/type
     */
    public function getType(string $key): string
    {
        return $this->queryExecutor->execute(['type', $key]);
    }

    /**
     * @link https://redis.io/commands/append
     */
    public function append(string $key, string $value): int
    {
        return $this->queryExecutor->execute(['append', $key, $value]);
    }

    /**
     * @link https://redis.io/commands/bitcount
     */
    public function countBits(string $key, ?int $start = null, ?int $end = null): int
    {
        $cmd = ['bitcount', $key];

        if (isset($start, $end)) {
            $cmd[] = $start;
            $cmd[] = $end;
        } elseif (isset($start) || isset($end)) {
            throw new \Error('Start and end must both be set or unset in countBits(), got start = ' . $start . ' and end = ' . $end);
        }

        return $this->queryExecutor->execute($cmd);
    }

    /**
     * @link https://redis.io/commands/bitop
     */
    public function storeBitwiseAnd(string $destination, string $key, string ...$keys): int
    {
        return $this->queryExecutor->execute(\array_merge(['bitop', 'and', $destination, $key], $keys));
    }

    /**
     * @link https://redis.io/commands/bitop
     */
    public function storeBitwiseOr(string $destination, string $key, string ...$keys): int
    {
        return $this->queryExecutor->execute(\array_merge(['bitop', 'or', $destination, $key], $keys));
    }

    /**
     * @link https://redis.io/commands/bitop
     */
    public function storeBitwiseXor(string $destination, string $key, string ...$keys): int
    {
        return $this->queryExecutor->execute(\array_merge(['bitop', 'xor', $destination, $key], $keys));
    }

    /**
     * @link https://redis.io/commands/bitop
     */
    public function storeBitwiseNot(string $destination, string $key): int
    {
        return $this->queryExecutor->execute(['bitop', 'not', $destination, $key]);
    }

    /**
     * @param int    $start
     * @param int    $end
     *
     * @link https://redis.io/commands/bitpos
     */
    public function getBitPosition(string $key, bool $bit, ?int $start = null, ?int $end = null): int
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
     * @link https://redis.io/commands/decrby
     */
    public function decrement(string $key, int $decrement = 1): int
    {
        if ($decrement === 1) {
            return $this->queryExecutor->execute(['decr', $key]);
        }

        return $this->queryExecutor->execute(['decrby', $key, $decrement]);
    }

    /**
     * @link https://redis.io/commands/get
     */
    public function get(string $key): ?string
    {
        return $this->queryExecutor->execute(['get', $key]);
    }

    /**
     * @link https://redis.io/commands/getbit
     */
    public function getBit(string $key, int $offset): bool
    {
        return $this->queryExecutor->execute(['getbit', $key, $offset], toBool(...));
    }

    /**
     * @link https://redis.io/commands/getrange
     */
    public function getRange(string $key, int $start = 0, int $end = -1): string
    {
        return $this->queryExecutor->execute(['getrange', $key, $start, $end]);
    }

    /**
     * @link https://redis.io/commands/getset
     */
    public function getAndSet(string $key, string $value): string
    {
        return $this->queryExecutor->execute(['getset', $key, $value]);
    }

    /**
     * @link https://redis.io/commands/incrby
     */
    public function increment(string $key, int $increment = 1): int
    {
        if ($increment === 1) {
            return $this->queryExecutor->execute(['incr', $key]);
        }

        return $this->queryExecutor->execute(['incrby', $key, $increment]);
    }

    /**
     * @link https://redis.io/commands/incrbyfloat
     */
    public function incrementByFloat(string $key, float $increment): float
    {
        return $this->queryExecutor->execute(['incrbyfloat', $key, $increment], toFloat(...));
    }

    /**
     * @return array<string|null>
     *
     * @link https://redis.io/commands/mget
     */
    public function getMultiple(string $key, string ...$keys): array
    {
        $query = \array_merge(['mget', $key], $keys);

        return $this->queryExecutor->execute($query, static function ($response) use ($keys) {
            return \array_combine($keys, $response);
        });
    }

    /**
     * @link https://redis.io/commands/mset
     */
    public function setMultiple(array $data): void
    {
        $payload = ['mset'];

        foreach ($data as $key => $value) {
            $payload[] = $key;
            $payload[] = $value;
        }

        $this->queryExecutor->execute($payload, toNull(...));
    }

    /**
     * @link https://redis.io/commands/msetnx
     */
    public function setMultipleWithoutOverwrite(array $data): void
    {
        $payload = ['msetnx'];

        foreach ($data as $key => $value) {
            $payload[] = $key;
            $payload[] = $value;
        }

        $this->queryExecutor->execute($payload, toNull(...));
    }

    /**
     * @link https://redis.io/commands/setnx
     */
    public function setWithoutOverwrite(string $key, string $value): bool
    {
        return $this->queryExecutor->execute(['setnx', $key, $value], toBool(...));
    }

    public function set(string $key, string $value, ?SetOptions $options = null): bool
    {
        $query = ['set', $key, $value];

        if ($options !== null) {
            $query = \array_merge($query, $options->toQuery());
        }

        return $this->queryExecutor->execute($query, toBool(...));
    }

    /**
     * @link https://redis.io/commands/setbit
     */
    public function setBit(string $key, int $offset, bool $value): int
    {
        return $this->queryExecutor->execute(['setbit', $key, $offset, (int) $value]);
    }

    /**
     * @param mixed  $value
     *
     * @link https://redis.io/commands/setrange
     */
    public function setRange(string $key, int $offset, string $value): int
    {
        return $this->queryExecutor->execute(['setrange', $key, $offset, $value]);
    }

    /**
     * @link https://redis.io/commands/strlen
     */
    public function getLength(string $key): int
    {
        return $this->queryExecutor->execute(['strlen', $key]);
    }

    /**
     * @link https://redis.io/commands/publish
     */
    public function publish(string $channel, string $message): int
    {
        return $this->queryExecutor->execute(['publish', $channel, $message]);
    }

    /**
     * @param string $pattern
     *
     * @link https://redis.io/commands/pubsub
     */
    public function getActiveChannels(?string $pattern = null): array
    {
        $payload = ['pubsub', 'channels'];

        if ($pattern !== null) {
            $payload[] = $pattern;
        }

        return $this->queryExecutor->execute($payload);
    }

    /**
     * @return int[]
     *
     * @link https://redis.io/commands/pubsub
     */
    public function getNumberOfSubscriptions(string ...$channels): array
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
     * @link https://redis.io/commands/pubsub
     */
    public function getNumberOfPatternSubscriptions(): int
    {
        return $this->queryExecutor->execute(['pubsub', 'numpat']);
    }

    /**
     * @link https://redis.io/commands/ping
     */
    public function ping(): void
    {
        $this->queryExecutor->execute(['ping'], toNull(...));
    }

    /**
     * @link https://redis.io/commands/quit
     */
    public function quit(): void
    {
        $this->queryExecutor->execute(['quit'], toNull(...));
    }

    /**
     * @link https://redis.io/commands/bgrewriteaof
     */
    public function rewriteAofAsync(): void
    {
        $this->queryExecutor->execute(['bgrewriteaof'], toNull(...));
    }

    /**
     * @link https://redis.io/commands/bgsave
     */
    public function saveAsync(): void
    {
        $this->queryExecutor->execute(['bgsave'], toNull(...));
    }

    /**
     * @link https://redis.io/commands/client-getname
     */
    public function getName(): string
    {
        return $this->queryExecutor->execute(['client', 'getname']);
    }

    /**
     * @link https://redis.io/commands/client-pause
     */
    public function pauseMillis(int $timeInMillis): void
    {
        $this->queryExecutor->execute(['client', 'pause', $timeInMillis], toNull(...));
    }

    /**
     * @link https://redis.io/commands/client-setname
     */
    public function setName(string $name): void
    {
        $this->queryExecutor->execute(['client', 'setname', $name], toNull(...));
    }

    /**
     * @link https://redis.io/commands/config-get
     */
    public function getConfig(string $parameter): array
    {
        return $this->queryExecutor->execute(['config', 'get', $parameter]);
    }

    /**
     * @link https://redis.io/commands/config-resetstat
     */
    public function resetStatistics(): void
    {
        $this->queryExecutor->execute(['config', 'resetstat'], toNull(...));
    }

    /**
     * @link https://redis.io/commands/config-rewrite
     */
    public function rewriteConfig(): void
    {
        $this->queryExecutor->execute(['config', 'rewrite'], toNull(...));
    }

    /**
     * @link https://redis.io/commands/config-set
     */
    public function setConfig(string $parameter, string $value): void
    {
        $this->queryExecutor->execute(['config', 'set', $parameter, $value], toNull(...));
    }

    /**
     * @link https://redis.io/commands/dbsize
     */
    public function getDatabaseSize(): int
    {
        return $this->queryExecutor->execute(['dbsize']);
    }

    /**
     * @link https://redis.io/commands/flushall
     */
    public function flushAll(): void
    {
        $this->queryExecutor->execute(['flushall'], toNull(...));
    }

    /**
     * @link https://redis.io/commands/flushdb
     */
    public function flushDatabase(): void
    {
        $this->queryExecutor->execute(['flushdb'], toNull(...));
    }

    /**
     * @link https://redis.io/commands/lastsave
     */
    public function getLastSave(): int
    {
        return $this->queryExecutor->execute(['lastsave']);
    }

    /**
     * @link https://redis.io/commands/role
     */
    public function getRole(): array
    {
        return $this->queryExecutor->execute(['role']);
    }

    /**
     * @link https://redis.io/commands/save
     */
    public function save(): void
    {
        $this->queryExecutor->execute(['save'], toNull(...));
    }

    /**
     * @link https://redis.io/commands/shutdown
     */
    public function shutdownWithSave(): string
    {
        return $this->queryExecutor->execute(['shutdown', 'save']);
    }

    /**
     * @link https://redis.io/commands/shutdown
     */
    public function shutdownWithoutSave(): string
    {
        return $this->queryExecutor->execute(['shutdown', 'nosave']);
    }

    /**
     * @link https://redis.io/commands/shutdown
     */
    public function shutdown(): string
    {
        return $this->queryExecutor->execute(['shutdown']);
    }

    /**
     * @link https://redis.io/commands/slaveof
     */
    public function enableReplication(string $host, int $port): void
    {
        $this->queryExecutor->execute(['slaveof', $host, $port], toNull(...));
    }

    /**
     * @link https://redis.io/commands/slaveof
     */
    public function disableReplication(): void
    {
        $this->queryExecutor->execute(['slaveof', 'no', 'one'], toNull(...));
    }

    /**
     * @param int $count
     *
     * @link https://redis.io/commands/slowlog
     */
    public function getSlowlog(?int $count = null): array
    {
        $payload = ['slowlog', 'get'];

        if ($count !== null) {
            $payload[] = $count;
        }

        return $this->queryExecutor->execute($payload);
    }

    /**
     * @link https://redis.io/commands/slowlog
     */
    public function getSlowlogLength(): int
    {
        return $this->queryExecutor->execute(['slowlog', 'len']);
    }

    /**
     * @link https://redis.io/commands/slowlog
     */
    public function resetSlowlog(): void
    {
        $this->queryExecutor->execute(['slowlog', 'reset'], toNull(...));
    }

    /**
     * @link https://redis.io/commands/time
     */
    public function getTime(): array
    {
        return $this->queryExecutor->execute(['time']);
    }

    /**
     * @link https://redis.io/commands/script-exists
     */
    public function hasScript(string $sha1): bool
    {
        return $this->queryExecutor->execute(['script', 'exists', $sha1], static function (array $array) {
            return (bool) $array[0];
        });
    }

    /**
     * @link https://redis.io/commands/script-flush
     */
    public function flushScripts(): void
    {
        $this->evalCache = []; // same as internal redis behavior

        $this->queryExecutor->execute(['script', 'flush'], toNull(...));
    }

    /**
     * @link https://redis.io/commands/script-kill
     */
    public function killScript(): void
    {
        $this->queryExecutor->execute(['script', 'kill'], toNull(...));
    }

    /**
     * @link https://redis.io/commands/script-load
     */
    public function loadScript(string $script): string
    {
        return $this->queryExecutor->execute(['script', 'load', $script]);
    }

    /**
     * @link https://redis.io/commands/echo
     */
    public function echo(string $text): string
    {
        return $this->queryExecutor->execute(['echo', $text]);
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
            $query = \array_merge(['evalsha', $sha1, \count($keys)], $keys, $args);

            return $this->queryExecutor->execute($query);
        } catch (QueryException $e) {
            if (\strtok($e->getMessage(), ' ') === 'NOSCRIPT') {
                $query = \array_merge(['eval', $script, \count($keys)], $keys, $args);
                return $this->queryExecutor->execute($query);
            }

            throw $e;
        }
    }

    public function select(int $database): void
    {
        $this->queryExecutor->execute(['select', $database], toNull(...));
    }
}
