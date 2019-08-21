<?php

namespace Amp\Redis;

use Amp\Promise;
use function Amp\call;

abstract class Redis
{
    /** @var string[] */
    private $evalCache = [];

    /**
     * @param string|string[] $arg
     * @param string[]        ...$args
     *
     * @return Promise
     */
    public function query($arg, ...$args): Promise
    {
        return $this->send(\array_merge((array) $arg, $args));
    }

    /**
     * @param string|string[] $key
     * @param string[]        ...$keys
     *
     * @return Promise
     * @yield int
     */
    public function del($key, ...$keys): Promise
    {
        return $this->send(\array_merge(['del'], (array) $key, $keys));
    }

    abstract protected function send(array $strings, callable $transform = null);

    /**
     * @param string $key
     *
     * @return Promise
     * @yield string
     */
    public function dump($key): Promise
    {
        return $this->send(['dump', $key]);
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield bool
     */
    public function exists($key): Promise
    {
        return $this->send(['exists', $key], static function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     * @param int    $seconds
     * @param bool   $inMillis
     *
     * @return Promise
     * @yield bool
     */
    public function expire($key, $seconds, $inMillis = false): Promise
    {
        $cmd = $inMillis ? 'pexpire' : 'expire';

        return $this->send([$cmd, $key, $seconds], static function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     * @param int    $timestamp
     * @param bool   $inMillis
     *
     * @return Promise
     * @yield bool
     */
    public function expireAt($key, $timestamp, $inMillis = false): Promise
    {
        $cmd = $inMillis ? 'pexpireat' : 'expireat';

        return $this->send([$cmd, $key, $timestamp], static function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $pattern
     *
     * @return Promise
     * @yield array
     */
    public function keys($pattern): Promise
    {
        return $this->send(['keys', $pattern]);
    }

    /**
     * @param string $key
     * @param int    $db
     *
     * @return Promise
     * @yield bool
     */
    public function move($key, $db): Promise
    {
        return $this->send(['move', $key, $db], static function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield int
     */
    public function objectRefcount($key): Promise
    {
        return $this->send(['object', 'refcount', $key]);
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield string
     */
    public function objectEncoding($key): Promise
    {
        return $this->send(['object', 'encoding', $key]);
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield int
     */
    public function objectIdletime($key): Promise
    {
        return $this->send(['object', 'idletime', $key]);
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield bool
     */
    public function persist($key): Promise
    {
        return $this->send(['persist', $key], static function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @return Promise
     * @yield string
     */
    public function randomKey(): Promise
    {
        return $this->send(['randomkey']);
    }

    /**
     * @param string $key
     * @param string $replacement
     * @param bool   $existingOnly
     *
     * @return Promise
     * @yield bool
     */
    public function rename($key, $replacement, $existingOnly = false): Promise
    {
        $cmd = $existingOnly ? 'renamenx' : 'rename';

        return $this->send([$cmd, $key, $replacement], static function ($response) use ($existingOnly) {
            return $existingOnly || (bool) $response;
        });
    }

    /**
     * @param string $key
     * @param string $serializedValue
     * @param int    $ttlMillis
     *
     * @return Promise
     * @yield string
     */
    public function restore($key, $serializedValue, $ttlMillis = 0): Promise
    {
        return $this->send(['restore', $key, $ttlMillis, $serializedValue]);
    }

    /**
     * @param string $cursor
     * @param string $pattern
     * @param int    $count
     *
     * @return Promise
     * @yield array
     */
    public function scan($cursor, $pattern = null, $count = null): Promise
    {
        $payload = ['scan', $cursor];

        if ($pattern !== null) {
            $payload[] = 'PATTERN';
            $payload[] = $pattern;
        }

        if ($count !== null) {
            $payload[] = 'COUNT';
            $payload[] = $count;
        }

        return $this->send($payload);
    }

    /**
     * @param string          $key
     * @param string          $pattern
     * @param string          $direction
     * @param string|string[] $get
     * @param int             $offset
     * @param int             $count
     * @param bool            $alpha
     * @param string          $store
     *
     * @return Promise
     * @yield array|int
     */
    public function sort($key, $pattern = null, $direction = null, $get = null, $offset = null, $count = null, $alpha = false, $store = null): Promise
    {
        $payload = ['sort', $key];

        if ($pattern !== null) {
            $payload[] = 'BY';
            $payload[] = $pattern;
        }

        if ($offset !== null && $count !== null) {
            $payload[] = 'LIMIT';
            $payload[] = $offset;
            $payload[] = $count;
        }

        if ($direction !== null) {
            $payload[] = $direction;
        }

        if ($get !== null) {
            $get = (array) $get;
            foreach ($get as $getPattern) {
                $payload[] = 'GET';
                $payload[] = $getPattern;
            }
        }

        if ($alpha) {
            $payload[] = 'ALPHA';
        }

        if ($store !== null) {
            $payload[] = 'STORE';
            $payload[] = $store;
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     * @param bool   $millis
     *
     * @return Promise
     * @yield int
     */
    public function ttl($key, $millis = false): Promise
    {
        $cmd = $millis ? 'pttl' : 'ttl';

        return $this->send([$cmd, $key]);
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield string
     */
    public function type($key): Promise
    {
        return $this->send(['type', $key]);
    }

    /**
     * @param string $key
     * @param string $value
     *
     * @return Promise
     * @yield int
     */
    public function append($key, $value): Promise
    {
        return $this->send(['append', $key, $value]);
    }

    /**
     * @param string   $key
     * @param int|null $start
     * @param int|null $end
     *
     * @return Promise
     */
    public function bitCount($key, $start = null, $end = null): Promise
    {
        $cmd = ['bitcount', $key];

        if (isset($start, $end)) {
            $cmd[] = $start;
            $cmd[] = $end;
        }

        return $this->send($cmd);
    }

    /**
     * @param string          $operation
     * @param string          $destination
     * @param string|string[] $key
     * @param string[]        ...$keys
     *
     * @return Promise
     * @yield int
     */
    public function bitOp($operation, $destination, $key, ...$keys): Promise
    {
        return $this->send(\array_merge(['bitop', $operation, $destination], (array) $key, $keys));
    }

    /**
     * @param string $key
     * @param int    $bit
     * @param int    $start
     * @param int    $end
     *
     * @return Promise
     * @yield int
     */
    public function bitPos($key, $bit, $start = null, $end = null): Promise
    {
        $payload = ['bitpos', $key, $bit];

        if ($start !== null) {
            $payload[] = $start;

            if ($end !== null) {
                $payload[] = $end;
            }
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     * @param int    $decrement
     *
     * @return Promise
     * @yield int
     */
    public function decr($key, $decrement = 1): Promise
    {
        if ($decrement === 1) {
            return $this->send(['decr', $key]);
        }

        return $this->send(['decrby', $key, $decrement]);
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield string
     */
    public function get($key): Promise
    {
        return $this->send(['get', $key]);
    }

    /**
     * @param string $key
     * @param int    $offset
     *
     * @return Promise
     * @yield int
     */
    public function getBit($key, $offset): Promise
    {
        return $this->send(['getbit', $key, $offset]);
    }

    /**
     * @param string $key
     * @param int    $start
     * @param int    $end
     *
     * @return Promise
     * @yield string
     */
    public function getRange($key, $start = 0, $end = -1): Promise
    {
        return $this->send(['getrange', $key, $start, $end]);
    }

    /**
     * @param string $key
     * @param string $value
     *
     * @return Promise
     * @yield string
     */
    public function getSet($key, $value): Promise
    {
        return $this->send(['getset', $key, $value]);
    }

    /**
     * @param string $key
     * @param int    $increment
     *
     * @return Promise
     * @yield int
     */
    public function incr($key, $increment = 1): Promise
    {
        if ($increment === 1) {
            return $this->send(['incr', $key]);
        }

        return $this->send(['incrby', $key, $increment]);
    }

    /**
     * @param string $key
     * @param float  $increment
     *
     * @return Promise
     * @yield float
     */
    public function incrByFloat($key, $increment): Promise
    {
        return $this->send(['incrbyfloat', $key, $increment], static function ($response) {
            return (float) $response;
        });
    }

    /**
     * @param string|string[] $key
     * @param string[]        ...$keys
     *
     * @return Promise
     * @yield array
     */
    public function mGet($key, ...$keys): Promise
    {
        return $this->send(\array_merge(['mget'], (array) $key, $keys));
    }

    /**
     * @param array $data
     * @param bool  $onlyIfNoneExists
     *
     * @return Promise
     * @yield bool
     */
    public function mSet(array $data, $onlyIfNoneExists = false): Promise
    {
        $payload = [$onlyIfNoneExists ? 'msetnx' : 'mset'];

        foreach ($data as $key => $value) {
            $payload[] = $key;
            $payload[] = $value;
        }

        return $this->send($payload, static function ($response) use ($onlyIfNoneExists) {
            return !$onlyIfNoneExists || (bool) $response;
        });
    }

    /**
     * @param string $key
     * @param string $value
     *
     * @return Promise
     * @yield bool
     */
    public function setNx($key, $value): Promise
    {
        return $this->set($key, $value, 0, false, 'NX');
    }

    /**
     * @param string $key
     * @param string $value
     * @param int    $expire
     * @param bool   $useMillis
     * @param string $existOption
     *
     * @return Promise
     * @yield bool
     */
    public function set($key, $value, $expire = 0, $useMillis = false, $existOption = null): Promise
    {
        $payload = ['set', $key, $value];

        if ($expire !== 0) {
            $payload[] = $useMillis ? 'PX' : 'EX';
            $payload[] = $expire;
        }

        if ($existOption !== null) {
            $payload[] = $existOption;
        }

        return $this->send($payload, static function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     * @param string $value
     *
     * @return Promise
     * @yield bool
     */
    public function setXx($key, $value): Promise
    {
        return $this->set($key, $value, 0, false, 'XX');
    }

    /**
     * @param string $key
     * @param int    $offset
     * @param bool   $value
     *
     * @return Promise
     * @yield int
     */
    public function setBit($key, $offset, $value): Promise
    {
        return $this->send(['setbit', $key, $offset, (int) $value]);
    }

    /**
     * @param $key
     * @param $offset
     * @param $value
     *
     * @return Promise
     * @yield int
     */
    public function setRange($key, $offset, $value): Promise
    {
        return $this->send(['setrange', $key, $offset, $value]);
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield int
     */
    public function strlen($key): Promise
    {
        return $this->send(['strlen', $key]);
    }

    /**
     * @param string          $key
     * @param string|string[] $field
     * @param string[]        ...$fields
     *
     * @return Promise
     * @yield int
     */
    public function hDel($key, $field, ...$fields): Promise
    {
        return $this->send(\array_merge(['hdel', $key], (array) $field, $fields));
    }

    /**
     * @param string $key
     * @param string $field
     *
     * @return Promise
     * @yield bool
     */
    public function hExists($key, $field): Promise
    {
        return $this->send(['hexists', $key, $field], static function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     * @param string $field
     *
     * @return Promise
     * @yield string
     */
    public function hGet($key, $field): Promise
    {
        return $this->send(['hget', $key, $field]);
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield array
     */
    public function hGetAll($key): Promise
    {
        return $this->send(['hgetall', $key], static function ($response) {
            if ($response === null) {
                return null;
            }

            $size = \count($response);
            $result = [];

            for ($i = 0; $i < $size; $i += 2) {
                $result[$response[$i]] = $response[$i + 1];
            }

            return $result;
        });
    }

    /**
     * @param string $key
     * @param string $field
     * @param int    $increment
     *
     * @return Promise
     * @yield int
     */
    public function hIncrBy($key, $field, $increment = 1): Promise
    {
        return $this->send(['hincrby', $key, $field, $increment]);
    }

    /**
     * @param string $key
     * @param string $field
     * @param float  $increment
     *
     * @return Promise
     * @yield float
     */
    public function hIncrByFloat($key, $field, $increment): Promise
    {
        return $this->send(['hincrbyfloat', $key, $field, $increment], static function ($response) {
            return (float) $response;
        });
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield array
     */
    public function hKeys($key): Promise
    {
        return $this->send(['hkeys', $key]);
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield int
     */
    public function hLen($key): Promise
    {
        return $this->send(['hlen', $key]);
    }

    /**
     * @param string          $key
     * @param string|string[] $field
     * @param string[]        ...$fields
     *
     * @return Promise
     * @yield array
     */
    public function hmGet($key, $field, ...$fields): Promise
    {
        return $this->send(\array_merge(['hmget', $key], (array) $field, $fields), static function ($response) {
            if ($response === null) {
                return null;
            }

            $size = \count($response);
            $result = [];

            for ($i = 0; $i < $size; $i += 2) {
                $result[$response[$i]] = $response[$i + 1];
            }

            return $result;
        });
    }

    /**
     * @param string $key
     * @param array  $data
     *
     * @return Promise
     * @yield string
     */
    public function hmSet($key, array $data): Promise
    {
        $array = ['hmset', $key];

        foreach ($data as $dataKey => $value) {
            $array[] = $dataKey;
            $array[] = $value;
        }

        return $this->send($array);
    }

    /**
     * @param string $key
     * @param string $cursor
     * @param string $pattern
     * @param int    $count
     *
     * @return Promise
     * @yield array
     */
    public function hScan($key, $cursor, $pattern = null, $count = null): Promise
    {
        return $this->_scan('hscan', $key, $cursor, $pattern, $count);
    }

    /**
     * @param string $key
     * @param string $field
     * @param string $value
     * @param bool   $notExistingOnly
     *
     * @return Promise
     * @yield bool
     */
    public function hSet($key, $field, $value, $notExistingOnly = false): Promise
    {
        $cmd = $notExistingOnly ? 'hsetnx' : 'hset';

        return $this->send([$cmd, $key, $field, $value], static function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     * @param string $index
     *
     * @return Promise
     * @yield string
     */
    public function lIndex($key, $index): Promise
    {
        return $this->send(['lindex', $key, $index]);
    }

    /**
     * @param string $key
     * @param string $relativePosition
     * @param string $pivot
     * @param string $value
     *
     * @return Promise
     * @yield int
     */
    public function lInsert($key, $relativePosition, $pivot, $value): Promise
    {
        $relativePosition = \strtolower($relativePosition);

        if ($relativePosition !== 'before' && $relativePosition !== 'after') {
            throw new \UnexpectedValueException(
                \sprintf("relativePosition must be 'before' or 'after', was '%s'", $relativePosition)
            );
        }

        return $this->send(['linsert', $key, $relativePosition, $pivot, $value]);
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield int
     */
    public function lLen($key): Promise
    {
        return $this->send(['llen', $key]);
    }

    /**
     * @param string|string[] $keys
     * @param int             $timeout
     *
     * @return Promise
     * @yield string
     */
    public function blPop($keys, $timeout = 0): Promise
    {
        return $this->send(\array_merge(['blpop'], (array) $keys, [$timeout]));
    }

    /**
     * @param string|string[] $keys
     * @param int             $timeout
     *
     * @return Promise
     * @yield string
     */
    public function brPop($keys, $timeout = 0): Promise
    {
        return $this->send(\array_merge(['brpop'], (array) $keys, [$timeout]));
    }

    /**
     * @param string $source
     * @param string $destination
     * @param int    $timeout
     *
     * @return Promise
     * @yield string
     */
    public function brPoplPush($source, $destination, $timeout = 0): Promise
    {
        return $this->send(['brpoplpush', $source, $destination, $timeout]);
    }

    /**
     * @param string|string[] $key
     * @param string[]        ...$keys
     *
     * @return Promise
     * @yield string
     */
    public function lPop($key, ...$keys): Promise
    {
        return $this->send(\array_merge(['lpop'], (array) $key, $keys));
    }

    /**
     * @param string          $key
     * @param string|string[] $value
     * @param string[]        ...$values
     *
     * @return Promise
     * @yield int
     */
    public function lPush($key, $value, ...$values): Promise
    {
        return $this->send(\array_merge(['lpush', $key], (array) $value, $values));
    }

    /**
     * @param string          $key
     * @param string|string[] $value
     * @param string[]        ...$values
     *
     * @return Promise
     * @yield int
     */
    public function lPushX($key, $value, ...$values): Promise
    {
        return $this->send(\array_merge(['lpushx', $key], (array) $value, $values));
    }

    /**
     * @param string $key
     * @param int    $start
     * @param int    $end
     *
     * @return Promise
     * @yield array
     */
    public function lRange($key, $start = 0, $end = -1): Promise
    {
        return $this->send(['lrange', $key, $start, $end]);
    }

    /**
     * @param string $key
     * @param string $value
     * @param int    $count
     *
     * @return Promise
     * @yield int
     */
    public function lRem($key, $value, $count = 0): Promise
    {
        return $this->send(['lrem', $key, $count, $value]);
    }

    /**
     * @param string $key
     * @param int    $index
     * @param string $value
     *
     * @return Promise
     * @yield string
     */
    public function lSet($key, $index, $value): Promise
    {
        return $this->send(['lset', $key, $index, $value]);
    }

    /**
     * @param string $key
     * @param int    $start
     * @param int    $stop
     *
     * @return Promise
     * @yield string
     */
    public function lTrim($key, $start = 0, $stop = -1): Promise
    {
        return $this->send(['ltrim', $key, $start, $stop]);
    }

    /**
     * @param string|string[] $key
     * @param string[]        ...$keys
     *
     * @return Promise
     * @yield string
     */
    public function rPop($key, ...$keys): Promise
    {
        return $this->send(\array_merge(['rpop'], (array) $key, $keys));
    }

    /**
     * @param string $source
     * @param string $destination
     *
     * @return Promise
     * @yield string
     */
    public function rPoplPush($source, $destination): Promise
    {
        return $this->send(['rpoplpush', $source, $destination]);
    }

    /**
     * @param string          $key
     * @param string|string[] $value
     * @param string[]        ...$values
     *
     * @return Promise
     * @yield int
     */
    public function rPush($key, $value, ...$values): Promise
    {
        return $this->send(\array_merge(['rpush', $key], (array) $value, $values));
    }

    /**
     * @param string          $key
     * @param string|string[] $value
     * @param string[]        ...$values
     *
     * @return Promise
     * @yield int
     */
    public function rPushX($key, $value, ...$values): Promise
    {
        return $this->send(\array_merge(['rpushx', $key], (array) $value, $values));
    }

    /**
     * @param string          $key
     * @param string|string[] $member
     * @param string[]        ...$members
     *
     * @return Promise
     * @yield int
     */
    public function sAdd($key, $member, ...$members): Promise
    {
        return $this->send(\array_merge(['sadd', $key], (array) $member, $members));
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield int
     */
    public function sCard($key): Promise
    {
        return $this->send(['scard', $key]);
    }

    /**
     * @param string|string[] $key
     * @param string[]        ...$keys
     *
     * @return Promise
     * @yield array
     */
    public function sDiff($key, ...$keys): Promise
    {
        return $this->send(\array_merge(['sdiff'], (array) $key, $keys));
    }

    /**
     * @param string          $destination
     * @param string|string[] $key
     * @param string[]        ...$keys
     *
     * @return Promise
     * @yield int
     */
    public function sDiffStore($destination, $key, ...$keys): Promise
    {
        return $this->send(\array_merge(['sdiffstore', $destination], (array) $key, $keys));
    }

    /**
     * @param string|string[] $key
     * @param string[]        ...$keys
     *
     * @return Promise
     * @yield array
     */
    public function sInter($key, ...$keys): Promise
    {
        return $this->send(\array_merge(['sinter'], (array) $key, $keys));
    }

    /**
     * @param string          $destination
     * @param string|string[] $key
     * @param string[]        ...$keys
     *
     * @return Promise
     * @yield int
     */
    public function sInterStore($destination, $key, ...$keys): Promise
    {
        return $this->send(\array_merge(['sinterstore', $destination], (array) $key, $keys));
    }

    /**
     * @param string $key
     * @param string $member
     *
     * @return Promise
     * @yield bool
     */
    public function sIsMember($key, $member): Promise
    {
        return $this->send(['sismember', $key, $member], static function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield array
     */
    public function sMembers($key): Promise
    {
        return $this->send(['smembers', $key]);
    }

    /**
     * @param string $source
     * @param string $destination
     * @param string $member
     *
     * @return Promise
     * @yield bool
     */
    public function sMove($source, $destination, $member): Promise
    {
        return $this->send(['smove', $source, $destination, $member], static function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield string
     */
    public function sPop($key): Promise
    {
        return $this->send(['spop', $key]);
    }

    /**
     * @param string $key
     * @param int    $count
     * @param bool   $distinctOnly
     *
     * @return Promise
     * @yield string|string[]
     */
    public function sRandMember($key, $count = null, $distinctOnly = true): Promise
    {
        $payload = ['srandmember', $key];

        if ($count !== null) {
            $payload[] = $distinctOnly ? $count : -$count;
        }

        return $this->send($payload);
    }

    /**
     * @param string          $key
     * @param string|string[] $member
     * @param string[]        ...$members
     *
     * @return Promise
     * @yield int
     */
    public function sRem($key, $member, ...$members): Promise
    {
        return $this->send(\array_merge(['srem', $key], (array) $member, $members));
    }

    /**
     * @param string $key
     * @param string $cursor
     * @param string $pattern
     * @param int    $count
     *
     * @return Promise
     * @yield array
     */
    public function sScan($key, $cursor, $pattern = null, $count = null): Promise
    {
        return $this->_scan('sscan', $key, $cursor, $pattern, $count);
    }

    /**
     * @param string|string[] $key
     * @param string[]        ...$keys
     *
     * @return Promise
     * @yield array
     */
    public function sUnion($key, ...$keys): Promise
    {
        return $this->send(\array_merge(['sunion'], (array) $key, $keys));
    }

    /**
     * @param string          $destination
     * @param string|string[] $key
     * @param string[]        ...$keys
     *
     * @return Promise
     * @yield int
     */
    public function sUnionStore($destination, $key, ...$keys): Promise
    {
        return $this->send(\array_merge(['sunionstore', $destination], (array) $key, $keys));
    }

    /**
     * @param string $key
     * @param array  $data
     *
     * @return Promise
     * @yield int
     */
    public function zAdd($key, array $data): Promise
    {
        $payload = ['zadd', $key];

        foreach ($data as $member => $score) {
            $payload[] = $score;
            $payload[] = $member;
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     *
     * @return Promise
     * @yield int
     */
    public function zCard($key): Promise
    {
        return $this->send(['zcard', $key]);
    }

    /**
     * @param string $key
     * @param int    $min
     * @param int    $max
     *
     * @return Promise
     * @yield int
     */
    public function zCount($key, $min, $max): Promise
    {
        return $this->send(['zcount', $key, $min, $max]);
    }

    /**
     * @param string    $key
     * @param string    $member
     * @param int|float $increment
     *
     * @return Promise
     * @yield float
     */
    public function zIncrBy($key, $member, $increment = 1): Promise
    {
        return $this->send(['zincrby', $key, $increment, $member], static function ($response) {
            return (float) $response;
        });
    }

    /**
     * @param string          $destination
     * @param int             $numkeys
     * @param string|string[] $keys
     * @param string          $aggregate
     *
     * @return Promise
     * @yield int
     */
    public function zInterStore($destination, $numkeys, $keys, $aggregate = 'sum'): Promise
    {
        $payload = ['zinterstore', $destination, $numkeys];

        $keys = (array) $keys;
        $weights = [];

        if (\count(\array_filter(\array_keys($keys), 'is_string'))) {
            foreach ($keys as $key => $weight) {
                $payload[] = $key;
                $weights[] = $weight;
            }
        } else {
            foreach ($keys as $key) {
                $payload[] = $key;
            }
        }

        if (\count($weights) > 0) {
            $payload[] = 'WEIGHTS';

            foreach ($weights as $weight) {
                $payload[] = $weight;
            }
        }

        if (\strtolower($aggregate) !== 'sum') {
            $payload[] = 'AGGREGATE';
            $payload[] = $aggregate;
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     * @param string $min
     * @param string $max
     *
     * @return Promise
     * @yield int
     */
    public function zLexCount($key, $min, $max): Promise
    {
        return $this->send(['zlexcount', $key, $min, $max]);
    }

    /**
     * @param string $key
     * @param int    $start
     * @param int    $stop
     * @param bool   $withScores
     *
     * @return Promise
     * @yield array
     */
    public function zRange($key, $start = 0, $stop = -1, $withScores = false): Promise
    {
        return $this->_zRange('zrange', $key, $start, $stop, $withScores);
    }

    /**
     * @param string $key
     * @param string $min
     * @param string $max
     * @param int    $offset
     * @param int    $count
     *
     * @return Promise
     * @yield array
     */
    public function zRangeByLex($key, $min, $max, $offset = null, $count = null): Promise
    {
        return $this->_zRangeByLex('zrangebylex', $key, $min, $max, $offset, $count);
    }

    /**
     * @param string     $key
     * @param string|int $min
     * @param string|int $max
     * @param bool       $withScores
     * @param int        $offset
     * @param int        $count
     *
     * @return Promise
     * @yield array
     */
    public function zRangeByScore($key, $min = 0, $max = -1, $withScores = false, $offset = null, $count = null): Promise
    {
        $payload = ['zrangebyscore', $key, $min, $max];

        if ($withScores) {
            $payload[] = 'WITHSCORES';
        }

        if ($offset !== null && $count !== null) {
            $payload[] = 'LIMIT';
            $payload[] = $offset;
            $payload[] = $count;
        }

        return $this->send($payload, static function ($response) use ($withScores) {
            if ($withScores) {
                $result = [];

                for ($i = 0, $count = \count($response); $i < $count; $i += 2) {
                    $result[$response[$i]] = $response[$i + 1];
                }

                return $result;
            }

            return $response;
        });
    }

    /**
     * @param string $key
     * @param string $member
     *
     * @return Promise
     * @yield int|null
     */
    public function zRank($key, $member): Promise
    {
        return $this->send(['zrank', $key, $member]);
    }

    /**
     * @param string          $key
     * @param string|string[] $member
     * @param string[]        ...$members
     *
     * @return Promise
     * @yield int
     */
    public function zRem($key, $member, ...$members): Promise
    {
        return $this->send(\array_merge(['zrem', $key], (array) $member, $members));
    }

    /**
     * @param string $key
     * @param string $min
     * @param string $max
     *
     * @return Promise
     * @yield int
     */
    public function zRemRangeByLex($key, $min, $max): Promise
    {
        return $this->send(['zremrangebylex', $key, $min, $max]);
    }

    /**
     * @param string $key
     * @param int    $start
     * @param int    $stop
     *
     * @return Promise
     * @yield int
     */
    public function zRemRangeByRank($key, $start, $stop): Promise
    {
        return $this->send(['zremrangebyrank', $key, $start, $stop]);
    }

    /**
     * @param string $key
     * @param int    $min
     * @param int    $max
     *
     * @return Promise
     * @yield int
     */
    public function zRemRangeByScore($key, $min, $max): Promise
    {
        return $this->send(['zremrangebyscore', $key, $min, $max]);
    }

    /**
     * @param string $key
     * @param int    $start
     * @param int    $stop
     * @param bool   $withScores
     *
     * @return Promise
     * @yield array
     */
    public function zRevRange($key, $start = 0, $stop = -1, $withScores = false): Promise
    {
        return $this->_zRange('zrevrange', $key, $start, $stop, $withScores);
    }

    /**
     * @param string $key
     * @param string $min
     * @param string $max
     * @param int    $offset
     * @param int    $count
     *
     * @return Promise
     * @yield array
     */
    public function zRevRangeByLex($key, $min, $max, $offset = null, $count = null): Promise
    {
        return $this->_zRangeByLex('zrevrangebylex', $key, $min, $max, $offset, $count);
    }

    /**
     * @param string     $key
     * @param string|int $min
     * @param string|int $max
     * @param bool       $withScores
     * @param int        $offset
     * @param int        $count
     *
     * @return Promise
     * @yield array
     */
    public function zRevRangeByScore($key, $min = 0, $max = -1, $withScores = false, $offset = null, $count = null): Promise
    {
        $payload = ['zrangebyscore', $key, $min, $max];

        if ($withScores) {
            $payload[] = 'WITHSCORES';
        }

        if ($offset !== null && $count !== null) {
            $payload[] = 'LIMIT';
            $payload[] = $offset;
            $payload[] = $count;
        }

        return $this->send($payload, static function ($response) use ($withScores) {
            if ($withScores) {
                $result = [];

                for ($i = 0, $count = \count($response); $i < $count; $i += 2) {
                    $result[$response[$i]] = $response[$i + 1];
                }

                return $result;
            }

            return $response;
        });
    }

    /**
     * @param string $key
     * @param string $member
     *
     * @return Promise
     * @yield int|null
     */
    public function zRevRank($key, $member): Promise
    {
        return $this->send(['zrevrank', $key, $member]);
    }

    /**
     * @param string $key
     * @param string $cursor
     * @param string $pattern
     * @param int    $count
     *
     * @return Promise
     * @yield array
     */
    public function zScan($key, $cursor, $pattern = null, $count = null): Promise
    {
        return $this->_scan('zscan', $key, $cursor, $pattern, $count);
    }

    /**
     * @param string $key
     * @param string $member
     *
     * @return Promise
     * @yield int|null
     */
    public function zScore($key, $member): Promise
    {
        return $this->send(['zscore', $key, $member]);
    }

    /**
     * @param string          $destination
     * @param int             $numkeys
     * @param string|string[] $keys
     * @param string          $aggregate
     *
     * @return Promise
     * @yield int
     */
    public function zUnionStore($destination, $numkeys, $keys, $aggregate = 'sum'): Promise
    {
        $payload = ['zunionstore', $destination, $numkeys];

        $keys = (array) $keys;
        $weights = [];

        if (\count(\array_filter(\array_keys($keys), 'is_string'))) {
            foreach ($keys as $key => $weight) {
                $payload[] = $key;
                $weights[] = $weight;
            }
        } else {
            foreach ($keys as $key) {
                $payload[] = $key;
            }
        }

        if (\count($weights) > 0) {
            $payload[] = 'WEIGHTS';

            foreach ($weights as $weight) {
                $payload[] = $weight;
            }
        }

        if (\strtolower($aggregate) !== 'sum') {
            $payload[] = 'AGGREGATE';
            $payload[] = $aggregate;
        }

        return $this->send($payload);
    }

    /**
     * @param string          $key
     * @param string|string[] $element
     * @param string[]        ...$elements
     *
     * @return Promise
     * @yield bool
     */
    public function pfAdd($key, $element, ...$elements): Promise
    {
        return $this->send(\array_merge(['pfadd', $key], (array) $element, $elements), static function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string|string[] $key
     * @param string[]        ...$keys
     *
     * @return Promise
     * @yield int
     */
    public function pfCount($key, ...$keys): Promise
    {
        return $this->send(\array_merge(['pfcount'], (array) $key, $keys));
    }

    /**
     * @param string          $destinationKey
     * @param string|string[] $sourceKey
     * @param string[]        ...$sourceKeys
     *
     * @return Promise
     * @yield string
     */
    public function pfMerge($destinationKey, $sourceKey, ...$sourceKeys): Promise
    {
        return $this->send(\array_merge(['pfmerge', $destinationKey], (array) $sourceKey, $sourceKeys));
    }

    /**
     * @param string $channel
     * @param string $message
     *
     * @return Promise
     * @yield int
     */
    public function publish($channel, $message): Promise
    {
        return $this->send(['publish', $channel, $message]);
    }

    /**
     * @param string $pattern
     *
     * @return Promise
     * @yield array
     */
    public function pubSubChannels($pattern = null): Promise
    {
        $payload = ['pubsub', 'channels'];

        if ($pattern !== null) {
            $payload[] = $pattern;
        }

        return $this->send($payload);
    }

    /**
     * @param string|string[] $channel
     * @param string[]        ...$channels
     *
     * @return Promise
     * @yield array
     */
    public function pubSubNumSub($channel = [], ...$channels): Promise
    {
        return $this->send(\array_merge(['pubsub', 'numsub'], (array) $channel, $channels), static function ($response) {
            $result = [];

            for ($i = 0, $count = \count($response); $i < $count; $i += 2) {
                $result[$response[$i]] = $response[$i + 1];
            }

            return $result;
        });
    }

    /**
     * @return Promise
     * @yield int
     */
    public function pubSubNumPat(): Promise
    {
        return $this->send(['pubsub', 'numpat']);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function ping(): Promise
    {
        return $this->send(['ping']);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function quit(): Promise
    {
        return $this->send(['quit']);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function bgRewriteAOF(): Promise
    {
        return $this->send(['bgrewriteaof']);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function bgSave(): Promise
    {
        return $this->send(['bgsave']);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function clientGetName(): Promise
    {
        return $this->send(['client', 'getname']);
    }

    /**
     * @param string[] ...$args
     *
     * @return Promise
     * @yield string|int
     */
    public function clientKill(...$args): Promise
    {
        return $this->send(\array_merge(['client', 'kill'], $args));
    }

    /**
     * @return Promise
     * @yield array
     */
    public function clientList(): Promise
    {
        return $this->send(['client', 'list'], static function ($response) {
            return \explode("\n", $response);
        });
    }

    /**
     * @param int $timeout
     *
     * @return Promise
     * @yield string
     */
    public function clientPause($timeout): Promise
    {
        return $this->send(['client', 'pause', $timeout]);
    }

    /**
     * @param string $name
     *
     * @return Promise
     * @yield string
     */
    public function clientSetName($name): Promise
    {
        return $this->send(['client', 'setname', $name]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function clusterSlots(): Promise
    {
        return $this->send(['cluster', 'slots']);
    }

    /**
     * @return Promise
     * @yield array
     */
    public function command(): Promise
    {
        return $this->send(['command']);
    }

    /**
     * @return Promise
     * @yield array
     */
    public function commandCount(): Promise
    {
        return $this->send(['command', 'count']);
    }

    /**
     * @param string[] ...$args
     *
     * @return Promise
     * @yield array
     */
    public function commandGetKeys(...$args): Promise
    {
        return $this->send(\array_merge(['command', 'getkeys'], $args));
    }

    /**
     * @param string|string[] $command
     * @param string[]        ...$commands
     *
     * @return Promise
     * @yield array
     */
    public function commandInfo($command, ...$commands): Promise
    {
        return $this->send(\array_merge(['command', 'info'], (array) $command, $commands));
    }

    /**
     * @param string $parameter
     *
     * @return Promise
     * @yield array
     */
    public function configGet($parameter): Promise
    {
        return $this->send(['config', 'get', $parameter]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function configResetStat(): Promise
    {
        return $this->send(['config', 'resetstat']);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function configRewrite(): Promise
    {
        return $this->send(['config', 'rewrite']);
    }

    /**
     * @param string $parameter
     * @param string $value
     *
     * @return Promise
     * @yield string
     */
    public function configSet($parameter, $value): Promise
    {
        return $this->send(['config', 'set', $parameter, $value]);
    }

    /**
     * @return Promise
     * @yield int
     */
    public function dbSize(): Promise
    {
        return $this->send(['dbsize']);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function flushAll(): Promise
    {
        return $this->send(['flushall']);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function flushDB(): Promise
    {
        return $this->send(['flushdb']);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function info(): Promise
    {
        return $this->send(['info']);
    }

    /**
     * @return Promise
     * @yield int
     */
    public function lastSave(): Promise
    {
        return $this->send(['lastsave']);
    }

    /**
     * @return Promise
     * @yield array
     */
    public function role(): Promise
    {
        return $this->send(['role']);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function save(): Promise
    {
        return $this->send(['save']);
    }

    /**
     * @param string $modifier
     *
     * @return Promise
     * @yield string
     */
    public function shutdown($modifier = null): Promise
    {
        $payload = ['shutdown'];

        if ($modifier !== null) {
            $payload[] = $modifier;
        }

        return $this->send($payload);
    }

    public function slaveOf($host, $port = null): void
    {
        if ($host === null) {
            $host = 'no';
            $port = 'one';
        }

        $this->send(['slaveof', $host, $port]);
    }

    /**
     * @param int $count
     *
     * @return Promise
     * @yield array
     */
    public function slowlogGet($count = null): Promise
    {
        $payload = ['slowlog', 'get'];

        if ($count !== null) {
            $payload[] = $count;
        }

        return $this->send($payload);
    }

    /**
     * @return Promise
     * @yield int
     */
    public function slowlogLen(): Promise
    {
        return $this->send(['slowlog', 'len']);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function slowlogReset(): Promise
    {
        return $this->send(['slowlog', 'reset']);
    }

    /**
     * @return Promise
     * @yield array
     */
    public function time(): Promise
    {
        return $this->send(['time']);
    }

    /**
     * @param string|string[] $script
     * @param string[]        ...$scripts
     *
     * @return Promise
     * @yield array
     */
    public function scriptExists($script, ...$scripts): Promise
    {
        return $this->send(\array_merge(['script', 'exists'], (array) $script, $scripts));
    }

    /**
     * @return Promise
     * @yield string
     */
    public function scriptFlush(): Promise
    {
        $this->evalCache = []; // same as internal redis behavior

        return $this->send(['script', 'flush']);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function scriptKill(): Promise
    {
        return $this->send(['script', 'kill']);
    }

    /**
     * @param string $script
     *
     * @return Promise
     * @yield string
     */
    public function scriptLoad($script): Promise
    {
        return $this->send(['script', 'load', $script]);
    }

    /**
     * @param string $text
     *
     * @return Promise
     * @yield string
     */
    public function echo($text): Promise
    {
        return $this->send(['echo', $text]);
    }

    /**
     * @param string          $script
     * @param string|string[] $keys
     * @param string|string[] $args
     *
     * @return Promise
     * @yield mixed
     */
    public function eval($script, $keys = [], $args = []): Promise
    {
        return call(function () use ($script, $keys, $args) {
            try {
                $sha1 = $this->evalCache[$script] ?? ($this->evalCache[$script] = \sha1($script));
                return yield $this->send(\array_merge(['evalsha', $sha1, \count((array) $keys)], (array) $keys, (array) $args));
            } catch (QueryException $e) {
                if (\strtok($e->getMessage(), ' ') === 'NOSCRIPT') {
                    return $this->send(\array_merge(['eval', $script, \count((array) $keys)], (array) $keys, (array) $args));
                }

                throw $e;
            }
        });
    }

    /**
     * @param string          $sha1
     * @param string|string[] $keys
     * @param string|string[] $args
     *
     * @return Promise
     * @yield mixed
     *
     * @deprecated Please use 'eval', which automatically attempts to use 'evalSha'.
     */
    public function evalSha($sha1, $keys = [], $args = []): Promise
    {
        \trigger_error("'evalSha' is deprecated. Please use 'eval', which automatically attempts to use 'evalSha'.");

        return $this->send(\array_merge(['evalsha', $sha1, \count((array) $keys)], (array) $keys, (array) $args));
    }

    private function _scan($command, $key, $cursor, $pattern = null, $count = null)
    {
        $payload = [$command, $key, $cursor];

        if ($pattern !== null) {
            $payload[] = 'PATTERN';
            $payload[] = $pattern;
        }

        if ($count !== null) {
            $payload[] = 'COUNT';
            $payload[] = $count;
        }

        return $this->send($payload);
    }

    private function _zRange($command, $key, $start = 0, $stop = -1, $withScores = false)
    {
        $payload = [$command, $key, $start, $stop];

        if ($withScores) {
            $payload[] = 'WITHSCORES';
        }

        return $this->send($payload, static function ($response) use ($withScores) {
            if ($withScores) {
                $result = [];

                for ($i = 0, $count = \count($response); $i < $count; $i += 2) {
                    $result[$response[$i]] = $response[$i + 1];
                }

                return $result;
            }

            return $response;
        });
    }

    private function _zRangeByLex($command, $key, $min, $max, $offset = null, $count = null)
    {
        $payload = [$command, $key, $min, $max];

        if ($offset !== null && $count !== null) {
            $payload[] = 'LIMIT';
            $payload[] = $offset;
            $payload[] = $count;
        }

        return $this->send($payload);
    }
}
