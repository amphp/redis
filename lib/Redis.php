<?php

namespace Amp\Redis;

use Amp\Promise;
use BadMethodCallException;
use function Amp\getReactor;

/**
 * @method Future echo (string $text)
 * @method Future eval(string $script, array $keys, array $args)
 */
abstract class Redis {
    /**
     * @param string|string[] $arg
     * @param string ...$args
     * @return Promise
     */
    public function query ($arg, ...$args) {
        return $this->send(array_merge((array) $arg, $args));
    }

    /**
     * @param string|string[] $key
     * @param string ...$keys
     * @return Promise
     * @yield int
     */
    public function del ($key, ...$keys) {
        return $this->send(array_merge(["del"], (array) $key, $keys));
    }

    protected abstract function send (array $strings, callable $transform = null);

    /**
     * @param string $key
     * @return Promise
     * @yield string
     */
    public function dump ($key) {
        return $this->send(["dump", $key]);
    }

    /**
     * @param string $key
     * @return Promise
     * @yield bool
     */
    public function exists ($key) {
        return $this->send(["exists", $key], function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     * @param int $seconds
     * @param bool $inMillis
     * @return Promise
     * @yield bool
     */
    public function expire ($key, $seconds, $inMillis = false) {
        $cmd = $inMillis ? "pexpire" : "expire";
        return $this->send([$cmd, $key, $seconds], function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     * @param int $timestamp
     * @param bool $inMillis
     * @return Promise
     * @yield bool
     */
    public function expireAt ($key, $timestamp, $inMillis = false) {
        $cmd = $inMillis ? "pexpireat" : "expireat";
        return $this->send([$cmd, $key, $timestamp], function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $pattern
     * @return Promise
     * @yield array
     */
    public function keys ($pattern) {
        return $this->send(["keys", $pattern]);
    }

    /**
     * @param string $key
     * @param int $db
     * @return Promise
     * @yield bool
     */
    public function move ($key, $db) {
        return $this->send(["move", $key, $db], function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     * @return Promise
     * @yield int
     */
    public function objectRefcount ($key) {
        return $this->send(["object", "refcount", $key]);
    }

    /**
     * @param string $key
     * @return Promise
     * @yield string
     */
    public function objectEncoding ($key) {
        return $this->send(["object", "encoding", $key]);
    }

    /**
     * @param string $key
     * @return Promise
     * @yield int
     */
    public function objectIdletime ($key) {
        return $this->send(["object", "idletime", $key]);
    }

    /**
     * @param string $key
     * @return Promise
     * @yield bool
     */
    public function persist ($key) {
        return $this->send(["persist", $key], function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @return Promise
     * @yield string
     */
    public function randomKey () {
        return $this->send(["randomkey"]);
    }

    /**
     * @param string $key
     * @param string $replacement
     * @param bool $existingOnly
     * @return Promise
     * @yield bool
     */
    public function rename ($key, $replacement, $existingOnly = false) {
        $cmd = $existingOnly ? "renamenx" : "rename";
        return $this->send([$cmd, $key, $replacement], function ($response) use ($existingOnly) {
            return $existingOnly || (bool) $response;
        });
    }

    /**
     * @param string $key
     * @param string $serializedValue
     * @param int $ttlMillis
     * @return Promise
     * @yield string
     */
    public function restore ($key, $serializedValue, $ttlMillis = 0) {
        return $this->send(["restore", $key, $ttlMillis, $serializedValue]);
    }

    /**
     * @param string $cursor
     * @param string $pattern
     * @param int $count
     * @return Promise
     * @yield array
     */
    public function scan ($cursor, $pattern = null, $count = null) {
        $payload = ["scan", $cursor];

        if ($pattern !== null) {
            $payload[] = "PATTERN";
            $payload[] = $pattern;
        }

        if ($count !== null) {
            $payload[] = "COUNT";
            $payload[] = $count;
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     * @param string $pattern
     * @param string $direction
     * @param string|string[] $get
     * @param int $offset
     * @param int $count
     * @param bool $alpha
     * @param string $store
     * @return Promise
     * @yield array|int
     */
    public function sort ($key, $pattern = null, $direction = null, $get = null, $offset = null, $count = null, $alpha = false, $store = null) {
        $payload = ["sort", $key];

        if ($pattern !== null) {
            $payload[] = "BY";
            $payload[] = $pattern;
        }

        if ($offset !== null && $count !== null) {
            $payload[] = "LIMIT";
            $payload[] = $offset;
            $payload[] = $count;
        }

        if ($direction !== null) {
            $payload[] = $direction;
        }

        if ($get !== null) {
            $get = (array) $get;
            foreach ($get as $pattern) {
                $payload[] = "GET";
                $payload[] = $pattern;
            }
        }

        if ($alpha) {
            $payload[] = "ALPHA";
        }

        if ($store !== null) {
            $payload[] = "STORE";
            $payload[] = $store;
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     * @param bool $millis
     * @return Promise
     * @yield int
     */
    public function ttl ($key, $millis = false) {
        $cmd = $millis ? "pttl" : "ttl";
        return $this->send([$cmd, $key]);
    }

    /**
     * @param string $key
     * @return Promise
     * @yield string
     */
    public function type ($key) {
        return $this->send(["type", $key]);
    }

    /**
     * @param string $key
     * @param string $value
     * @return Promise
     * @yield int
     */
    public function append ($key, $value) {
        return $this->send(["append", $key, $value]);
    }

    /**
     * @param string $key
     * @param int|null $start
     * @param int|null $end
     * @return Promise
     */
    public function bitCount ($key, $start = null, $end = null) {
        $cmd = ["bitcount", $key];

        if (isset($start, $end)) {
            $cmd[] = $start;
            $cmd[] = $end;
        }

        return $this->send($cmd);
    }

    /**
     * @param string $operation
     * @param string $destination
     * @param string|string[] $key
     * @param string ...$keys
     * @return Promise
     * @yield int
     */
    public function bitOp ($operation, $destination, $key, ...$keys) {
        return $this->send(array_merge(["bitop", $operation, $destination], (array) $key, $keys));
    }

    /**
     * @param string $key
     * @param int $bit
     * @param int $start
     * @param int $end
     * @return Promise
     * @yield int
     */
    public function bitPos ($key, $bit, $start = null, $end = null) {
        $payload = ["bitpos", $key, $bit];

        if ($start != null) {
            $payload[] = $start;

            if ($end != null) {
                $payload[] = $end;
            }
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     * @param int $decrement
     * @return Promise
     * @yield int
     */
    public function decr ($key, $decrement = 1) {
        if ($decrement === 1) {
            return $this->send(["decr", $key]);
        } else {
            return $this->send(["decrby", $key, $decrement]);
        }
    }

    /**
     * @param string $key
     * @return Promise
     * @yield string
     */
    public function get ($key) {
        return $this->send(["get", $key]);
    }

    /**
     * @param string $key
     * @param int $offset
     * @return Promise
     * @yield int
     */
    public function getBit ($key, $offset) {
        return $this->send(["getbit", $key, $offset]);
    }

    /**
     * @param string $key
     * @param int $start
     * @param int $end
     * @return Promise
     * @yield string
     */
    public function getRange ($key, $start = 0, $end = -1) {
        return $this->send(["getrange", $key, $start, $end]);
    }

    /**
     * @param string $key
     * @param string $value
     * @return Promise
     * @yield string
     */
    public function getSet ($key, $value) {
        return $this->send(["getset", $key, $value]);
    }

    /**
     * @param string $key
     * @param int $increment
     * @return Promise
     * @yield int
     */
    public function incr ($key, $increment = 1) {
        if ($increment === 1) {
            return $this->send(["incr", $key]);
        } else {
            return $this->send(["incrby", $key, $increment]);
        }
    }

    /**
     * @param string $key
     * @param float $increment
     * @return Promise
     * @yield float
     */
    public function incrByFloat ($key, $increment) {
        return $this->send(["incrbyfloat", $key, $increment], function ($response) {
            return (float) $response;
        });
    }

    /**
     * @param string|string[] $key
     * @param string ...$keys
     * @return Promise
     * @yield array
     */
    public function mGet ($key, ...$keys) {
        return $this->send(array_merge(["mget"], (array) $key, $keys));
    }

    /**
     * @param array $data
     * @param bool $onlyIfNoneExists
     * @return Promise
     * @yield bool
     */
    public function mSet (array $data, $onlyIfNoneExists = false) {
        $payload = [$onlyIfNoneExists ? "msetnx" : "mset"];

        foreach ($data as $key => $value) {
            $payload[] = $key;
            $payload[] = $value;
        }

        return $this->send($payload, function ($response) use ($onlyIfNoneExists) {
            return !$onlyIfNoneExists || (bool) $response;
        });
    }

    /**
     * @param string $key
     * @param string $value
     * @return Promise
     * @yield bool
     */
    public function setNx ($key, $value) {
        return $this->set($key, $value, 0, false, "NX");
    }

    /**
     * @param string $key
     * @param string $value
     * @param int $expire
     * @param bool $useMillis
     * @param string $existOption
     * @return Promise
     * @yield bool
     */
    public function set ($key, $value, $expire = 0, $useMillis = false, $existOption = null) {
        $payload = ["set", $key, $value];

        if ($expire !== 0) {
            $payload[] = $useMillis ? "PX" : "EX";
            $payload[] = $expire;
        }

        if ($existOption !== null) {
            $payload[] = $existOption;
        }

        return $this->send($payload, function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     * @param string $value
     * @return Promise
     * @yield bool
     */
    public function setXx ($key, $value) {
        return $this->set($key, $value, 0, false, "XX");
    }

    /**
     * @param string $key
     * @param int $offset
     * @param bool $value
     * @return Promise
     * @yield int
     */
    public function setBit ($key, $offset, $value) {
        return $this->send(["setbit", $key, $offset, (int) $value]);
    }

    /**
     * @param $key
     * @param $offset
     * @param $value
     * @return Promise
     * @yield int
     */
    public function setRange ($key, $offset, $value) {
        return $this->send(["setrange", $key, $offset, $value]);
    }

    /**
     * @param string $key
     * @return Promise
     * @yield int
     */
    public function strlen ($key) {
        return $this->send(["strlen", $key]);
    }

    /**
     * @param string $key
     * @param string|string[] $field
     * @param string ...$fields
     * @return Promise
     * @yield int
     */
    public function hDel ($key, $field, ...$fields) {
        return $this->send(array_merge(["hdel", $key], (array) $field, $fields));
    }

    /**
     * @param string $key
     * @param string $field
     * @return Promise
     * @yield bool
     */
    public function hExists ($key, $field) {
        return $this->send(["hexists", $key, $field], function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     * @param string $field
     * @return Promise
     * @yield string
     */
    public function hGet ($key, $field) {
        return $this->send(["hget", $key, $field]);
    }

    /**
     * @param string $key
     * @return Promise
     * @yield array
     */
    public function hGetAll ($key) {
        return $this->send(["hgetall", $key], function ($response) {
            if ($response === null) {
                return null;
            }

            $size = sizeof($response);
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
     * @param int $increment
     * @return Promise
     * @yield int
     */
    public function hIncrBy ($key, $field, $increment = 1) {
        return $this->send(["hincrby", $key, $field, $increment]);
    }

    /**
     * @param string $key
     * @param string $field
     * @param float $increment
     * @return Promise
     * @yield float
     */
    public function hIncrByFloat ($key, $field, $increment) {
        return $this->send(["hincrbyfloat", $key, $field, $increment], function ($response) {
            return (float) $response;
        });
    }

    /**
     * @param string $key
     * @return Promise
     * @yield array
     */
    public function hKeys ($key) {
        return $this->send(["hkeys", $key]);
    }

    /**
     * @param string $key
     * @return Promise
     * @yield int
     */
    public function hLen ($key) {
        return $this->send(["hlen", $key]);
    }

    /**
     * @param string $key
     * @param string|string[] $field
     * @param string ...$fields
     * @return Promise
     * @yield array
     */
    public function hmGet ($key, $field, ...$fields) {
        return $this->send(array_merge(["hmget", $key], (array) $field, $fields), function ($response) {
            if ($response === null) {
                return null;
            }

            $size = sizeof($response);
            $result = [];

            for ($i = 0; $i < $size; $i += 2) {
                $result[$response[$i]] = $response[$i + 1];
            }

            return $result;
        });
    }

    /**
     * @param string $key
     * @param array $data
     * @return Promise
     * @yield string
     */
    public function hmSet ($key, array $data) {
        $array = ["hmset", $key];

        foreach ($data as $key => $value) {
            $array[] = $key;
            $array[] = $value;
        }

        return $this->send($array);
    }

    /**
     * @param string $key
     * @param string $cursor
     * @param string $pattern
     * @param int $count
     * @return Promise
     * @yield array
     */
    public function hScan ($key, $cursor, $pattern = null, $count = null) {
        $payload = ["hscan", $key, $cursor];

        if ($pattern !== null) {
            $payload[] = "PATTERN";
            $payload[] = $pattern;
        }

        if ($count !== null) {
            $payload[] = "COUNT";
            $payload[] = $count;
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     * @param string $field
     * @param string $value
     * @param bool $notExistingOnly
     * @return Promise
     * @yield bool
     */
    public function hSet ($key, $field, $value, $notExistingOnly = false) {
        $cmd = $notExistingOnly ? "hsetnx" : "hset";
        return $this->send([$cmd, $key, $field, $value], function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     * @param string $index
     * @return Promise
     * @yield string
     */
    public function lIndex ($key, $index) {
        return $this->send(["lindex", $key, $index]);
    }

    /**
     * @param string $key
     * @param string $relativePosition
     * @param string $pivot
     * @param string $value
     * @return Promise
     * @yield int
     */
    public function lInsert ($key, $relativePosition, $pivot, $value) {
        $relativePosition = strtolower($relativePosition);

        if ($relativePosition !== "before" && $relativePosition !== "after") {
            throw new \UnexpectedValueException(
                sprintf("relativePosition must be 'before' or 'after', was '%s'", $relativePosition)
            );
        }

        return $this->send(["linsert", $key, $relativePosition, $pivot, $value]);
    }

    /**
     * @param string $key
     * @return Promise
     * @yield int
     */
    public function lLen ($key) {
        return $this->send(["llen", $key]);
    }

    /**
     * @param string|string[] $keys
     * @param int $timeout
     * @return Promise
     * @yield string
     */
    public function blPop ($keys, $timeout = 0) {
        return $this->send(array_merge(["blpop"], (array) $keys, [$timeout]));
    }

    /**
     * @param string|string[] $keys
     * @param int $timeout
     * @return Promise
     * @yield string
     */
    public function brPop ($keys, $timeout = 0) {
        return $this->send(array_merge(["brpop"], (array) $keys, [$timeout]));
    }

    /**
     * @param string $source
     * @param string $destination
     * @param int $timeout
     * @return Promise
     * @yield string
     */
    public function brPoplPush ($source, $destination, $timeout = 0) {
        return $this->send(["brpoplpush", $source, $destination, $timeout]);
    }

    /**
     * @param string|string[] $key
     * @param string ...$keys
     * @return Promise
     * @yield string
     */
    public function lPop ($key, ...$keys) {
        return $this->send(array_merge(["lpop"], (array) $key, $keys));
    }

    /**
     * @param string $key
     * @param string|string[] $value
     * @param string ...$values
     * @return Promise
     * @yield int
     */
    public function lPush ($key, $value, ...$values) {
        return $this->send(array_merge(["lpush", $key], (array) $value, $values));
    }

    /**
     * @param string $key
     * @param string|string[] $value
     * @param string ...$values
     * @return Promise
     * @yield int
     */
    public function lPushX ($key, $value, ...$values) {
        return $this->send(array_merge(["lpushx", $key], (array) $value, $values));
    }

    /**
     * @param string $key
     * @param int $start
     * @param int $end
     * @return Promise
     * @yield array
     */
    public function lRange ($key, $start = 0, $end = -1) {
        return $this->send(["lrange", $key, $start, $end]);
    }

    /**
     * @param string $key
     * @param string $value
     * @param int $count
     * @return Promise
     * @yield int
     */
    public function lRem ($key, $value, $count = 0) {
        return $this->send(["lrem", $key, $count, $value]);
    }

    /**
     * @param string $key
     * @param int $index
     * @param string $value
     * @return Promise
     * @yield string
     */
    public function lSet ($key, $index, $value) {
        return $this->send(["lset", $key, $index, $value]);
    }

    /**
     * @param string $key
     * @param int $start
     * @param int $stop
     * @return Promise
     * @yield string
     */
    public function lTrim ($key, $start = 0, $stop = -1) {
        return $this->send(["ltrim", $key, $start, $stop]);
    }

    /**
     * @param string|string[] $key
     * @param string ...$keys
     * @return Promise
     * @yield string
     */
    public function rPop ($key, ...$keys) {
        return $this->send(array_merge(["rpop"], (array) $key, $keys));
    }

    /**
     * @param string $source
     * @param string $destination
     * @return Promise
     * @yield string
     */
    public function rPoplPush ($source, $destination) {
        return $this->send(["rpoplpush", $source, $destination]);
    }

    /**
     * @param string $key
     * @param string|string[] $value
     * @param string ...$values
     * @return Promise
     * @yield int
     */
    public function rPush ($key, $value, ...$values) {
        return $this->send(array_merge(["rpush", $key], (array) $value, $values));
    }

    /**
     * @param string $key
     * @param string|string[] $value
     * @param string ...$values
     * @return Promise
     * @yield int
     */
    public function rPushX ($key, $value, ...$values) {
        return $this->send(array_merge(["rpushx", $key], (array) $value, $values));
    }

    /**
     * @param string $key
     * @param string|string[] $member
     * @param string ...$members
     * @return Promise
     * @yield int
     */
    public function sAdd ($key, $member, ...$members) {
        return $this->send(array_merge(["sadd", $key], (array) $member, $members));
    }

    /**
     * @param string $key
     * @return Promise
     * @yield int
     */
    public function sCard ($key) {
        return $this->send(["scard", $key]);
    }

    /**
     * @param string|string[] $key
     * @param string ...$keys
     * @return Promise
     * @yield array
     */
    public function sDiff ($key, ...$keys) {
        return $this->send(array_merge(["sdiff"], (array) $key, $keys));
    }

    /**
     * @param string $destination
     * @param string|string[] $key
     * @param string ...$keys
     * @return Promise
     * @yield int
     */
    public function sDiffStore ($destination, $key, ...$keys) {
        return $this->send(array_merge(["sdiffstore", $destination], (array) $key, $keys));
    }

    /**
     * @param string|string[] $key
     * @param string ...$keys
     * @return Promise
     * @yield array
     */
    public function sInter ($key, ...$keys) {
        return $this->send(array_merge(["sinter"], (array) $key, $keys));
    }

    /**
     * @param string $destination
     * @param string|string[] $key
     * @param string ...$keys
     * @return Promise
     * @yield int
     */
    public function sInterStore ($destination, $key, ...$keys) {
        return $this->send(array_merge(["sinterstore", $destination], (array) $key, $keys));
    }

    /**
     * @param string $key
     * @param string $member
     * @return Promise
     * @yield bool
     */
    public function sIsMember ($key, $member) {
        return $this->send(["sismember", $key, $member], function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     * @return Promise
     * @yield array
     */
    public function sMembers ($key) {
        return $this->send(["smembers", $key]);
    }

    /**
     * @param string $source
     * @param string $destination
     * @param string $member
     * @return Promise
     * @yield bool
     */
    public function sMove ($source, $destination, $member) {
        return $this->send(["smove", $source, $destination, $member], function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string $key
     * @return Promise
     * @yield string
     */
    public function sPop ($key) {
        return $this->send(["spop", $key]);
    }

    /**
     * @param string $key
     * @param int $count
     * @param bool $distinctOnly
     * @return Promise
     * @yield string|string[]
     */
    public function sRandMember ($key, $count = null, $distinctOnly = true) {
        $payload = ["srandmember", $key];

        if ($count !== null) {
            $payload[] = $distinctOnly ? $count : -$count;
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     * @param string|string[] $member
     * @param string ...$members
     * @return Promise
     * @yield int
     */
    public function sRem ($key, $member, ...$members) {
        return $this->send(array_merge(["srem", $key], (array) $member, $members));
    }

    /**
     * @param string $key
     * @param string $cursor
     * @param string $pattern
     * @param int $count
     * @return Promise
     * @yield array
     */
    public function sScan ($key, $cursor, $pattern = null, $count = null) {
        $payload = ["sscan", $key, $cursor];

        if ($pattern !== null) {
            $payload[] = "PATTERN";
            $payload[] = $pattern;
        }

        if ($count !== null) {
            $payload[] = "COUNT";
            $payload[] = $count;
        }

        return $this->send($payload);
    }

    /**
     * @param string|string[] $key
     * @param string ...$keys
     * @return Promise
     * @yield array
     */
    public function sUnion ($key, ...$keys) {
        return $this->send(array_merge(["sunion"], (array) $key, $keys));
    }

    /**
     * @param string $destination
     * @param string|string[] $key
     * @param string ...$keys
     * @return Promise
     * @yield int
     */
    public function sUnionStore ($destination, $key, ...$keys) {
        return $this->send(array_merge(["sunionstore", $destination], (array) $key, $keys));
    }

    /**
     * @param string $key
     * @param array $data
     * @return Promise
     * @yield int
     */
    public function zAdd ($key, array $data) {
        $payload = ["zadd", $key];

        foreach ($data as $member => $score) {
            $payload[] = $score;
            $payload[] = $member;
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     * @return Promise
     * @yield int
     */
    public function zCard ($key) {
        return $this->send(["zcard", $key]);
    }

    /**
     * @param string $key
     * @param int $min
     * @param int $max
     * @return Promise
     * @yield int
     */
    public function zCount ($key, $min, $max) {
        return $this->send(["zcount", $key, $min, $max]);
    }

    /**
     * @param string $key
     * @param string $member
     * @param int|float $increment
     * @return Promise
     * @yield float
     */
    public function zIncrBy ($key, $member, $increment = 1) {
        return $this->send(["zincrby", $key, $increment, $member], function ($response) {
            return (float) $response;
        });
    }

    /**
     * @param string $destination
     * @param int $numkeys
     * @param string|string[] $keys
     * @param string $aggregate
     * @return Promise
     * @yield int
     */
    public function zInterStore ($destination, $numkeys, $keys, $aggregate = "sum") {
        $payload = ["zinterstore", $destination, $numkeys];

        $keys = (array) $keys;
        $weights = [];

        if (count(array_filter(array_keys($keys), 'is_string'))) {
            foreach ($keys as $key => $weight) {
                $payload[] = $key;
                $weights[] = $weight;
            }
        } else {
            foreach ($keys as $key) {
                $payload[] = $key;
            }
        }

        if (sizeof($weights) > 0) {
            $payload[] = "WEIGHTS";

            foreach ($weights as $weight) {
                $payload[] = $weight;
            }
        }

        if (strtolower($aggregate) !== "sum") {
            $payload[] = "AGGREGATE";
            $payload[] = $aggregate;
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     * @param string $min
     * @param string $max
     * @return Promise
     * @yield int
     */
    public function zLexCount ($key, $min, $max) {
        return $this->send(["zlexcount", $key, $min, $max]);
    }

    /**
     * @param string $key
     * @param int $start
     * @param int $stop
     * @param bool $withScores
     * @return Promise
     * @yield array
     */
    public function zRange ($key, $start = 0, $stop = -1, $withScores = false) {
        $payload = ["zrange", $key, $start, $stop];

        if ($withScores) {
            $payload[] = "WITHSCORES";
        }

        return $this->send($payload, function ($response) use ($withScores) {
            if ($withScores) {
                $result = [];

                for ($i = 0; $i < sizeof($response); $i += 2) {
                    $result[$response[$i]] = $response[$i + 1];
                }

                return $result;
            } else {
                return $response;
            }
        });
    }

    /**
     * @param string $key
     * @param string $min
     * @param string $max
     * @param int $offset
     * @param int $count
     * @return Promise
     * @yield array
     */
    public function zRangeByLex ($key, $min, $max, $offset = null, $count = null) {
        $payload = ["zrangebylex", $key, $min, $max];

        if ($offset !== null && $count !== null) {
            $payload[] = "LIMIT";
            $payload[] = $offset;
            $payload[] = $count;
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     * @param string|int $min
     * @param string|int $max
     * @param bool $withScores
     * @param int $offset
     * @param int $count
     * @return Promise
     * @yield array
     */
    public function zRangeByScore ($key, $min = 0, $max = -1, $withScores = false, $offset = null, $count = null) {
        $payload = ["zrangebyscore", $key, $min, $max];

        if ($withScores) {
            $payload[] = "WITHSCORES";
        }

        if ($offset !== null && $count !== null) {
            $payload[] = "LIMIT";
            $payload[] = $offset;
            $payload[] = $count;
        }

        return $this->send($payload, function ($response) use ($withScores) {
            if ($withScores) {
                $result = [];

                for ($i = 0; $i < sizeof($response); $i += 2) {
                    $result[$response[$i]] = $response[$i + 1];
                }

                return $result;
            } else {
                return $response;
            }
        });
    }

    /**
     * @param string $key
     * @param string $member
     * @return Promise
     * @yield int|null
     */
    public function zRank ($key, $member) {
        return $this->send(["zrank", $key, $member]);
    }

    /**
     * @param string $key
     * @param string|string[] $member
     * @param string ...$members
     * @return Promise
     * @yield int
     */
    public function zRem ($key, $member, ...$members) {
        return $this->send(array_merge(["zrem", $key], (array) $member, $members));
    }

    /**
     * @param string $key
     * @param string $min
     * @param string $max
     * @return Promise
     * @yield int
     */
    public function zRemRangeByLex ($key, $min, $max) {
        return $this->send(["zremrangebylex", $key, $min, $max]);
    }

    /**
     * @param string $key
     * @param int $start
     * @param int $stop
     * @return Promise
     * @yield int
     */
    public function zRemRangeByRank ($key, $start, $stop) {
        return $this->send(["zremrangebyrank", $key, $start, $stop]);
    }

    /**
     * @param string $key
     * @param int $min
     * @param int $max
     * @return Promise
     * @yield int
     */
    public function zRemRangeByScore ($key, $min, $max) {
        return $this->send(["zremrangebyscore", $key, $min, $max]);
    }

    /**
     * @param string $key
     * @param int $min
     * @param int $max
     * @param bool $withScores
     * @return Promise
     * @yield array
     */
    public function zRevRange ($key, $min = 0, $max = -1, $withScores = false) {
        $payload = ["zrevrange", $key, $min, $max];

        if ($withScores) {
            $payload[] = "WITHSCORES";
        }

        return $this->send($payload, function ($response) use ($withScores) {
            if ($withScores) {
                $result = [];

                for ($i = 0; $i < sizeof($response); $i += 2) {
                    $result[$response[$i]] = $response[$i + 1];
                }

                return $result;
            } else {
                return $response;
            }
        });
    }

    /**
     * @param string $key
     * @param string $min
     * @param string $max
     * @param int $offset
     * @param int $count
     * @return Promise
     * @yield array
     */
    public function zRevRangeByLex ($key, $min, $max, $offset = null, $count = null) {
        $payload = ["zrevrangebylex", $key, $min, $max];

        if ($offset !== null && $count !== null) {
            $payload[] = "LIMIT";
            $payload[] = $offset;
            $payload[] = $count;
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     * @param string|int $min
     * @param string|int $max
     * @param bool $withScores
     * @param int $offset
     * @param int $count
     * @return Promise
     * @yield array
     */
    public function zRevRangeByScore ($key, $min = 0, $max = -1, $withScores = false, $offset = null, $count = null) {
        $payload = ["zrangebyscore", $key, $min, $max];

        if ($withScores) {
            $payload[] = "WITHSCORES";
        }

        if ($offset !== null && $count !== null) {
            $payload[] = "LIMIT";
            $payload[] = $offset;
            $payload[] = $count;
        }

        return $this->send($payload, function ($response) use ($withScores) {
            if ($withScores) {
                $result = [];

                for ($i = 0; $i < sizeof($response); $i += 2) {
                    $result[$response[$i]] = $response[$i + 1];
                }

                return $result;
            } else {
                return $response;
            }
        });
    }

    /**
     * @param string $key
     * @param string $member
     * @return Promise
     * @yield int|null
     */
    public function zRevRank ($key, $member) {
        return $this->send(["zrevrank", $key, $member]);
    }

    /**
     * @param string $key
     * @param string $cursor
     * @param string $pattern
     * @param int $count
     * @return Promise
     * @yield array
     */
    public function zScan ($key, $cursor, $pattern = null, $count = null) {
        $payload = ["zscan", $key, $cursor];

        if ($pattern !== null) {
            $payload[] = "PATTERN";
            $payload[] = $pattern;
        }

        if ($count !== null) {
            $payload[] = "COUNT";
            $payload[] = $count;
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     * @param string $member
     * @return Promise
     * @yield int|null
     */
    public function zScore ($key, $member) {
        return $this->send(["zscore", $key, $member]);
    }

    /**
     * @param string $destination
     * @param int $numkeys
     * @param string|string[] $keys
     * @param string $aggregate
     * @return Promise
     * @yield int
     */
    public function zUnionStore ($destination, $numkeys, $keys, $aggregate = "sum") {
        $payload = ["zunionstore", $destination, $numkeys];

        $keys = (array) $keys;
        $weights = [];

        if (count(array_filter(array_keys($keys), 'is_string'))) {
            foreach ($keys as $key => $weight) {
                $payload[] = $key;
                $weights[] = $weight;
            }
        } else {
            foreach ($keys as $key) {
                $payload[] = $key;
            }
        }

        if (sizeof($weights) > 0) {
            $payload[] = "WEIGHTS";

            foreach ($weights as $weight) {
                $payload[] = $weight;
            }
        }

        if (strtolower($aggregate) !== "sum") {
            $payload[] = "AGGREGATE";
            $payload[] = $aggregate;
        }

        return $this->send($payload);
    }

    /**
     * @param string $key
     * @param string|string[] $element
     * @param string ...$elements
     * @return Promise
     * @yield bool
     */
    public function pfAdd ($key, $element, ...$elements) {
        return $this->send(array_merge(["pfadd", $key], (array) $element, $elements), function ($response) {
            return (bool) $response;
        });
    }

    /**
     * @param string|string[] $key
     * @param string ...$keys
     * @return Promise
     * @yield int
     */
    public function pfCount ($key, ...$keys) {
        return $this->send(array_merge(["pfcount"], (array) $key, $keys));
    }

    /**
     * @param string $destinationKey
     * @param string|string[] $sourceKey
     * @param string ...$sourceKeys
     * @return Promise
     * @yield string
     */
    public function pfMerge ($destinationKey, $sourceKey, ...$sourceKeys) {
        return $this->send(array_merge(["pfmerge", $destinationKey], (array) $sourceKey, $sourceKeys));
    }

    /**
     * @param $channel
     * @param $message
     * @return Promise
     * @yield int
     */
    public function publish ($channel, $message) {
        return $this->send(["publish", $channel, $message]);
    }

    /**
     * @param string $pattern
     * @return Promise
     * @yield array
     */
    public function pubSubChannels ($pattern = null) {
        $payload = ["pubsub", "channels"];

        if ($pattern !== null) {
            $payload[] = $pattern;
        }

        return $this->send($payload);
    }

    /**
     * @param string|string[] $channel
     * @param string ...$channels
     * @return Promise
     * @yield array
     */
    public function pubSubNumSub ($channel = [], ...$channels) {
        return $this->send(array_merge(["pubsub", "numsub"], (array) $channel, $channels), function ($response) {
            $result = [];

            for ($i = 0; $i < sizeof($response); $i += 2) {
                $result[$response[$i]] = $response[$i + 1];
            }

            return $result;
        });
    }

    /**
     * @return Promise
     * @yield int
     */
    public function pubSubNumPat () {
        return $this->send(["pubsub", "numpat"]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function ping () {
        return $this->send(["ping"]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function quit () {
        return $this->send(["quit"]);
    }

    /**
     * @param int $index
     * @return Promise
     * @yield string
     */
    public function select ($index) {
        return $this->send(["select", $index]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function bgRewriteAOF () {
        return $this->send(["bgrewriteaof"]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function bgSave () {
        return $this->send(["bgsave"]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function clientGetName () {
        return $this->send(["client", "getname"]);
    }

    /**
     * @param string ...$args
     * @return Promise
     * @yield string|int
     */
    public function clientKill (...$args) {
        return $this->send(array_merge(["client", "kill"], $args));
    }

    /**
     * @return Promise
     * @yield array
     */
    public function clientList () {
        return $this->send(["client", "list"], function ($response) {
            return explode("\n", $response);
        });
    }

    /**
     * @param int $timeout
     * @return Promise
     * @yield string
     */
    public function clientPause ($timeout) {
        return $this->send(["client", "pause", $timeout]);
    }

    /**
     * @param string $name
     * @return Promise
     * @yield string
     */
    public function clientSetName ($name) {
        return $this->send(["client", "setname", $name]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function clusterSlots () {
        return $this->send(["cluster", "slots"]);
    }

    /**
     * @return Promise
     * @yield array
     */
    public function command () {
        return $this->send(["command"]);
    }

    /**
     * @return Promise
     * @yield array
     */
    public function commandCount () {
        return $this->send(["command", "count"]);
    }

    /**
     * @param string ...$args
     * @return Promise
     * @yield array
     */
    public function commandGetKeys (...$args) {
        return $this->send(array_merge(["command", "getkeys"], $args));
    }

    /**
     * @param string|string[] $command
     * @param string ...$commands
     * @return Promise
     * @yield array
     */
    public function commandInfo ($command, ...$commands) {
        return $this->send(array_merge(["command", "info"], (array) $command, $commands));
    }

    /**
     * @param string $parameter
     * @return Promise
     * @yield array
     */
    public function configGet ($parameter) {
        return $this->send(["config", "get", $parameter]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function configResetStat () {
        return $this->send(["config", "resetstat"]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function configRewrite () {
        return $this->send(["config", "rewrite"]);
    }

    /**
     * @param string $parameter
     * @param string $value
     * @return Promise
     * @yield string
     */
    public function configSet ($parameter, $value) {
        return $this->send(["config", "set", $parameter, $value]);
    }

    /**
     * @return Promise
     * @yield int
     */
    public function dbSize () {
        return $this->send(["dbsize"]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function flushAll () {
        return $this->send(["flushall"]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function flushDB () {
        return $this->send(["flushdb"]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function info () {
        return $this->send(["info"]);
    }

    /**
     * @return Promise
     * @yield int
     */
    public function lastSave () {
        return $this->send(["lastsave"]);
    }

    /**
     * @return Promise
     * @yield array
     */
    public function role () {
        return $this->send(["role"]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function save () {
        return $this->send(["save"]);
    }

    /**
     * @param string $modifier
     * @return Promise
     * @yield string
     */
    public function shutdown ($modifier = null) {
        $payload = ["shutdown"];

        if ($modifier !== null) {
            $payload[] = $modifier;
        }

        return $this->send($payload);
    }

    public function slaveOf ($host, $port = null) {
        if ($host === null) {
            $host = "no";
            $port = "one";
        }

        $this->send(["slaveof", $host, $port]);
    }

    /**
     * @param int $count
     * @return Promise
     * @yield array
     */
    public function slowlogGet ($count = null) {
        $payload = ["slowlog", "get"];

        if ($count !== null) {
            $payload[] = $count;
        }

        return $this->send($payload);
    }

    /**
     * @return Promise
     * @yield int
     */
    public function slowlogLen () {
        return $this->send(["slowlog", "len"]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function slowlogReset () {
        return $this->send(["slowlog", "reset"]);
    }

    /**
     * @return Promise
     * @yield array
     */
    public function time () {
        return $this->send(["time"]);
    }

    /**
     * @param string $sha1
     * @param string|string[] $keys
     * @param string|string[] $args
     * @return Promise
     * @yield mixed
     */
    public function evalSha ($sha1, $keys = [], $args = []) {
        return $this->send(array_merge(["evalsha"], $sha1, sizeof((array) $keys), (array) $keys, (array) $args));
    }

    /**
     * @param string|string[] $script
     * @param string ...$scripts
     * @return Promise
     * @yield array
     */
    public function scriptExists ($script, ...$scripts) {
        return $this->send(array_merge(["script", "exists"], (array) $script, $scripts));
    }

    /**
     * @return Promise
     * @yield string
     */
    public function scriptFlush () {
        return $this->send(["script", "flush"]);
    }

    /**
     * @return Promise
     * @yield string
     */
    public function scriptKill () {
        return $this->send(["script", "kill"]);
    }

    /**
     * @param string $script
     * @return Promise
     * @yield string
     */
    public function scriptLoad ($script) {
        return $this->send(["script", "load", $script]);
    }

    public function __call ($method, $args) {
        // work arround for method names which conflict with php reserved keywords
        if (method_exists($this, "_{$method}")) {
            return call_user_func_array([$this, "_{$method}"], $args);
        }

        throw new BadMethodCallException("Method {$method} doesn't exist");
    }

    /**
     * @param string $text
     * @return Promise
     * @yield string
     */
    private function _echo ($text) {
        return $this->send(["echo", $text]);
    }

    /**
     * @param string $script
     * @param string|string[] $keys
     * @param string|string[] $args
     * @return Promise
     * @yield mixed
     */
    private function _eval ($script, $keys, $args) {
        return $this->send(array_merge(["eval"], $script, sizeof((array) $keys), (array) $keys, (array) $args));
    }
}
