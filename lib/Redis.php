<?php

namespace Amp\Redis;

use function Amp\getReactor;
use Amp\Reactor;
use Nbsock\Connector;

class Redis {
	const MODE_DEFAULT = 0;
	const MODE_PUBSUB = 1;

	/**
	 * @var Reactor
	 */
	private $reactor;

	/**
	 * @var Connector
	 */
	private $connector;

	/**
	 * @var array
	 */
	private $options;

	/**
	 * @var int
	 */
	private $mode;

	/**
	 * @var resource
	 */
	private $socket;

	private $readWatcher;
	private $writeWatcher;
	private $outputBuffer;
	private $outputBufferLength;

	/**
	 * @ver RespParser
	 */
	private $parser;

	/**
	 * @var Future
	 */
	private $connectFuture;

	/**
	 * @var Future[]
	 */
	private $futures = [];

	/**
	 * @var callable[]
	 */
	private $callbacks;

	/**
	 * @var callable[]
	 */
	private $patternCallbacks;

	/**
	 * @param Reactor $reactor
	 * @param array $options
	 */
	public function __construct (array $options = [], Reactor $reactor = null) {
		$this->options = array_merge([
			"host" => "127.0.0.1",
			"password" => null
		], $options);
		$this->reactor = $reactor ?: getReactor();

		$this->mode = self::MODE_DEFAULT;
		$this->outputBufferLength = 0;
		$this->outputBuffer = "";

		$this->connector = new Connector($reactor);
		$this->parser = new RespParser(function ($result) {
			$this->onResponse($result);
		});
	}

	public function connect () {
		if ($this->connectFuture || $this->readWatcher) {
			return;
		}

		$this->connectFuture = $this->connector->connect("tcp://" . $this->options["host"]);
		$this->connectFuture->when(function ($error, $socket) {
			if ($error) {
				throw $error;
			}

			$this->socket = $socket;
			$this->onRead();

			if ($this->options["password"] !== null) {
				array_unshift($this->futures, new Future);
				$this->outputBuffer = "*2\r\n$4\r\rauth\r\n$" . strlen($this->options["password"]) . "\r\n" . $this->options["password"] . "\r\n" . $this->outputBuffer;
				$this->outputBufferLength = strlen($this->outputBuffer);
			}

			$this->readWatcher = $this->reactor->onReadable($this->socket, function () {
				$this->onRead();
			});

			$this->writeWatcher = $this->reactor->onWritable($this->socket, function (Reactor $reactor, $watcherId) {
				$this->onWrite($reactor, $watcherId);
			}, !empty($this->outputBuffer));

			$this->connectFuture = null;
		});
	}

	private function onRead () {
		$read = fread($this->socket, 8192);

		if ($read !== false && $read !== "") {
			$this->parser->append($read);
		} else if (!is_resource($this->socket) || @feof($this->socket)) {
			if ($this->readWatcher || $this->writeWatcher) {
				$this->reactor->cancel($this->readWatcher);
				$this->reactor->cancel($this->writeWatcher);

				$this->readWatcher = null;
				$this->writeWatcher = null;
			} else {
				throw new ConnectException("connection could not be initialized");
			}
		}
	}

	private function onResponse ($result) {
		if ($this->mode === self::MODE_DEFAULT) {
			$future = array_shift($this->futures);

			if ($result instanceof RedisException) {
				$future->fail($result);
			} else {
				$future->succeed($result);
			}
		} else {
			switch ($result[0]) {
				case "message":
					call_user_func($this->callbacks[$result[1]], $result[2]);
					break;
				case "unsubscribe":
					if ($result[2] === 0) {
						$this->mode = self::MODE_DEFAULT;
					}

					unset($this->callbacks[$result[1]]);
					break;
				case "punsubscribe":
					if ($result[2] === 0) {
						$this->mode = self::MODE_DEFAULT;
					}

					unset($this->patternCallbacks[$result[1]]);
					break;
			}
		}
	}

	private function onWrite (Reactor $reactor, $watcherId) {
		if ($this->outputBufferLength === 0) {
			$reactor->disable($watcherId);
			return;
		}

		$bytes = fwrite($this->socket, $this->outputBuffer);

		if ($bytes === 0) {
			$this->reactor->cancel($this->readWatcher);
			$this->reactor->cancel($this->writeWatcher);

			$this->readWatcher = null;
			$this->writeWatcher = null;

			throw new ConnectException("connection gone");
		} else {
			$this->outputBuffer = (string) substr($this->outputBuffer, $bytes);
			$this->outputBufferLength -= $bytes;
		}
	}

	public function close () {
		$this->closeSocket();
	}

	private function closeSocket () {
		$this->reactor->cancel($this->readWatcher);
		$this->reactor->cancel($this->writeWatcher);

		$this->readWatcher = null;
		$this->writeWatcher = null;

		if (is_resource($this->socket)) {
			fclose($this->socket);
		}
	}

	private function send (array $strings, callable $responseCallback = null, $addFuture = true) {
		$this->connect();

		$payload = "";

		foreach ($strings as $string) {
			$payload .= sprintf("$%d\r\n%s\r\n", strlen($string), $string);
		}

		$payload = sprintf("*%d\r\n%s", sizeof($strings), $payload);

		if ($addFuture) {
			$future = new Future($responseCallback);
			$this->futures[] = $future;
		} else {
			$future = null;
		}

		$this->outputBuffer .= $payload;
		$this->outputBufferLength += strlen($payload);

		if ($this->writeWatcher !== null) {
			$this->reactor->enable($this->writeWatcher);
		}

		return $future;
	}

	/**
	 * @param string $key
	 * @param string $keys
	 * @return Future
	 * @yield int
	 */
	public function del ($key, ...$keys) {
		return $this->send(array_merge(["del", $key], $keys));
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield string
	 */
	public function dump ($key) {
		return $this->send(["dump", $key]);
	}

	/**
	 * @param string $key
	 * @return Future
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
	 * @return Future
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
	 * @return Future
	 * @yield bool
	 */
	public function expireat ($key, $timestamp, $inMillis = false) {
		$cmd = $inMillis ? "pexpireat" : "expireat";
		return $this->send([$cmd, $key, $timestamp], function ($response) {
			return (bool) $response;
		});
	}

	/**
	 * @param string $pattern
	 * @return Future
	 * @yield array
	 */
	public function keys ($pattern) {
		return $this->send(["keys", $pattern]);
	}

	/**
	 * @param string $key
	 * @param int $db
	 * @return Future
	 * @yield bool
	 */
	public function move ($key, $db) {
		return $this->send(["move", $key, $db], function ($response) {
			return (bool) $response;
		});
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield int
	 */
	public function object_refcount ($key) {
		return $this->send(["object", "refcount", $key]);
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield string
	 */
	public function object_encoding ($key) {
		return $this->send(["object", "encoding", $key]);
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield int
	 */
	public function object_idletime ($key) {
		return $this->send(["object", "idletime", $key]);
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield bool
	 */
	public function persist ($key) {
		return $this->send(["persist", $key], function ($response) {
			return (bool) $response;
		});
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function randomkey () {
		return $this->send(["randomkey"]);
	}

	/**
	 * @param string $key
	 * @param string $replacement
	 * @param bool $existingOnly
	 * @return Future
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
	 * @return Future
	 * @yield string
	 */
	public function restore ($key, $serializedValue, $ttlMillis = 0) {
		return $this->send(["restore", $key, $ttlMillis, $serializedValue]);
	}

	/**
	 * @param string $cursor
	 * @param string $pattern
	 * @param int $count
	 * @return Future
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
	 * @param array|string $get
	 * @param int $offset
	 * @param int $count
	 * @param bool $alpha
	 * @param string $store
	 * @return Future
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
	 * @return Future
	 * @yield int
	 */
	public function ttl ($key, $millis = false) {
		$cmd = $millis ? "pttl" : "ttl";
		return $this->send([$cmd, $key]);
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield string
	 */
	public function type ($key) {
		return $this->send(["type", $key]);
	}

	/**
	 * @param string $key
	 * @param string $value
	 * @return Future
	 * @yield int
	 */
	public function append ($key, $value) {
		return $this->send(["append", $key, $value]);
	}

	/**
	 * @param string $key
	 * @param int|null $start
	 * @param int|null $end
	 * @return Future
	 */
	public function bitcount ($key, $start = null, $end = null) {
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
	 * @param string $key
	 * @param string ...$keys
	 * @return Future
	 * @yield int
	 */
	public function bitop ($operation, $destination, $key, ...$keys) {
		return $this->send(array_merge(["bitop", $operation, $destination, $key], $keys));
	}

	/**
	 * @param string $key
	 * @param int $bit
	 * @param int $start
	 * @param int $end
	 * @return Future
	 * @yield int
	 */
	public function bitpos ($key, $bit, $start = null, $end = null) {
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
	 * @return Future
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
	 * @return Future
	 * @yield string
	 */
	public function get ($key) {
		return $this->send(["get", $key]);
	}

	/**
	 * @param string $key
	 * @param int $offset
	 * @return Future
	 * @yield int
	 */
	public function getbit ($key, $offset) {
		return $this->send(["getbit", $key, $offset]);
	}

	/**
	 * @param string $key
	 * @param int $start
	 * @param int $end
	 * @return Future
	 * @yield string
	 */
	public function getrange ($key, $start = 0, $end = -1) {
		return $this->send(["getrange", $key, $start, $end]);
	}

	/**
	 * @param string $key
	 * @param string $value
	 * @return Future
	 * @yield string
	 */
	public function getset ($key, $value) {
		return $this->send(["getset", $key, $value]);
	}

	/**
	 * @param string $key
	 * @param int $increment
	 * @return Future
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
	 * @return Future
	 * @yield float
	 */
	public function incrbyfloat ($key, $increment) {
		return $this->send(["incrbyfloat", $key, $increment], function ($response) {
			return (float) $response;
		});
	}

	/**
	 * @param string $key
	 * @param string ...$keys
	 * @return Future
	 * @yield array
	 */
	public function mget ($key, ...$keys) {
		return $this->send(array_merge(["mget", $key], $keys));
	}

	/**
	 * @param array $data
	 * @param bool $onlyIfNoneExists
	 * @return Future
	 * @yield bool
	 */
	public function mset (array $data, $onlyIfNoneExists = false) {
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
	 * @param int $expire
	 * @param bool $useMillis
	 * @param string $existOption
	 * @return Future
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
	 * @return Future
	 * @yield bool
	 */
	public function setnx ($key, $value) {
		return $this->set($key, $value, 0, false, "NX");
	}

	/**
	 * @param string $key
	 * @param string $value
	 * @return Future
	 * @yield bool
	 */
	public function setxx ($key, $value) {
		return $this->set($key, $value, 0, false, "XX");
	}

	/**
	 * @param string $key
	 * @param int $offset
	 * @param bool $value
	 * @return Future
	 * @yield int
	 */
	public function setbit ($key, $offset, $value) {
		return $this->send(["setbit", $key, $offset, (int) $value]);
	}

	/**
	 * @param $key
	 * @param $offset
	 * @param $value
	 * @return Future
	 * @yield int
	 */
	public function setrange ($key, $offset, $value) {
		return $this->send(["setrange", $key, $offset, $value]);
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield int
	 */
	public function strlen ($key) {
		return $this->send(["strlen", $key]);
	}

	/**
	 * @param string $key
	 * @param string $field
	 * @param string ...$fields
	 * @return Future
	 * @yield int
	 */
	public function hdel ($key, $field, ...$fields) {
		return $this->send(array_merge(["hdel", $key, $field], $fields));
	}

	/**
	 * @param string $key
	 * @param string $field
	 * @return Future
	 * @yield bool
	 */
	public function hexists ($key, $field) {
		return $this->send(["hexists", $key, $field], function ($response) {
			return (bool) $response;
		});
	}

	/**
	 * @param string $key
	 * @param string $field
	 * @return Future
	 * @yield string
	 */
	public function hget ($key, $field) {
		return $this->send(["hget", $key, $field]);
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield array
	 */
	public function hgetall ($key) {
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
	 * @return Future
	 * @yield int
	 */
	public function hincrby ($key, $field, $increment = 1) {
		return $this->send(["hincrby", $key, $field, $increment]);
	}

	/**
	 * @param string $key
	 * @param string $field
	 * @param float $increment
	 * @return Future
	 * @yield float
	 */
	public function hincrbyfloat ($key, $field, $increment) {
		return $this->send(["hincrbyfloat", $key, $field, $increment], function ($response) {
			return (float) $response;
		});
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield array
	 */
	public function hkeys ($key) {
		return $this->send(["hkeys", $key]);
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield int
	 */
	public function hlen ($key) {
		return $this->send(["hlen", $key]);
	}

	/**
	 * @param string $key
	 * @param string $field
	 * @param string ...$fields
	 * @return Future
	 * @yield array
	 */
	public function hmget ($key, $field, ...$fields) {
		return $this->send(array_merge(["hmget", $key, $field], $fields), function ($response) {
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
	 * @return Future
	 * @yield string
	 */
	public function hmset ($key, array $data) {
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
	 * @return Future
	 * @yield array
	 */
	public function hscan ($key, $cursor, $pattern = null, $count = null) {
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
	 * @return Future
	 * @yield bool
	 */
	public function hset ($key, $field, $value, $notExistingOnly = false) {
		$cmd = $notExistingOnly ? "hsetnx" : "hset";
		return $this->send([$cmd, $key, $field, $value], function ($response) {
			return (bool) $response;
		});
	}

	/**
	 * @param string $key
	 * @param string $index
	 * @return Future
	 * @yield string
	 */
	public function lindex ($key, $index) {
		return $this->send(["lindex", $key, $index]);
	}

	/**
	 * @param string $key
	 * @param string $relativePosition
	 * @param string $pivot
	 * @param string $value
	 * @return Future
	 * @yield int
	 */
	public function linsert ($key, $relativePosition, $pivot, $value) {
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
	 * @return Future
	 * @yield int
	 */
	public function llen ($key) {
		return $this->send(["llen", $key]);
	}

	/**
	 * @param string $key
	 * @param string $keys
	 * @return Future
	 * @yield string
	 */
	public function lpop ($key, ...$keys) {
		return $this->send(array_merge(["lpop", $key], $keys));
	}

	/**
	 * @param string $key
	 * @param string $value
	 * @param string ...$values
	 * @return Future
	 * @yield int
	 */
	public function lpush ($key, $value, ...$values) {
		return $this->send(array_merge(["lpush", $key, $value], $values));
	}

	/**
	 * @param string $key
	 * @param string $value
	 * @param string ...$values
	 * @return Future
	 * @yield int
	 */
	public function lpushx ($key, $value, ...$values) {
		return $this->send(array_merge(["lpushx", $key, $value], $values));
	}

	/**
	 * @param string $key
	 * @param int $start
	 * @param int $end
	 * @return Future
	 * @yield array
	 */
	public function lrange ($key, $start = 0, $end = -1) {
		return $this->send(["lrange", $key, $start, $end]);
	}

	/**
	 * @param string $key
	 * @param string $value
	 * @param int $count
	 * @return Future
	 * @yield int
	 */
	public function lrem ($key, $value, $count = 0) {
		return $this->send(["lrem", $key, $count, $value]);
	}

	/**
	 * @param string $key
	 * @param int $index
	 * @param string $value
	 * @return Future
	 * @yield string
	 */
	public function lset ($key, $index, $value) {
		return $this->send(["lset", $key, $index, $value]);
	}

	/**
	 * @param string $key
	 * @param int $start
	 * @param int $stop
	 * @return Future
	 * @yield string
	 */
	public function ltrim ($key, $start = 0, $stop = -1) {
		return $this->send(["ltrim", $key, $start, $stop]);
	}

	/**
	 * @param string $key
	 * @param string $keys
	 * @return Future
	 * @yield string
	 */
	public function rpop ($key, ...$keys) {
		return $this->send(array_merge(["rpop", $key], $keys));
	}

	/**
	 * @param string $source
	 * @param string $destination
	 * @return Future
	 * @yield string
	 */
	public function rpoplpush ($source, $destination) {
		return $this->send(["rpoplpush", $source, $destination]);
	}

	/**
	 * @param string $key
	 * @param string $value
	 * @param string ...$values
	 * @return Future
	 * @yield int
	 */
	public function rpush ($key, $value, ...$values) {
		return $this->send(array_merge(["rpush", $key, $value], $values));
	}

	/**
	 * @param string $key
	 * @param string $value
	 * @param string ...$values
	 * @return Future
	 * @yield int
	 */
	public function rpushx ($key, $value, ...$values) {
		return $this->send(array_merge(["rpushx", $key, $value], $values));
	}

	/**
	 * @param string $key
	 * @param string $member
	 * @param string ...$members
	 * @return Future
	 * @yield int
	 */
	public function sadd ($key, $member, ...$members) {
		return $this->send(array_merge(["sadd", $key, $member], $members));
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield int
	 */
	public function scard ($key) {
		return $this->send(["scard", $key]);
	}

	/**
	 * @param string $key
	 * @param string ...$keys
	 * @return Future
	 * @yield array
	 */
	public function sdiff ($key, ...$keys) {
		return $this->send(array_merge(["sdiff", $key], $keys));
	}

	/**
	 * @param string $destination
	 * @param string $key
	 * @param string ...$keys
	 * @return Future
	 * @yield int
	 */
	public function sdiffstore ($destination, $key, ...$keys) {
		return $this->send(array_merge(["sdiffstore", $destination, $key], $keys));
	}

	/**
	 * @param string $key
	 * @param string ...$keys
	 * @return Future
	 * @yield array
	 */
	public function sinter ($key, ...$keys) {
		return $this->send(array_merge(["sinter", $key], $keys));
	}

	/**
	 * @param string $destination
	 * @param string $key
	 * @param string ...$keys
	 * @return Future
	 * @yield int
	 */
	public function sinterstore ($destination, $key, ...$keys) {
		return $this->send(array_merge(["sinterstore", $destination, $key], $keys));
	}

	/**
	 * @param string $key
	 * @param string $member
	 * @return Future
	 * @yield bool
	 */
	public function sismember ($key, $member) {
		return $this->send(["sismember", $key, $member], function ($response) {
			return (bool) $response;
		});
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield array
	 */
	public function smembers ($key) {
		return $this->send(["smembers", $key]);
	}

	/**
	 * @param string $source
	 * @param string $destination
	 * @param string $member
	 * @return Future
	 * @yield bool
	 */
	public function smove ($source, $destination, $member) {
		return $this->send(["sismember", $source, $destination, $member], function ($response) {
			return (bool) $response;
		});
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield string
	 */
	public function spop ($key) {
		return $this->send(["spop", $key]);
	}

	/**
	 * @param string $key
	 * @param int $count
	 * @param bool $distinctOnly
	 * @return Future
	 * @yield string|array
	 */
	public function srandmember ($key, $count = null, $distinctOnly = true) {
		$payload = ["srandmember", $key];

		if ($count !== null) {
			$payload[] = $distinctOnly ? $count : -$count;
		}

		return $this->send($payload);
	}

	/**
	 * @param string $key
	 * @param string $member
	 * @param string ...$members
	 * @return Future
	 * @yield int
	 */
	public function srem ($key, $member, ...$members) {
		return $this->send(array_merge(["srem", $key, $member], $members));
	}

	/**
	 * @param string $key
	 * @param string $cursor
	 * @param string $pattern
	 * @param int $count
	 * @return Future
	 * @yield array
	 */
	public function sscan ($key, $cursor, $pattern = null, $count = null) {
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
	 * @param string $key
	 * @param string ...$keys
	 * @return Future
	 * @yield array
	 */
	public function sunion ($key, ...$keys) {
		return $this->send(array_merge(["sunion", $key], $keys));
	}

	/**
	 * @param string $destination
	 * @param string $key
	 * @param string ...$keys
	 * @return Future
	 * @yield int
	 */
	public function sunionstore ($destination, $key, ...$keys) {
		return $this->send(array_merge(["sunionstore", $destination, $key], $keys));
	}

	/**
	 * @param string $key
	 * @param array $data
	 * @return Future
	 * @yield int
	 */
	public function zadd ($key, array $data) {
		$payload = ["zadd", $key];

		foreach ($data as $member => $score) {
			$payload[] = $score;
			$payload[] = $member;
		}

		return $this->send($payload);
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield int
	 */
	public function zcard ($key) {
		return $this->send(["zcard", $key]);
	}

	/**
	 * @param string $key
	 * @param int $min
	 * @param int $max
	 * @return Future
	 * @yield int
	 */
	public function zcount ($key, $min, $max) {
		return $this->send(["zcount", $key, $min, $max]);
	}

	/**
	 * @param string $key
	 * @param string $member
	 * @param int|float $increment
	 * @return Future
	 * @yield float
	 */
	public function zincrby ($key, $member, $increment = 1) {
		return $this->send(["zincrby", $key, $increment, $member], function ($response) {
			return (float) $response;
		});
	}

	/**
	 * @param string $destination
	 * @param int $numkeys
	 * @param string|array $keys
	 * @param string $aggregate
	 * @return Future
	 * @yield int
	 */
	public function zinterstore ($destination, $numkeys, $keys, $aggregate = "sum") {
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
	 * @return Future
	 * @yield int
	 */
	public function zlexcount ($key, $min, $max) {
		return $this->send(["zlexcount", $key, $min, $max]);
	}

	/**
	 * @param string $key
	 * @param int $start
	 * @param int $stop
	 * @param bool $withScores
	 * @return Future
	 * @yield array
	 */
	public function zrange ($key, $start = 0, $stop = -1, $withScores = false) {
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
	 * @return Future
	 * @yield array
	 */
	public function zrangebylex ($key, $min, $max, $offset = null, $count = null) {
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
	 * @return Future
	 * @yield array
	 */
	public function zrangebyscore ($key, $min = 0, $max = -1, $withScores = false, $offset = null, $count = null) {
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
	 * @return Future
	 * @yield int|null
	 */
	public function zrank ($key, $member) {
		return $this->send(["zrank", $key, $member]);
	}

	/**
	 * @param string $key
	 * @param string $member
	 * @param string ...$members
	 * @return Future
	 * @yield int
	 */
	public function zrem ($key, $member, ...$members) {
		return $this->send(array_merge(["zrem"], $key, $member, $members));
	}

	/**
	 * @param string $key
	 * @param string $min
	 * @param string $max
	 * @return Future
	 * @yield int
	 */
	public function zremrangebylex ($key, $min, $max) {
		return $this->send(["zremrangebylex", $key, $min, $max]);
	}

	/**
	 * @param string $key
	 * @param int $start
	 * @param int $stop
	 * @return Future
	 * @yield int
	 */
	public function zremrangebyrank ($key, $start, $stop) {
		return $this->send(["zremrangebyrank", $key, $start, $stop]);
	}

	/**
	 * @param string $key
	 * @param int $min
	 * @param int $max
	 * @return Future
	 * @yield int
	 */
	public function zremrangebyscore ($key, $min, $max) {
		return $this->send(["zremrangebyscore", $key, $min, $max]);
	}

	/**
	 * @param string $key
	 * @param int $min
	 * @param int $max
	 * @param bool $withScores
	 * @return Future
	 * @yield array
	 */
	public function zrevrange ($key, $min = 0, $max = -1, $withScores = false) {
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
	 * @return Future
	 * @yield array
	 */
	public function zrevrangebylex ($key, $min, $max, $offset = null, $count = null) {
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
	 * @return Future
	 * @yield array
	 */
	public function zrevrangebyscore ($key, $min = 0, $max = -1, $withScores = false, $offset = null, $count = null) {
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
	 * @return Future
	 * @yield int|null
	 */
	public function zrevrank ($key, $member) {
		return $this->send(["zrevrank", $key, $member]);
	}

	/**
	 * @param string $key
	 * @param string $cursor
	 * @param string $pattern
	 * @param int $count
	 * @return Future
	 * @yield array
	 */
	public function zscan ($key, $cursor, $pattern = null, $count = null) {
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
	 * @return Future
	 * @yield int|null
	 */
	public function zscore ($key, $member) {
		return $this->send(["zscore", $key, $member]);
	}

	/**
	 * @param string $destination
	 * @param int $numkeys
	 * @param string|array $keys
	 * @param string $aggregate
	 * @return Future
	 * @yield int
	 */
	public function zunionstore ($destination, $numkeys, $keys, $aggregate = "sum") {
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
	 * @param string $element
	 * @param string ...$elements
	 * @return Future
	 * @yield bool
	 */
	public function pfadd ($key, $element, ...$elements) {
		return $this->send(array_merge(["pfadd", $key, $element], $elements), function ($response) {
			return (bool) $response;
		});
	}

	/**
	 * @param string $key
	 * @param string ...$keys
	 * @return Future
	 * @yield int
	 */
	public function pfcount ($key, ...$keys) {
		return $this->send(array_merge(["pfcount", $key], $keys));
	}

	/**
	 * @param string $destinationKey
	 * @param string $sourceKey
	 * @param string ...$sourceKeys
	 * @return Future
	 * @yield string
	 */
	public function pfmerge ($destinationKey, $sourceKey, ...$sourceKeys) {
		return $this->send(array_merge(["pfmerge", $destinationKey, $sourceKey], $sourceKeys));
	}

	/**
	 * @param string|array $channel
	 * @param callable $callback
	 * @return void
	 */
	public function subscribe ($channel, callable $callback) {
		$this->mode = self::MODE_PUBSUB;

		$channel = (array) $channel;
		foreach ($channel as $c) {
			$this->callbacks[$c] = $callback;
		}

		$this->send(array_merge(["subscribe"], $channel), null, false);
	}

	/**
	 * @param string|array $pattern
	 * @param callable $callback
	 * @return void
	 */
	public function psubscribe ($pattern, callable $callback) {
		$this->mode = self::MODE_PUBSUB;

		$pattern = (array) $pattern;
		foreach ($pattern as $p) {
			$this->patternCallbacks[$p] = $callback;
		}

		$this->send(array_merge(["psubscribe"], $pattern), null, false);
	}

	/**
	 * @param string $channel
	 * @return void
	 */
	public function unsubscribe ($channel) {
		$this->send(array_merge(["unsubscribe"], (array) $channel), null, false);
	}

	/**
	 * @param string $pattern
	 * @return void
	 */
	public function punsubscribe ($pattern) {
		$this->send(array_merge(["punsubscribe"], (array) $pattern), null, false);
	}

	/**
	 * @param $channel
	 * @param $message
	 * @return Future
	 * @yield int
	 */
	public function publish ($channel, $message) {
		return $this->send(["publish", $channel, $message]);
	}

	/**
	 * @param string $pattern
	 * @return Future
	 * @yield array
	 */
	public function pubsub_channels ($pattern = null) {
		$payload = ["pubsub", "channels"];

		if ($pattern !== null) {
			$payload[] = $pattern;
		}

		return $this->send($payload);
	}

	/**
	 * @param string $channel
	 * @return Future
	 * @yield array
	 */
	public function pubsub_numsub (...$channel) {
		return $this->send(array_merge(["pubsub", "numsub"], $channel), function ($response) {
			$result = [];

			for ($i = 0; $i < sizeof($response); $i += 2) {
				$result[$response[$i]] = $response[$i + 1];
			}

			return $result;
		});
	}

	/**
	 * @return Future
	 * @yield int
	 */
	public function pubsub_numpat () {
		return $this->send(["pubsub", "numpat"]);
	}

	/**
	 * @param string $text
	 * @return Future
	 * @yield string
	 */
	public function echotest ($text) {
		return $this->send(["echo", $text]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function ping () {
		return $this->send(["ping"]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function quit () {
		return $this->send(["quit"]);
	}

	/**
	 * @param int $index
	 * @return Future
	 * @yield string
	 */
	public function select ($index) {
		return $this->send(["select", $index]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function bgrewriteaof () {
		return $this->send(["bgrewriteaof"]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function bgsave () {
		return $this->send(["bgsave"]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function client_getname () {
		return $this->send(["client", "getname"]);
	}

	/**
	 * @param string $args
	 * @return Future
	 * @yield string|int
	 */
	public function client_kill (...$args) {
		return $this->send(array_merge(["client", "kill"], $args));
	}

	/**
	 * @return Future
	 * @yield array
	 */
	public function client_list () {
		return $this->send(["client", "list"], function ($response) {
			return explode("\n", $response);
		});
	}

	/**
	 * @param int $timeout
	 * @return Future
	 * @yield string
	 */
	public function client_pause ($timeout) {
		return $this->send(["client", "pause", $timeout]);
	}

	/**
	 * @param string $name
	 * @return Future
	 * @yield string
	 */
	public function client_setname ($name) {
		return $this->send(["client", "setname", $name]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function cluster_slots () {
		return $this->send(["cluster", "slots"]);
	}

	/**
	 * @return Future
	 * @yield array
	 */
	public function command () {
		return $this->send(["command"]);
	}

	/**
	 * @return Future
	 * @yield array
	 */
	public function command_count () {
		return $this->send(["command", "count"]);
	}

	/**
	 * @param string $args
	 * @return Future
	 * @yield array
	 */
	public function command_getkeys (...$args) {
		return $this->send(array_merge(["command", "getkeys"], $args));
	}

	/**
	 * @param string $command
	 * @param string $commands
	 * @return Future
	 * @yield array
	 */
	public function command_info ($command, ...$commands) {
		return $this->send(array_merge(["command", "info", $command], $commands));
	}

	/**
	 * @param string $parameter
	 * @return Future
	 * @yield array
	 */
	public function config_get ($parameter) {
		return $this->send(["config", "get", $parameter]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function config_resetstat () {
		return $this->send(["config", "resetstat"]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function config_rewrite () {
		return $this->send(["config", "rewrite"]);
	}

	/**
	 * @param string $parameter
	 * @param string $value
	 * @return Future
	 * @yield string
	 */
	public function config_set ($parameter, $value) {
		return $this->send(["config", "set", $parameter, $value]);
	}

	/**
	 * @return Future
	 * @yield int
	 */
	public function dbsize () {
		return $this->send(["dbsize"]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function flushall () {
		return $this->send(["flushall"]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function flushdb () {
		return $this->send(["flushdb"]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function info () {
		return $this->send(["info"]);
	}

	/**
	 * @return Future
	 * @yield int
	 */
	public function lastsave () {
		return $this->send(["lastsave"]);
	}

	/**
	 * @return Future
	 * @yield array
	 */
	public function role () {
		return $this->send(["role"]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function save () {
		return $this->send(["save"]);
	}

	/**
	 * @param string $modifier
	 * @return Future
	 * @yield string
	 */
	public function shutdown ($modifier = null) {
		$payload = ["shutdown"];

		if ($modifier !== null) {
			$payload[] = $modifier;
		}

		return $this->send($payload);
	}

	public function slaveof ($host, $port = null) {
		if ($host === null) {
			$host = "no";
			$port = "one";
		}

		$this->send(["slaveof", $host, $port]);
	}

	/**
	 * @param int $count
	 * @return Future
	 * @yield array
	 */
	public function slowlog_get ($count = null) {
		$payload = ["slowlog", "get"];

		if ($count !== null) {
			$payload[] = $count;
		}

		return $this->send($payload);
	}

	/**
	 * @return Future
	 * @yield int
	 */
	public function slowlog_len () {
		return $this->send(["slowlog", "len"]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function slowlog_reset () {
		return $this->send(["slowlog", "reset"]);
	}

	/**
	 * @return Future
	 * @yield array
	 */
	public function time () {
		return $this->send(["time"]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function discard () {
		return $this->send(["discard"]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function exec () {
		return $this->send(["exec"]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function multi () {
		return $this->send(["multi"]);
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function unwatch () {
		return $this->send(["unwatch"]);
	}

	/**
	 * @param string $key
	 * @param string $keys
	 * @return Future
	 * @yield string
	 */
	public function watch ($key, ...$keys) {
		return $this->send(array_merge(["watch", $key], $keys));
	}

	public function __destruct () {
		$this->close();
	}
}
