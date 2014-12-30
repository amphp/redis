<?php

/*
 * TODO:
 *
 * keys:
 *  - migrate
 *  - object
 *  - sort
 *  - scan
 *
 * hashes:
 *  - hscan
 */

namespace Amphp\Redis;

use Amp\Reactor;
use Nbsock\Connector;
use function Amp\cancel;
use function Amp\getReactor;
use function Amp\onReadable;
use function Amp\wait;

class Redis {
	const MODE_DEFAULT = 0;
	const MODE_SUBSCRIBE = 1;

	/**
	 * @var Reactor
	 */
	private $reactor;

	/**
	 * @var Connector
	 */
	private $connector;

	/**
	 * @var ConnectionConfig
	 */
	private $config;

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
	 * @var callable
	 */
	private $subscribeCallback;

	public function __construct (ConnectionConfig $config) {
		$this->reactor = getReactor();
		$this->connector = new Connector;
		$this->config = $config;
		$this->mode = self::MODE_DEFAULT;
		$this->outputBufferLength = 0;
		$this->outputBuffer = "";
		$this->parser = new RespParser(function($result) {
			$this->onResponse($result);
		});
	}

	public function connect () {
		if($this->connectFuture || $this->readWatcher) {
			return;
		}

		$this->connectFuture = $this->connector->connect("tcp://" . $this->config->getHost() . ":" . $this->config->getPort());
		$this->connectFuture->when(function ($error, $socket) {
			if ($error) {
				throw $error;
			}

			if (!is_resource($socket) || @feof($socket)) {
				throw new \Exception("Connection could not be initialised!");
			}

			$this->socket = $socket;

			if ($this->config->hasPassword()) {
				$pass = $this->config->getPassword();
				array_unshift($this->futures, new Future);
				$this->outputBuffer = "*2\r\n$4\r\rauth\r\n$" . strlen($pass) . "\r\n" . $pass . "\r\n" . $this->outputBuffer;
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

		if($read) {
			$this->parser->append($read);
		}

		else if (!is_resource($this->socket) || @feof($this->socket)) {
			$this->reactor->cancel($this->readWatcher);
			$this->reactor->cancel($this->writeWatcher);

			$this->readWatcher = null;
			$this->writeWatcher = null;
		}
	}

	private function onResponse($result) {
		if (sizeof($this->futures) > 0) {
			$future = array_shift($this->futures);

			if ($result instanceof RedisException) {
				$future->fail($result);
			} else {
				$future->succeed($result);
			}
		}

		else if (isset($response)) {
			if (is_array($result) && sizeof($result) === 3 && $result[0] === "message") {
				call_user_func($this->subscribeCallback, $response[1], $response[2]);
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

			throw new RedisException("connection gone");
		} else {
			$this->outputBuffer = substr($this->outputBuffer, $bytes);
			$this->outputBufferLength -= $bytes;
		}
	}

	public function close () {
		$this->closeSocket();
	}

	public function getConfig () {
		return $this->config;
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

	private function send (array $strings, callable $responseCallback = null) {
		$this->connect();

		$payload = "";

		foreach ($strings as $string) {
			$payload .= sprintf("$%d\r\n%s\r\n", strlen($string), $string);
		}

		$payload = sprintf("*%d\r\n%s", sizeof($strings), $payload);

		$future = new Future($responseCallback);
		$this->futures[] = $future;

		$this->outputBuffer .= $payload;
		$this->outputBufferLength += strlen($payload);

		if($this->writeWatcher) {
			$this->reactor->enable($this->writeWatcher);
		}

		return $future;
	}

	/**
	 * @param string $keys
	 * @return Future
	 * @yield int
	 */
	public function del (...$keys) {
		return $this->send(array_merge(["del"], $keys));
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
	 * @param string $op
	 * @param string $destination
	 * @param string ...$keys
	 * @return Future
	 * @yield int
	 */
	public function bitop ($op, $destination, ...$keys) {
		return $this->send(array_combine(["bitop", $op, $destination], $keys));
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

		if($start != null) {
			$payload[] = $start;

			if($end != null) {
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
	public function decr($key, $decrement = 1) {
		if($decrement === 1) {
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
	public function get($key) {
		return $this->send(["get", $key]);
	}

	/**
	 * @param string $key
	 * @param int $offset
	 * @return Future
	 * @yield int
	 */
	public function getbit($key, $offset) {
		return $this->send(["getbit", $key, $offset]);
	}

	/**
	 * @param string $key
	 * @param int $start
	 * @param int $end
	 * @return Future
	 * @yield string
	 */
	public function getrange($key, $start = 0, $end = -1) {
		return $this->send(["getrange", $key, $start, $end]);
	}

	/**
	 * @param string $key
	 * @param string $value
	 * @return Future
	 * @yield string
	 */
	public function getset($key, $value) {
		return $this->send(["getset", $key, $value]);
	}

	/**
	 * @param string $key
	 * @param int $increment
	 * @return Future
	 * @yield int
	 */
	public function incr($key, $increment = 1) {
		if($increment === 1) {
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
	public function incrbyfloat($key, $increment) {
		return $this->send(["incrbyfloat", $key, $increment], function ($response) {
			return (float) $response;
		});
	}

	/**
	 * @param string ...$keys
	 * @return Future
	 * @yield array
	 */
	public function mget(...$keys) {
		return $this->send(array_combine(["mget"], $keys));
	}

	/**
	 * @param array $data
	 * @param bool $onlyIfNoneExists
	 * @return Future
	 * @yield bool
	 */
	public function mset(array $data, $onlyIfNoneExists = false) {
		$payload = [$onlyIfNoneExists ? "msetnx" : "mset"];

		foreach($data as $key => $value) {
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
	public function set($key, $value, $expire = 0, $useMillis = false, $existOption = null) {
		$payload = ["set", $key, $value];

		if($expire !== 0) {
			$payload[] = $useMillis ? "PX" : "EX";
			$payload[] = $expire;
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
	public function setnx($key, $value) {
		return $this->set($key, $value, 0, false, "NX");
	}

	/**
	 * @param string $key
	 * @param string $value
	 * @return Future
	 * @yield bool
	 */
	public function setxx($key, $value) {
		return $this->set($key, $value, 0, false, "XX");
	}

	/**
	 * @param string $key
	 * @param int $offset
	 * @param bool $value
	 * @return Future
	 * @yield int
	 */
	public function setbit($key, $offset, $value) {
		return $this->send(["setbit", $key, $offset, (int) $value]);
	}

	/**
	 * @param $key
	 * @param $offset
	 * @param $value
	 * @return Future
	 * @yield int
	 */
	public function setrange($key, $offset, $value) {
		return $this->send(["setrange", $key, $offset, $value]);
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield int
	 */
	public function strlen($key) {
		return $this->send(["strlen", $key]);
	}

	/**
	 * @param string $key
	 * @param string ...$fields
	 * @return Future
	 * @yield int
	 */
	public function hdel($key, ...$fields) {
		return $this->send(array_combine(["hdel", $key], $fields));
	}

	/**
	 * @param string $key
	 * @param string $field
	 * @return Future
	 * @yield bool
	 */
	public function hexists($key, $field) {
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
	public function hget($key, $field) {
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
	public function hincrby($key, $field, $increment = 1) {
		return $this->send(["hincrby", $key, $field, $increment]);
	}

	/**
	 * @param string $key
	 * @param string $field
	 * @param float $increment
	 * @return Future
	 * @yield float
	 */
	public function hincrbyfloat($key, $field, $increment) {
		return $this->send(["hincrbyfloat", $key, $field, $increment], function ($response) {
			return (float) $response;
		});
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield array
	 */
	public function hkeys($key) {
		return $this->send(["hkeys", $key]);
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield int
	 */
	public function hlen($key) {
		return $this->send(["hlen", $key]);
	}

	/**
	 * @param string $key
	 * @param string ...$fields
	 * @return Future
	 * @yield array
	 */
	public function hmget ($key, ...$fields) {
		return $this->send(array_combine(["hmget", $key], $fields), function ($response) {
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
	 * @param string $field
	 * @param string $value
	 * @param bool $notExistingOnly
	 * @return Future
	 * @yield bool
	 */
	public function hset($key, $field, $value, $notExistingOnly = false) {
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
	public function lindex($key, $index) {
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
	public function linsert($key, $relativePosition, $pivot, $value) {
		$relativePosition = strtolower($relativePosition);

		if($relativePosition !== "before" && $relativePosition !== "after") {
			throw new \UnexpectedValueException(
				sprintf("relativePosition should be 'before' or 'after', was '%s'", $relativePosition)
			);
		}

		return $this->send(["linsert", $key, $relativePosition, $pivot, $value]);
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield int
	 */
	public function llen($key) {
		return $this->send(["llen", $key]);
	}

	/**
	 * @param string $keys
	 * @return Future
	 * @yield string
	 */
	public function lpop(...$keys) {
		return $this->send(array_combine(["lpop"], $keys));
	}

	/**
	 * @param string $key
	 * @param string ...$values
	 * @return Future
	 * @yield int
	 */
	public function lpush($key, ...$values) {
		return $this->send(array_combine(["lpush", $key], $values));
	}

	/**
	 * @param string $key
	 * @param string ...$values
	 * @return Future
	 * @yield int
	 */
	public function lpushx($key, ...$values) {
		return $this->send(array_combine(["lpushx", $key], $values));
	}

	/**
	 * @param string $key
	 * @param int $start
	 * @param int $end
	 * @return Future
	 * @yield array
	 */
	public function lrange($key, $start = 0, $end = -1) {
		return $this->send(["lrange", $key, $start, $end]);
	}

	/**
	 * @param string $key
	 * @param string $value
	 * @param int $count
	 * @return Future
	 * @yield int
	 */
	public function lrem($key, $value, $count = 0) {
		return $this->send(["lrem", $key, $count, $value]);
	}

	/**
	 * @param string $key
	 * @param int $index
	 * @param string $value
	 * @return Future
	 * @yield string
	 */
	public function lset($key, $index, $value) {
		return $this->send(["lset", $key, $index, $value]);
	}

	/**
	 * @param string $key
	 * @param int $start
	 * @param int $stop
	 * @return Future
	 * @yield string
	 */
	public function ltrim($key, $start = 0, $stop = -1) {
		return $this->send(["ltrim", $key, $start, $stop]);
	}

	/**
	 * @param string $keys
	 * @return Future
	 * @yield string
	 */
	public function rpop(...$keys) {
		return $this->send(array_combine(["rpop"], $keys));
	}

	/**
	 * @param string $source
	 * @param string $destination
	 * @return Future
	 * @yield string
	 */
	public function rpoplpush($source, $destination) {
		return $this->send(["rpoplpush", $source, $destination]);
	}

	/**
	 * @param string $key
	 * @param string ...$values
	 * @return Future
	 * @yield int
	 */
	public function rpush($key, ...$values) {
		return $this->send(array_combine(["rpush", $key], $values));
	}

	/**
	 * @param string $key
	 * @param string ...$values
	 * @return Future
	 * @yield int
	 */
	public function rpushx($key, ...$values) {
		return $this->send(array_combine(["rpushx", $key], $values));
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function ping () {
		return $this->send(["ping"]);
	}

	/**
	 * @param string $text
	 * @return Future
	 * @yield string
	 */
	public function echotest ($text) {
		return $this->send(["echo", $text]);
	}

	public function subscribe ($channel, $callback) {
		$this->send(["subscribe", $channel]);

		$this->mode = self::MODE_SUBSCRIBE;
		$this->subscribeCallback = $callback;
	}

	public function __destruct () {
		$this->close();
	}
}
