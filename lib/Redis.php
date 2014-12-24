<?php

/*
 * TODO:
 *
 * keys:
 *  - migrate
 *  - object
 *  - sort
 *  - scan
 */

namespace Amphp\Redis;

use Amp\Reactor;
use Amp\Success;
use Nbsock\Connector;
use function Amp\cancel;
use function Amp\getReactor;
use function Amp\onReadable;
use function Amp\wait;

class Redis {
	const STATE_INIT = 0;
	const STATE_READY = 1;
	const STATE_CLOSED = 2;

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
	private $state;

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
	private $watcherEnabled;
	private $outputBuffer;
	private $outputBufferLength;

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
		$this->state = self::STATE_INIT;
		$this->mode = self::MODE_DEFAULT;
		$this->outputBufferLength = 0;
		$this->outputBuffer = "";

		wait($this->connect());
	}

	public function connect () {
		if ($this->isAlive()) {
			return new Success($this);
		}

		$future = new Future;

		$this->connector->connect("tcp://" . $this->config->getHost() . ":" . $this->config->getPort())->when(function ($error, $socket) use ($future) {
			if ($error) {
				$future->fail($error);
			} else {
				$this->socket = $socket;
				$this->readWatcher = $this->reactor->onReadable($this->socket, function () {
					$this->onRead();
				});
				$this->writeWatcher = $this->reactor->onWritable($this->socket, function (Reactor $reactor, $watcherId) {
					$this->onWrite($reactor, $watcherId);
				}, false);
				$this->state = self::STATE_READY;

				if ($this->config->hasPassword()) {
					$this->send(["auth", $this->config->getPassword()]);
					$this->futures[] = $f = new Future;
					wait($f);
				}

				$future->succeed($this);
			}
		});

		return $future;
	}

	private function onRead () {
		$error = null;

		try {
			$response = $this->readLine();
		} catch (\Exception $e) {
			$error = $e;
		}

		if (sizeof($this->futures) > 0) {
			$future = array_shift($this->futures);

			if (isset($error)) {
				$future->fail($error);
			} else {
				$future->succeed($response);
			}
		} else if (isset($response)) {
			if ($this->mode = self::MODE_SUBSCRIBE) {
				if ($response[0] === "message") {
					call_user_func($this->subscribeCallback, $response[1], $response[2]);
				}
			}
		}
	}

	private function onWrite (Reactor $reactor, $watcherId) {
		if ($this->outputBufferLength === 0) {
			$reactor->disable($watcherId);
			return;
		}

		$bytes = fwrite($this->socket, $this->outputBuffer);
		$this->outputBufferLength -= $bytes;

		if ($bytes === 0) {
			// TODO: Recover
			print "TODO: RECOVER\n";
		} else {
			$this->outputBuffer = substr($this->outputBuffer, $bytes);
		}
	}

	private function readLine () {
		$bytes = fgets($this->socket);

		if ($bytes != "") {
			return $this->parseRESP($bytes);
		} else {
			if (!is_resource($this->socket) || @feof($this->socket)) {
				throw new \Exception("Connection gone");
			}

			return null;
		}
	}

	public function close () {
		$this->closeSocket();
	}

	public function isAlive () {
		return $this->state === self::STATE_READY;
	}

	public function getConfig () {
		return $this->config;
	}

	private function parseRESP ($input) {
		$type = $input[0];
		$pending = substr($input, 1, -2);

		switch ($type) {
			case "+":
				goto parse_simple_string;
			case "-":
				goto parse_error;
			case ":":
				goto parse_integer;
			case "$":
				goto parse_bulk_string;
			case "*":
				goto parse_array;
			default:
				throw new \Exception("unknown RESP type: " . $type);
		}

		parse_simple_string: {
			return $pending;
		}

		parse_bulk_string: {
			if ($pending === "-1") {
				return null;
			}

			$length = intval($pending);

			if ($length > 0) {
				$response = stream_get_contents($this->socket, $length);
			} else {
				$response = "";
			}

			// CRLF
			fread($this->socket, 2);

			return $response;
		}

		parse_error: {
			$message = explode(" ", $pending, 2)[1];
			throw new \Exception($message);
		}

		parse_array: {
			$size = intval($pending);

			if ($size === -1) {
				return null;
			}

			$response = [];

			for ($i = 0; $i < $size; $i++) {
				$response[] = $this->readLine();
			}

			return $response;
		}

		parse_integer: {
			return intval($pending);
		}
	}

	private function closeSocket () {
		$this->state = self::STATE_CLOSED;

		$this->reactor->cancel($this->readWatcher);
		$this->reactor->cancel($this->writeWatcher);

		if (is_resource($this->socket)) {
			fclose($this->socket);
		}
	}

	private function send ($strings, callable $callback = null) {
		$payload = "";

		foreach ($strings as $string) {
			$payload .= sprintf("$%d\r\n%s\r\n", strlen($string), $string);
		}

		$payload = sprintf("*%d\r\n%s", sizeof($strings), $payload);

		$this->outputBuffer .= $payload;
		$this->outputBufferLength += strlen($payload);
		$this->reactor->enable($this->writeWatcher);
		$this->watcherEnabled = true;

		$future = new Future($callback);
		$this->futures[] = $future;
		return $future;
	}

	public function __call ($method, $args) {
		if ($this->mode !== self::MODE_DEFAULT) {
			throw new \Exception("object currently in publish or subscribe mode");
		}

		$this->send(array_merge([$method], $args));

		$this->futures[] = $future = new Future;
		return $future;
	}

	/**
	 * @param string $keys
	 * @return Future
	 * @yield int
	 */
	public function del(...$keys) {
		return $this->send(array_merge(["del"], $keys));
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield string
	 */
	public function dump($key) {
		return $this->send(["dump", $key]);
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield bool
	 */
	public function exists($key) {
		return $this->send(["exists", $key], function($response) {
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
	public function expire($key, $seconds, $inMillis = false) {
		$cmd = $inMillis ? "pexpire" : "expire";
		return $this->send([$cmd, $key, $seconds], function($response) {
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
	public function expireat($key, $timestamp, $inMillis = false) {
		$cmd = $inMillis ? "pexpireat" : "expireat";
		return $this->send([$cmd, $key, $timestamp], function($response) {
			return (bool) $response;
		});
	}

	/**
	 * @param string $pattern
	 * @return Future
	 * @yield array
	 */
	public function keys($pattern) {
		return $this->send(["keys", $pattern]);
	}

	/**
	 * @param string $key
	 * @param int $db
	 * @return Future
	 * @yield bool
	 */
	public function move($key, $db) {
		return $this->send(["move", $key, $db], function($response) {
			return (bool) $response;
		});
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield bool
	 */
	public function persist($key) {
		return $this->send(["persist", $key], function($response) {
			return (bool) $response;
		});
	}

	/**
	 * @return Future
	 * @yield string
	 */
	public function randomkey() {
		return $this->send(["randomkey"]);
	}

	/**
	 * @param string $key
	 * @param string $replacement
	 * @param bool $existingOnly
	 * @return Future
	 * @yield bool
	 */
	public function rename($key, $replacement, $existingOnly = false) {
		$cmd = $existingOnly ? "renamenx" : "rename";
		return $this->send([$cmd, $key, $replacement], function($response) use($existingOnly) {
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
	public function restore($key, $serializedValue, $ttlMillis = 0) {
		return $this->send(["restore", $key, $ttlMillis, $serializedValue]);
	}

	/**
	 * @param string $key
	 * @param bool $millis
	 * @return Future
	 * @yield int
	 */
	public function ttl($key, $millis = false) {
		$cmd = $millis ? "pttl" : "ttl";
		return $this->send([$cmd, $key]);
	}

	/**
	 * @param string $key
	 * @return Future
	 * @yield string
	 */
	public function type($key) {
		return $this->send(["type", $key]);
	}

	public function hmset ($key, array $data) {
		$array = ["hmset", $key];

		foreach ($data as $key => $value) {
			$array[] = $key;
			$array[] = $value;
		}

		return $this->send($array);
	}

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

			return (object) $result;
		});
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
