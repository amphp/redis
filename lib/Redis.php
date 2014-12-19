<?php

namespace Amphp\Redis;

use Amp\Future;
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
				$this->readWatcher = $this->reactor->onReadable($this->socket, [$this, "onRead"]);
				$this->writeWatcher = $this->reactor->onWritable($this->socket, [$this, "onWrite"], false);
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

	public function onRead () {
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

	public function onWrite (Reactor $reactor, $watcherId) {
		if ($this->outputBufferLength === 0) {
			$reactor->disable($watcherId);
			$this->watcherEnabled = false;

			return;
		}

		$bytes = @fwrite($this->socket, $this->outputBuffer);
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

	public function parseRESP ($input) {
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

	private function send ($entries) {
		$str = "";

		foreach ($entries as $entry) {
			$str .= sprintf("$%d\r\n%s\r\n", strlen($entry), $entry);
		}

		$cmd = sprintf("*%d\r\n%s", sizeof($entries), $str);

		$this->outputBuffer .= $cmd;
		$this->outputBufferLength += strlen($cmd);
		$this->reactor->enable($this->writeWatcher);
		$this->watcherEnabled = true;
	}

	public function __call ($method, $args) {
		if ($this->mode !== self::MODE_DEFAULT) {
			throw new \Exception("object currently in publish or subscribe mode");
		}

		$this->send(array_merge([$method], $args));

		$this->futures[] = $future = new Future;
		return $future;
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