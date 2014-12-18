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

	public function __construct (ConnectionConfig $config) {
		$this->reactor = getReactor();
		$this->connector = new Connector;
		$this->config = $config;
		$this->state = self::STATE_INIT;
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
				$future->succeed($this);
			}
		});

		return $future;
	}

	public function onRead () {
		$future = array_shift($this->futures);

		try {
			$response = $this->readLine();
			$future->succeed($response);
		} catch (\Exception $e) {
			$this->closeSocket();
			$future->fail($e);
		}
	}

	public function onWrite (Reactor $reactor, $watcherId, $socket) {
		if ($this->outputBufferLength === 0) {
			$reactor->disable($watcherId);
			$this->watcherEnabled = false;
		}

		$bytes = @fwrite($socket, $this->outputBuffer);
		$this->outputBufferLength -= $bytes;

		if ($this->outputBufferLength > 0) {
			if ($bytes === 0) {
				// TODO: Recover
			} else {
				$this->outputBuffer = substr($this->outputBuffer, $bytes);
			}
		}
	}

	private function readLine () {
		$bytes = @fgets($this->socket);

		if ($bytes != "") {
			return $this->parseRESP($bytes);
		} else {
			throw new \Exception("Connection gone");
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

	public function query ($query) {
		$future = new Future;
		$cmd = $query . "\r\n";

		$this->outputBuffer .= $cmd;
		$this->outputBufferLength += strlen($cmd);
		$this->reactor->enable($this->writeWatcher);
		$this->watcherEnabled = true;

		$this->futures[] = $future;
		return $future;
	}

	public function __call ($method, $args) {
		$future = new Future;

		$array = array_merge([$method], $args);

		$str = "";

		foreach ($array as $entry) {
			$str .= sprintf("$%d\r\n%s\r\n", strlen($entry), $entry);
		}

		$cmd = sprintf("*%d\r\n%s", sizeof($array), $str);

		$this->outputBuffer .= $cmd;
		$this->outputBufferLength += strlen($cmd);
		$this->reactor->enable($this->writeWatcher);
		$this->watcherEnabled = true;

		$this->futures[] = $future;
		return $future;
	}

	public function __destruct () {
		$this->close();
	}
}