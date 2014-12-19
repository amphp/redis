<?php

namespace Amphp\Redis;


class ConnectionConfig {
	private $host;
	private $port;
	private $password;

	public function __construct($host = "127.0.0.1", $port = 6379, $password = null) {
		$this->host = $host;
		$this->port = $port;
		$this->password = $password;
	}

	public function setHost($host) {
		$this->host = $host;
		return $this;
	}

	public function getHost() {
		return $this->host;
	}

	public function setPort ($port) {
		$this->port = $port;
		return $this;
	}

	public function getPort () {
		return $this->port;
	}

	public function setPasswort($password) {
		$this->password = $password;
		return $this;
	}

	public function getPassword() {
		return $this->password;
	}

	public function hasPassword() {
		return !is_null($this->password);
	}
} 