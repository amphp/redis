<?php
/**
 * Created by PhpStorm.
 * User: kelunik
 * Date: 17.12.14
 * Time: 00:24
 */

namespace Amphp\Redis;


class ConnectionConfig {
	private $host;
	private $port;

	public function __construct() {

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
} 