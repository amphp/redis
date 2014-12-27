<?php

namespace Amphp\Redis;

use function Amp\run;
use function Amp\wait;

class BasicTest extends \PHPUnit_Framework_TestCase {
	function setUp () {
		print `redis-server --daemonize yes --port 25325 --timeout 3 --pidfile /tmp/amp-redis.pid`;
	}

	function tearDown () {
		$pid = @file_get_contents("/tmp/amp-redis.pid");
		@unlink("/tmp/amp-redis.pid");

		if(!empty($pid)) {
			print `kill $pid`;
		}
	}

	function testConnect () {
		$config = new ConnectionConfig("127.0.0.1", 25325, null);
		$response = null;

		$callable = function() use ($config, &$response) {
			$redis = new Redis($config);
			$response = (yield $redis->ping());
			$redis->close();
		};

		run($callable);

		$this->assertEquals("PONG", $response);
	}

	function testTimeout () {
		$config = new ConnectionConfig("127.0.0.1", 25325, null);
		$response = null;

		$callable = function() use ($config, &$response) {
			$redis = new Redis($config);
			$response = (yield $redis->echotest("1"));
			yield "pause" => 5000;
			$response = (yield $redis->echotest("2"));
			$redis->close();
		};

		run($callable);

		$this->assertEquals("2", $response);
	}
}
