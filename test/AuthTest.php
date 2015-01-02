<?php

namespace Amphp\Redis;

use function Amp\run;
use function Amp\wait;

class AuthTest extends \PHPUnit_Framework_TestCase {
	static function setUpBeforeClass () {
		print `redis-server --daemonize yes --port 25325 --timeout 3 --pidfile /tmp/amp-redis.pid --requirepass secret`;
	}

	static function tearDownAfterClass () {
		$pid = @file_get_contents("/tmp/amp-redis.pid");
		@unlink("/tmp/amp-redis.pid");

		if(!empty($pid)) {
			print `kill $pid`;
			sleep(1);
		}
	}

	/**
	 * @test
	 */
	function ping () {
		$config = new ConnectionConfig("127.0.0.1", 25325, "secret");
		$response = null;

		$callable = function() use ($config, &$response) {
			$redis = new Redis($config);
			$response = (yield $redis->ping());
			$redis->close();
		};

		run($callable);

		$this->assertEquals("PONG", $response);
	}
}
