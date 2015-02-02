<?php

namespace Amp\Redis;

use Amp\NativeReactor;
use function Amp\run;

class CloseTest extends \PHPUnit_Framework_TestCase {
	static function setUpBeforeClass () {
		print `redis-server --daemonize yes --port 25325 --timeout 3 --pidfile /tmp/amp-redis.pid`;
		sleep(2);
	}

	static function tearDownAfterClass () {
		$pid = @file_get_contents("/tmp/amp-redis.pid");
		@unlink("/tmp/amp-redis.pid");

		if (!empty($pid)) {
			print `kill $pid`;
			sleep(2);
		}
	}

	/**
	 * @test
	 */
	function reconnect () {
		(new NativeReactor())->run(function ($reactor) {
			$redis = new Redis(["host" => "127.0.0.1:25325"], $reactor);
			$this->assertEquals("PONG", (yield $redis->ping()));
			yield $redis->close();
			$this->assertEquals("PONG", (yield $redis->ping()));
		});
	}
}
