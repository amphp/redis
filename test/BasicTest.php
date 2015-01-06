<?php

namespace Amp\Redis;

use Amp\NativeReactor;

class BasicTest extends \PHPUnit_Framework_TestCase {
	static function setUpBeforeClass () {
		print `redis-server --daemonize yes --port 25325 --timeout 3 --pidfile /tmp/amp-redis.pid`;
		sleep(1);
	}

	static function tearDownAfterClass () {
		$pid = @file_get_contents("/tmp/amp-redis.pid");
		@unlink("/tmp/amp-redis.pid");

		if (!empty($pid)) {
			print `kill $pid`;
			sleep(1);
		}
	}

	/**
	 * @test
	 */
	function connect () {
		(new NativeReactor)->run(function ($reactor) {
			$redis = new Redis($reactor, ["host" => "127.0.0.1:25325"]);
			$this->assertEquals("PONG", (yield $redis->ping()));
			$redis->close();
		});
	}

	/**
	 * @test
	 */
	function multiCommand () {
		(new NativeReactor)->run(function ($reactor) {
			$redis = new Redis($reactor, ["host" => "127.0.0.1:25325"]);
			$redis->echotest("1");
			$this->assertEquals("2", (yield $redis->echotest("2")));
			$redis->close();
		});
	}

	/**
	 * @test
	 * @medium
	 */
	function timeout () {
		(new NativeReactor)->run(function ($reactor) {
			$redis = new Redis($reactor, ["host" => "127.0.0.1:25325"]);
			$redis->echotest("1");

			yield "pause" => 8000;

			$this->assertEquals("2", (yield $redis->echotest("2")));
			$redis->close();
		});
	}
}
