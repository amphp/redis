<?php

namespace Amphp\Redis;

use function Amp\run;
use function Amp\wait;

class KeyTest extends \PHPUnit_Framework_TestCase {
	static function setUpBeforeClass () {
		print `redis-server --daemonize yes --port 25325 --timeout 3 --pidfile /tmp/amp-redis.pid`;
		sleep(1);
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
	function keys () {
		$config = new ConnectionConfig("127.0.0.1", 25325, null);
		$response = [null, null];

		$callable = function() use ($config, &$response) {
			$redis = new Redis($config);
			$response[0] = (yield $redis->keys("*"));
			$redis->set("foo", "bar");
			$response[1] = (yield $redis->keys("*"));
			$redis->close();
		};

		run($callable);

		$this->assertEquals([], $response[0]);
		$this->assertEquals(["foo"], $response[1]);
	}

	/**
	 * @test
	 */
	function exists () {
		$config = new ConnectionConfig("127.0.0.1", 25325, null);
		$response = [null, null];

		$callable = function() use ($config, &$response) {
			$redis = new Redis($config);
			$response[0] = (yield $redis->exists("foo"));
			$response[1] = (yield $redis->exists("bar"));
			$redis->close();
		};

		run($callable);

		$this->assertTrue($response[0]);
		$this->assertFalse($response[1]);
	}

	/**
	 * @test
	 */
	function del () {
		$config = new ConnectionConfig("127.0.0.1", 25325, null);
		$response = [null, null];

		$callable = function() use ($config, &$response) {
			$redis = new Redis($config);
			$response[0] = (yield $redis->exists("foo"));
			$redis->del("foo");
			$response[1] = (yield $redis->exists("foo"));
			$redis->close();
		};

		run($callable);

		$this->assertTrue($response[0]);
		$this->assertFalse($response[1]);
	}

	/**
	 * @test
	 */
	function expire () {
		$config = new ConnectionConfig("127.0.0.1", 25325, null);
		$response = [null, null, null, null];

		$callable = function() use ($config, &$response) {
			$redis = new Redis($config);
			$response[0] = (yield $redis->set("foo", "bar"));

			$redis->expire("foo", 1);
			$response[1] = (yield $redis->exists("foo"));

			yield "pause" => 900;
			$response[2] = (yield $redis->exists("foo"));

			yield "pause" => 100;
			$response[3] = (yield $redis->exists("foo"));

			$redis->close();
		};

		run($callable);

		$this->assertTrue($response[0]);
		$this->assertTrue($response[1]);
		$this->assertTrue($response[2]);
		$this->assertFalse($response[3]);
	}

	/**
	 * @test
	 */
	function dumpRestore () {
		$config = new ConnectionConfig("127.0.0.1", 25325, null);
		$response = [null, null];

		$callable = function() use ($config, &$response) {
			$redis = new Redis($config);
			$redis->set("foo", "bar");

			$dump = (yield $redis->dump("foo"));
			$redis->del("foo");
			$redis->restore("foo", $dump);
			$response[1] = (yield $redis->get("foo"));

			$redis->close();
		};

		run($callable);

		$this->assertEquals("bar", $response[1]);
	}

	/**
	 * @test
	 */
	function rename () {
		$config = new ConnectionConfig("127.0.0.1", 25325, null);
		$response = [null, null];

		$callable = function() use ($config, &$response) {
			$redis = new Redis($config);
			$redis->del("foo", "bar");

			$redis->set("foo", "test");
			$redis->rename("foo", "bar");

			$response[0] = (yield $redis->exists("foo"));
			$response[1] = (yield $redis->exists("bar"));

			$redis->close();
		};

		run($callable);

		$this->assertFalse($response[0]);
		$this->assertTrue($response[1]);
	}

	/**
	 * @test
	 */
	function randomkey () {
		$config = new ConnectionConfig("127.0.0.1", 25325, null);
		$response = [null, null];

		$callable = function() use ($config, &$response) {
			$redis = new Redis($config);

			$response[0] = (yield $redis->keys("*"));
			$response[1] = (yield $redis->randomkey());

			$redis->close();
		};

		run($callable);

		$this->assertTrue(sizeof($response[0]) > 0 && is_string($response[1]) || sizeof($response[0]) === 0 && is_null($response[1]));
	}
}

