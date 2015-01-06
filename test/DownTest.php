<?php

namespace Amp\Redis;

use Amp\NativeReactor;

class DownTest extends \PHPUnit_Framework_TestCase {
	/**
	 * @test
	 */
	function ping () {
		(new NativeReactor)->run(function ($reactor) {
			$redis = new Redis($reactor, ["host" => "127.0.0.1:25325", "password" => "secret"]);

			try {
				yield $redis->ping();
				$this->fail("no exception thrown");
			} catch (\Exception $e) {
				$this->assertInstanceOf("\\Amp\\Redis\\RedisException", $e);
			}
		});
	}
}
