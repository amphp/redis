<?php

namespace Amp\Redis;

use Amp\NativeReactor;
use function Amp\run;

class DownTest extends \PHPUnit_Framework_TestCase {
	/**
	 * @test
	 * @expectedException \Amp\Redis\RedisException
	 */
	function ping () {
		(new NativeReactor())->run(function ($reactor) {
			$redis = new Redis($reactor, ["host" => "127.0.0.1:25325"]);
			yield $redis->ping();
		});
	}
}
