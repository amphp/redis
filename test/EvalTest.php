<?php

namespace Amp\Redis;

use Amp\Loop;

class EvalTest extends RedisTest
{
    public function testEval()
    {
        Loop::run(function () {
            $redis = new Client('tcp://127.0.0.1:25325');
            yield $redis->set('foo', 'eval-test');

            $script = "return redis.call('get','foo')";

            $value = yield $redis->eval($script, 'foo');
            $this->assertSame('eval-test', $value);
        });
    }
}
