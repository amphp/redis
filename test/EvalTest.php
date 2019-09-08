<?php

namespace Amp\Redis;

class EvalTest extends IntegrationTest
{
    public function testEval(): \Generator
    {
        $redis = $this->createInstance();
        yield $redis->set('foo', 'eval-test');

        $script = "return redis.call('get','foo')";

        $value = yield $redis->eval($script, ['foo']);
        $this->assertSame('eval-test', $value);
    }
}
