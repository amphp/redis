<?php

namespace Amp\Redis;

class EvalTest extends IntegrationTest
{
    public function testEval(): void
    {
        $redis = $this->createInstance();
        $redis->set('foo', 'eval-test');

        $script = "return redis.call('get','foo')";

        $value = $redis->eval($script, ['foo']);
        $this->assertSame('eval-test', $value);
    }
}
