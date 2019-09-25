<?php

namespace Amp\Redis;

class RedisHyperLogLogTest extends IntegrationTest
{
    public function test(): \Generator
    {
        yield $this->redis->flushAll();
        $hll1 = $this->redis->getHyperLogLog('hll1');
        $hll2 = $this->redis->getHyperLogLog('hll2');
        $hll3 = $this->redis->getHyperLogLog('hll3');

        $this->assertTrue(yield $hll1->add('foo', 'bar', 'zap', 'a'));
        $this->assertTrue(yield $hll2->add('a', 'b', 'c', 'foo'));
        $this->assertNull(yield $hll3->storeUnion('hll1', 'hll2'));
        $this->assertSame(6, yield $hll3->count());
    }
}
