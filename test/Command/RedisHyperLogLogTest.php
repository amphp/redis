<?php declare(strict_types=1);

namespace Amp\Redis\Command;

use Amp\Redis\IntegrationTest;

class RedisHyperLogLogTest extends IntegrationTest
{
    public function test(): void
    {
        $this->redis->flushAll();
        $hll1 = $this->redis->getHyperLogLog('hll1');
        $hll2 = $this->redis->getHyperLogLog('hll2');
        $hll3 = $this->redis->getHyperLogLog('hll3');

        $this->assertTrue($hll1->add('foo', 'bar', 'zap', 'a'));
        $this->assertTrue($hll2->add('a', 'b', 'c', 'foo'));
        $hll3->storeUnion('hll1', 'hll2');
        $this->assertSame(6, $hll3->count());
    }
}
