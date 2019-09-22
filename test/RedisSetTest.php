<?php

namespace Amp\Redis;

class RedisSetTest extends IntegrationTest
{
    public function test(): \Generator
    {
        $this->redis->flushAll();
        $set = $this->redis->getSet('set1');

        $this->assertSame(0, yield $set->getSize());
        $this->assertSame([], yield $set->getAll());
        $this->assertSame(3, yield $set->add('a', 'b', 'c'));
        $this->assertSame(3, yield $set->getSize());

        $values = yield $set->getAll();
        \sort($values);

        $this->assertEquals(['a', 'b', 'c'], $values);
    }
}
