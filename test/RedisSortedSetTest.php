<?php declare(strict_types=1);

namespace Amp\Redis;

class RedisSortedSetTest extends IntegrationTest
{
    public function test(): void
    {
        $this->redis->flushAll();
        $set = $this->redis->getSortedSet('sorted-set-1');

        $this->assertSame(2, $set->add([
            'foo' => 1,
            'bar' => 3,
        ]));

        $this->assertSame([['foo', 1.0], ['bar', 3.0]], \iterator_to_array($set->scan()));
        $this->assertSame([['foo', 1.0]], \iterator_to_array($set->scan('f*')));
        $this->assertSame([['foo', 1.0]], \iterator_to_array($set->scan('f*', 1)));

        $this->assertSame(1.0, $set->getScore('foo'));
        $this->assertSame(3.0, $set->getScore('bar'));

        $this->assertSame(2, $set->count(0, 10));
        $this->assertSame(1, $set->count(1, 1));
        $this->assertSame(1, $set->count(3, 3));

        $this->assertSame(0, $set->getRank('foo'));
        $this->assertSame(1, $set->getRank('bar'));

        $this->assertSame(1, $set->getReversedRank('foo'));
        $this->assertSame(0, $set->getReversedRank('bar'));

        $this->assertSame(1, $set->remove('foo'));
        $this->assertSame(0, $set->getRank('bar'));
    }
}
