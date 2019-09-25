<?php

namespace Amp\Redis;

use Amp\Iterator;

class RedisSetTest extends IntegrationTest
{
    public function test(): \Generator
    {
        $this->redis->flushAll();
        $set = $this->redis->getSet('set-1');

        $this->assertSame(0, yield $set->getSize());
        $this->assertSame([], yield $set->getAll());
        $this->assertSame(3, yield $set->add('a', 'b', 'c'));
        $this->assertSame(3, yield $set->getSize());

        $values = yield $set->getAll();
        \sort($values);

        $this->assertEquals(['a', 'b', 'c'], $values);

        $a = $this->redis->getSet('set-a');
        $this->assertSame(4, yield $a->add('a', 'b', 'c', 'd'));

        $b = $this->redis->getSet('set-b');
        $this->assertSame(3, yield $b->add('a', 'c', 'e'));

        $this->assertSame(['a', 'c'], $this->sorted(yield $a->intersect('set-b')));
        $this->assertSame(['a', 'b', 'c', 'd', 'e'], $this->sorted(yield $a->union('set-b')));
        $this->assertSame(['b', 'd'], $this->sorted(yield $a->diff('set-b')));

        $this->assertSame(['a', 'b', 'c', 'd'], $this->sorted(yield Iterator\toArray($a->scan())));
        $this->assertSame(['a'], $this->sorted(yield Iterator\toArray($a->scan('a'))));
        $this->assertSame(2, yield $a->remove('a', 'b'));
        $this->assertSame(['c', 'd'], $this->sorted(yield $a->getRandomMembers(2)));
        $this->assertContains(yield $a->getRandomMember(), ['c', 'd']);
    }

    private function sorted(array $values): array
    {
        \sort($values);

        return $values;
    }
}
