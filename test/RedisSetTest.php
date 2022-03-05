<?php

namespace Amp\Redis;

class RedisSetTest extends IntegrationTest
{
    public function test(): void
    {
        $this->redis->flushAll();
        $set = $this->redis->getSet('set-1');

        $this->assertSame(0, $set->getSize());
        $this->assertSame([], $set->getAll());
        $this->assertSame(3, $set->add('a', 'b', 'c'));
        $this->assertSame(3, $set->getSize());

        $values = $set->getAll();
        \sort($values);

        $this->assertEquals(['a', 'b', 'c'], $values);

        $a = $this->redis->getSet('set-a');
        $this->assertSame(4, $a->add('a', 'b', 'c', 'd'));

        $b = $this->redis->getSet('set-b');
        $this->assertSame(3, $b->add('a', 'c', 'e'));

        $this->assertSame(['a', 'c'], $this->sorted($a->intersect('set-b')));
        $this->assertSame(['a', 'b', 'c', 'd', 'e'], $this->sorted($a->union('set-b')));
        $this->assertSame(['b', 'd'], $this->sorted($a->diff('set-b')));

        $this->assertSame(['a', 'b', 'c', 'd'], $this->sorted(\iterator_to_array($a->scan())));
        $this->assertSame(['a'], $this->sorted(\iterator_to_array($a->scan('a'))));
        $this->assertSame(2, $a->remove('a', 'b'));
        $this->assertSame(['c', 'd'], $this->sorted($a->getRandomMembers(2)));
        $this->assertContains($a->getRandomMember(), ['c', 'd']);
    }

    private function sorted(array $values): array
    {
        \sort($values);

        return $values;
    }
}
