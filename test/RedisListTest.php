<?php

namespace Amp\Redis;


class RedisListTest extends IntegrationTest
{
    public function test(): \Generator
    {
        $this->redis->flushAll();
        $list = $this->redis->getList('list1');

        $this->assertSame(0, yield $list->getSize());
        $this->assertSame(1, yield $list->pushHead('a'));
        $this->assertSame(2, yield $list->pushHead('b'));
        $this->assertSame(2, yield $list->getSize());
        $this->assertSame(['b', 'a'], yield $list->getRange());
        $this->assertSame(['b'], yield $list->getRange(0, 0));
        $this->assertSame(['a'], yield $list->getRange(1));
        $this->assertSame('b', yield $list->get(0));
        $this->assertSame('a', yield $list->get(1));
        $this->assertNull(yield $list->set(0, 'b+'));
        $this->assertSame(3, yield $list->pushTail('c'));
        $this->assertSame('c', yield $list->popTail());
        $this->assertSame('b+', yield $list->popHead());
        $this->assertSame('a', yield $list->popHead());
        $this->assertNull(yield $list->popHead());
        $this->assertNull(yield $list->popTail());

        $this->assertSame(1, yield $list->pushTail('x'));
        $this->assertSame(4, yield $list->pushTailIfExists('a', 'b', 'c'));
        $this->assertNull(yield $list->trim(1));
        $this->assertSame(['b', 'c'], yield $list->getRange(1, 2));
        $this->assertNull(yield $list->trim(0, -2));
        $this->assertSame(2, yield $list->getSize());
        $this->assertSame(['a', 'b'], yield $list->getRange());
    }
}
