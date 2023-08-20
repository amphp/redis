<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\Redis\Command\Option\RedisSortOptions;

class RedisListTest extends IntegrationTest
{
    public function test(): void
    {
        $this->redis->flushAll();
        $list = $this->redis->getList('list1');

        $this->assertSame(0, $list->getSize());
        $this->assertSame(1, $list->pushHead('a'));
        $this->assertSame(2, $list->pushHead('b'));
        $this->assertSame(2, $list->getSize());
        $this->assertSame(['b', 'a'], $list->getRange());
        $this->assertSame(['b'], $list->getRange(0, 0));
        $this->assertSame(['a'], $list->getRange(1));
        $this->assertSame('b', $list->get('0'));
        $this->assertSame('a', $list->get('1'));
        $list->set(0, 'b+');
        $this->assertSame(3, $list->pushTail('c'));
        $this->assertSame('c', $list->popTail());
        $this->assertSame('b+', $list->popHead());
        $this->assertSame('a', $list->popHead());
        $this->assertNull($list->popHead());
        $this->assertNull($list->popTail());

        $this->assertSame(1, $list->pushTail('x'));
        $this->assertSame(4, $list->pushTailIfExists('a', 'b', 'c'));
        $list->trim(1);
        $this->assertSame(['b', 'c'], $list->getRange(1, 2));
        $list->trim(0, -2);
        $this->assertSame(2, $list->getSize());
        $this->assertSame(['a', 'b'], $list->getRange());

        $this->assertSame(3, $list->insertAfter('a', 'x'));
        $this->assertSame(['a', 'x', 'b'], $list->getRange());

        $this->assertSame(4, $list->insertBefore('a', 'y'));
        $this->assertSame(['y', 'a', 'x', 'b'], $list->getRange());

        $this->assertSame(1, $list->remove('a'));
        $this->assertSame(['y', 'x', 'b'], $list->getRange());

        $this->assertSame(['b', 'x', 'y'], $list->sort((new RedisSortOptions)->withLexicographicSorting()));

        $this->assertSame('y', $list->popHeadBlocking());
        $this->assertSame('b', $list->popTailBlocking());

        $this->assertNull($this->redis->getList('nonexistent')->popHeadBlocking(1));
        $this->assertNull($this->redis->getList('nonexistent')->popTailBlocking(1));
        $this->assertNull($this->redis->getList('nonexistent')->popTailPushHeadBlocking('nonexistent', 1));
    }
}
