<?php

namespace Amp\Redis;

use Amp\Iterator;

class RedisMapTest extends IntegrationTest
{
    public function test(): \Generator
    {
        $this->redis->flushAll();
        $map = $this->redis->getMap('map1');

        $this->assertSame(0, yield $map->getSize());
        $this->assertSame([], yield $map->getKeys());
        $this->assertSame([], yield $map->getAll());

        $this->assertNull(yield $map->setValues([
            'foo' => 'bar',
            'rofl' => 'lol',
        ]));

        $this->assertSame('bar', yield $map->getValue('foo'));
        $this->assertSame('lol', yield $map->getValue('rofl'));
        $this->assertSame(2, yield $map->getSize());
        $this->assertTrue(yield $map->hasKey('foo'));
        $this->assertSame(1, yield $map->remove('foo'));
        $this->assertFalse(yield $map->hasKey('foo'));

        $this->assertTrue(yield $map->setValue('number', 1));
        $this->assertSame(2, yield $map->increment('number'));
        $this->assertSame(3.5, yield $map->incrementByFloat('number', 1.5));

        $this->assertSame(['lol', '3.5'], yield $map->getValues('rofl', 'number'));
        $this->assertSame(3, yield $map->getLength('rofl'));

        $this->assertSame([['rofl', 'lol'], ['number', '3.5']], yield Iterator\toArray($map->scan()));
        $this->assertSame([['number', '3.5']], yield Iterator\toArray($map->scan('num*')));
        $this->assertSame([], yield Iterator\toArray($map->scan('none*')));
        $this->assertSame([], yield Iterator\toArray($map->scan('none*', 1)));

        $this->assertFalse(yield $map->setValueWithoutOverwrite('rofl', 'test'));
        $this->assertSame('lol', yield $map->getValue('rofl'));

        $this->assertTrue(yield $map->setValueWithoutOverwrite('new', 'test'));
        $this->assertSame('test', yield $map->getValue('new'));
    }
}
