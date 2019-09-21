<?php

namespace Amp\Redis;

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
    }
}
