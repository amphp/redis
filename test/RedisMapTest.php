<?php declare(strict_types=1);

namespace Amp\Redis;

class RedisMapTest extends IntegrationTest
{
    public function test(): void
    {
        $this->redis->flushAll();
        $map = $this->redis->getMap('map1');

        $this->assertSame(0, $map->getSize());
        $this->assertSame([], $map->getKeys());
        $this->assertSame([], $map->getAll());

        $map->setValues([
            'foo' => 'bar',
            'rofl' => 'lol',
        ]);

        $this->assertSame('bar', $map->getValue('foo'));
        $this->assertSame('lol', $map->getValue('rofl'));
        $this->assertSame(2, $map->getSize());
        $this->assertTrue($map->hasKey('foo'));
        $this->assertSame(1, $map->remove('foo'));
        $this->assertFalse($map->hasKey('foo'));

        $this->assertTrue($map->setValue('number', '1'));
        $this->assertSame(2, $map->increment('number'));
        $this->assertSame(3.5, $map->incrementByFloat('number', 1.5));

        $this->assertSame(['lol', '3.5'], $map->getValues('rofl', 'number'));
        $this->assertSame(3, $map->getLength('rofl'));

        $this->assertSame([['rofl', 'lol'], ['number', '3.5']], \iterator_to_array($map->scan()));
        $this->assertSame([['number', '3.5']], \iterator_to_array($map->scan('num*')));
        $this->assertSame([], \iterator_to_array($map->scan('none*')));
        $this->assertSame([], \iterator_to_array($map->scan('none*', 1)));

        $this->assertFalse($map->setValueWithoutOverwrite('rofl', 'test'));
        $this->assertSame('lol', $map->getValue('rofl'));

        $this->assertTrue($map->setValueWithoutOverwrite('new', 'test'));
        $this->assertSame('test', $map->getValue('new'));
    }
}
