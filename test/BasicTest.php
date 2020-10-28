<?php

namespace Amp\Redis;

use Amp\ByteStream\ClosedException;
use Amp\Delayed;
use function Amp\delay;

class BasicTest extends IntegrationTest
{
    public function testRaw(): void
    {
        $this->setTimeout(5000);

        $config = Config::fromUri($this->getUri());
        /** @var RespSocket $resp */
        $resp = connect($config);

        $resp->write('PING');

        $this->assertEquals(['PONG'], $resp->read());

        $resp->close();

        $this->expectException(ClosedException::class);
        $this->expectExceptionMessage('Redis connection already closed');

        $resp->write('PING');
    }

    public function testRawCloseReadRemote(): void
    {
        $this->setTimeout(5000);

        $config = Config::fromUri($this->getUri());
        /** @var RespSocket $resp */
        $resp = connect($config);

        $resp->write('QUIT');

        $this->assertEquals(['OK'], $resp->read());

        $this->assertNull($resp->read());
    }

    public function testRawCloseReadLocal(): void
    {
        $this->setTimeout(5000);

        $config = Config::fromUri($this->getUri());
        /** @var RespSocket $resp */
        $resp = connect($config);

        $resp->write('QUIT');

        $this->assertEquals(['OK'], $resp->read());

        $resp->close();

        $this->assertNull($resp->read());
    }

    public function testRawCloseWriteRemote(): void
    {
        $this->setTimeout(5000);

        $config = Config::fromUri($this->getUri());
        /** @var RespSocket $resp */
        $resp = connect($config);

        $resp->write('QUIT');

        $this->assertEquals(['OK'], $resp->read());

        delay(0);

        $this->expectException(ClosedException::class);

        $resp->write('PING');
    }

    public function testRawCloseWriteLocal(): void
    {
        $this->setTimeout(5000);

        $config = Config::fromUri($this->getUri());
        /** @var RespSocket $resp */
        $resp = connect($config);

        $resp->write('QUIT');

        $this->assertEquals(['OK'], $resp->read());

        delay(0);

        $resp->close();

        $this->expectException(ClosedException::class);
        $this->expectExceptionMessage('Redis connection already closed');

        $resp->write('PING');
    }

    public function testConnect(): void
    {
        $this->assertEquals('PONG', $this->createInstance()->echo('PONG'));
    }

    public function testLongPayload(): void
    {
        $redis = $this->createInstance();
        $payload = \str_repeat('a', 6000000);
        $redis->set('foobar', $payload);
        $this->assertEquals($payload, $redis->get('foobar'));
    }

    public function testAcceptsOnlyScalars(): void
    {
        $this->expectException(\TypeError::class);

        $redis = $this->createInstance();
        /** @noinspection PhpParamsInspection */
        $redis->set('foobar', ['abc']);
    }

    public function testMultiCommand(): void
    {
        $redis = $this->createInstance();
        $redis->echo('1');
        $this->assertEquals('2', ($redis->echo('2')));
    }

    /**
     * @medium
     */
    public function testTimeout(): void
    {
        $redis = $this->createInstance();
        $redis->echo('1');
        new Delayed(8000);
        $this->assertEquals('2', ($redis->echo('2')));
    }
}
