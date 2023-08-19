<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\Redis\Connection\RedisConnector;
use Amp\Redis\Connection\SocketRedisConnector;
use function Amp\delay;

class BasicTest extends IntegrationTest
{
    private RedisConnector $connector;

    public function setUp(): void
    {
        parent::setUp();

        $this->connector = new SocketRedisConnector();
    }

    public function testRaw(): void
    {
        $this->setTimeout(5);

        $config = RedisConfig::fromUri($this->getUri());
        $resp = $this->connector->connect($config);

        $resp->write('PING');

        $this->assertSame('PONG', $resp->read()->unwrap());

        $resp->close();

        $this->expectException(RedisException::class);
        $this->expectExceptionMessage('Redis connection already closed');

        $resp->write('PING');
    }

    public function testRawCloseReadRemote(): void
    {
        $this->setTimeout(5);

        $config = RedisConfig::fromUri($this->getUri());
        $resp = $this->connector->connect($config);

        $resp->write('QUIT');

        $this->assertSame('OK', $resp->read()->unwrap());

        $this->assertNull($resp->read());
    }

    public function testRawCloseReadLocal(): void
    {
        $this->setTimeout(5);

        $config = RedisConfig::fromUri($this->getUri());
        $resp = $this->connector->connect($config);

        $resp->write('QUIT');

        $this->assertSame('OK', $resp->read()->unwrap());

        $resp->close();

        $this->assertNull($resp->read());
    }

    public function testRawCloseWriteRemote(): void
    {
        $this->setTimeout(5);

        $config = RedisConfig::fromUri($this->getUri());
        $resp = $this->connector->connect($config);

        $resp->write('QUIT');

        $this->assertSame('OK', $resp->read()->unwrap());

        delay(0);

        $this->expectException(RedisException::class);

        $resp->write('PING');
    }

    public function testRawCloseWriteLocal(): void
    {
        $this->setTimeout(5);

        $config = RedisConfig::fromUri($this->getUri());
        $resp = $this->connector->connect($config);

        $resp->write('QUIT');

        $this->assertSame('OK', $resp->read()->unwrap());

        delay(0);

        $resp->close();

        $this->expectException(RedisException::class);
        $this->expectExceptionMessage('Redis connection already closed');

        $resp->write('PING');
    }

    public function testConnect(): void
    {
        $this->assertSame('PONG', $this->createInstance()->echo('PONG'));
    }

    public function testLongPayload(): void
    {
        $redis = $this->createInstance();
        $payload = \str_repeat('a', 6000000);
        $redis->set('foobar', $payload);
        $this->assertSame($payload, $redis->get('foobar'));
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
        $this->assertSame('2', $redis->echo('2'));
    }

    /**
     * @medium
     */
    public function testTimeout(): void
    {
        $redis = $this->createInstance();
        $redis->echo('1');
        delay(0.1);
        $this->assertSame('2', $redis->echo('2'));
    }
}
