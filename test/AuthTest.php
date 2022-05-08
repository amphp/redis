<?php

namespace Amp\Redis;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Process\Process;
use function Amp\delay;

class AuthTest extends AsyncTestCase
{
    private const PORT = 25325;
    private const TIMEOUT = 3;
    private const PASSWORD = 'secret';

    private const URI_FORMAT = 'redis://localhost:%d?password=%s';

    private static Process $process;

    public static function setUpBeforeClass(): void
    {
        self::$process = Process::start([
            'redis-server',
            '--port', self::PORT,
            '--timeout', self::TIMEOUT,
            '--pidfile', '/tmp/amp-redis.pid',
            '--requirepass', self::PASSWORD,
        ]);

        delay(1); // Give redis-server process time to start accepting connections.
    }

    public static function tearDownAfterClass(): void
    {
        self::$process->signal(\defined('SIGTERM') ? \SIGTERM : 15);
        self::$process->join();
    }

    public function testSuccess(): void
    {
        $redis = new Redis(new RemoteExecutor(
            RedisConfig::fromUri(\sprintf(self::URI_FORMAT, self::PORT, self::PASSWORD))
        ));
        $this->assertSame('PONG', $redis->echo('PONG'));
        $redis->quit();
    }

    public function testFailure(): void
    {
        $redis = new Redis(new RemoteExecutor(
            RedisConfig::fromUri(\sprintf(self::URI_FORMAT, self::PORT, 'wrong'))
        ));
        $this->expectException(RedisSocketException::class);

        $this->expectExceptionMessage('invalid');

        $redis->echo('PONG');
    }
}
