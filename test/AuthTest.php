<?php /** @noinspection PhpUnhandledExceptionInspection */

namespace Amp\Redis;

use Amp\Deferred;
use Amp\Delayed;
use Amp\PHPUnit\AsyncTestCase;

class AuthTest extends AsyncTestCase
{
    public static function setUpBeforeClass(): void
    {
        print \shell_exec('redis-server --daemonize yes --port 25325 --timeout 3 --pidfile /tmp/amp-redis.pid --requirepass secret');
        \sleep(2);
    }

    public static function tearDownAfterClass(): void
    {
        $pid = @\file_get_contents('/tmp/amp-redis.pid');
        @\unlink('/tmp/amp-redis.pid');

        if (!empty($pid)) {
            print \shell_exec("kill $pid");
            \sleep(2);
        }
    }

    public function testGarbageCollection(): \Generator
    {
        // This will hit stream select limits if garbage isn't collected as it should (e.g. due to circular references)
        for ($i = 0; $i < 10000; $i++) {
            $redis = new Redis(new RemoteExecutor(Config::fromUri('tcp://127.0.0.1:25325?password=secret')));
            $this->assertSame('PONG', yield $redis->echo('PONG'));
        }

        yield $redis->quit();
    }

    public function testSuccess(): \Generator
    {
        $redis = new Redis(new RemoteExecutor(Config::fromUri('tcp://127.0.0.1:25325?password=secret')));
        $this->assertSame('PONG', yield $redis->echo('PONG'));
        yield $redis->quit();
    }

    public function testFailure(): \Generator
    {
        $redis = new Redis(new RemoteExecutor(Config::fromUri('tcp://127.0.0.1:25325?password=wrong')));

        $this->expectException(QueryException::class);

        if (\method_exists($this, 'expectExceptionMessageMatches')) {
            $this->expectExceptionMessageMatches('(ERR invalid password|WRONGPASS invalid username-password pair)');
        } else {
            $this->expectExceptionMessageRegExp('(ERR invalid password|WRONGPASS invalid username-password pair)');
        }

        yield $redis->echo('PONG');
    }
}
