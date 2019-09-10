<?php /** @noinspection PhpUnhandledExceptionInspection */

namespace Amp\Redis;

use Amp\PHPUnit\AsyncTestCase;

class DownTest extends AsyncTestCase
{
    public function test(): \Generator
    {
        $this->expectException(SocketException::class);

        $redis = new Redis(new RemoteExecutor(Config::fromUri('tcp://127.0.0.1:25325')));
        yield $redis->ping();
    }
}
