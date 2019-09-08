<?php

namespace Amp\Redis;

use Amp\PHPUnit\AsyncTestCase;

class DownTest extends AsyncTestCase
{
    public function test(): \Generator
    {
        $this->expectException(ConnectException::class);

        $redis = new Redis(new RemoteExecutor('tcp://127.0.0.1:25325'));
        yield $redis->ping();
    }
}
