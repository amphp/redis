<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\PHPUnit\AsyncTestCase;
use Amp\Redis\Connection\ChannelRedisLink;
use Amp\Redis\Connection\SocketRedisChannelFactory;

class DownTest extends AsyncTestCase
{
    public function test(): void
    {
        $this->expectException(RedisSocketException::class);

        $config = RedisConfig::fromUri('tcp://127.0.0.1:25325');
        $redis = new Redis(new RedisClient(new ChannelRedisLink($config, new SocketRedisChannelFactory($config))));
        $redis->ping();
    }
}
