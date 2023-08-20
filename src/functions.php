<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\Redis\Connection\Authenticator;
use Amp\Redis\Connection\ChannelLink;
use Amp\Redis\Connection\DatabaseSelector;
use Amp\Redis\Connection\RedisConnector;
use Amp\Redis\Connection\SocketRedisConnector;
use Amp\Socket\ConnectContext;

function createRedisChannelFactory(RedisConfig|string $config, ?RedisConnector $channelFactory = null): RedisConnector
{
    if (\is_string($config)) {
        $config = RedisConfig::fromUri($config);
    }

    $channelFactory ??= new SocketRedisConnector(
        $config->getConnectUri(),
        (new ConnectContext())->withConnectTimeout($config->getTimeout())
    );

    if ($config->hasPassword()) {
        $channelFactory = new Authenticator($config->getPassword(), $channelFactory);
    }

    if ($config->getDatabase() !== 0) {
        $channelFactory = new DatabaseSelector($config->getDatabase(), $channelFactory);
    }

    return $channelFactory;
}

function createRedisClient(RedisConfig|string $config, ?RedisConnector $channelFactory = null): RedisClient
{
    return new RedisClient(new ChannelLink(createRedisChannelFactory($config, $channelFactory)));
}
