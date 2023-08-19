<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\Cancellation;
use Amp\Redis\RedisConfig;
use Amp\Redis\RedisException;
use Amp\Redis\RedisSocketException;
use Amp\Socket;
use Amp\Socket\ConnectContext;
use Amp\Socket\SocketConnector;

final class SocketRedisConnector implements RedisConnector
{
    public function __construct(
        private readonly ?SocketConnector $connector = null,
    ) {
    }

    public function connect(
        RedisConfig $config,
        ?ConnectContext $context = null,
        ?Cancellation $cancellation = null,
    ): RedisChannel {
        try {
            $context = ($context ?? new ConnectContext)->withConnectTimeout($config->getTimeout());
            $respSocket = new SocketRedisChannel(
                ($this->connector ?? Socket\socketConnector())->connect($config->getConnectUri(), $context)
            );
        } catch (Socket\SocketException $e) {
            throw new RedisSocketException(
                'Failed to connect to redis instance (' . $config->getConnectUri() . ')',
                0,
                $e
            );
        }

        $readsNeeded = 0;

        if ($config->hasPassword()) {
            $readsNeeded++;
            $respSocket->write('AUTH', $config->getPassword());
        }

        if ($config->getDatabase() !== 0) {
            $readsNeeded++;
            $respSocket->write('SELECT', (string) $config->getDatabase());
        }

        for ($i = 0; $i < $readsNeeded; $i++) {
            if (!($respSocket->read()?->unwrap())) {
                throw new RedisException('Failed to connect to redis instance (' . $config->getConnectUri() . ')');
            }
        }

        return $respSocket;
    }
}
