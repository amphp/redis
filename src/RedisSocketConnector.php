<?php

namespace Amp\Redis;

use Amp\Cancellation;
use Amp\Socket\ConnectContext;
use Amp\Socket;
use Amp\Socket\SocketConnector;

class RedisSocketConnector implements RedisConnector
{
    public function __construct(
        private readonly ?SocketConnector $connector = null,
    ) {
    }

    function connect(Config $config, ?ConnectContext $context = null, ?Cancellation $cancellation = null): RespSocket
    {
        try {
            $context = ($context ?? new ConnectContext)->withConnectTimeout($config->getTimeout());
            $respSocket = new RespSocket(
                ($this->connector ?? Socket\socketConnector())->connect($config->getConnectUri(), $context)
            );
        } catch (Socket\SocketException $e) {
            throw new SocketException(
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
            if ([$response] = $respSocket->read()) {
                if ($response instanceof \Throwable) {
                    throw $response;
                }
            } else {
                throw new RedisException('Failed to connect to redis instance (' . $config->getConnectUri() . ')');
            }
        }

        return $respSocket;
    }
}
