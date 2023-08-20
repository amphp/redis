<?php declare(strict_types=1);

namespace Amp\Redis\Connection;

use Amp\Cancellation;
use Amp\CancelledException;
use Amp\ForbidCloning;
use Amp\ForbidSerialization;
use Amp\Redis\RedisConfig;
use Amp\Redis\RedisException;
use Amp\Redis\RedisSocketException;
use Amp\Socket;
use Amp\Socket\ConnectContext;
use Amp\Socket\SocketConnector;

final class SocketChannelFactory implements RedisChannelFactory
{
    use ForbidCloning;
    use ForbidSerialization;

    private readonly ConnectContext $connectContext;

    public function __construct(
        private readonly RedisConfig $config,
        ?ConnectContext $connectContext = null,
        private readonly ?SocketConnector $socketConnector = null,
    ) {
        $this->connectContext = ($connectContext ?? new ConnectContext)->withConnectTimeout($config->getTimeout());
    }

    /**
     * @throws CancelledException
     * @throws RedisException
     * @throws RedisSocketException
     */
    public function createChannel(?Cancellation $cancellation = null): RedisChannel
    {
        try {
            $socketConnector = $this->socketConnector ?? Socket\socketConnector();
            $socket = $socketConnector->connect($this->config->getConnectUri(), $this->connectContext, $cancellation);

        } catch (Socket\SocketException $e) {
            throw new RedisSocketException(
                'Failed to connect to redis instance (' . $this->config->getConnectUri() . ')',
                0,
                $e
            );
        }

        $channel = new SocketChannel($socket);

        $readsNeeded = 0;

        if ($this->config->hasPassword()) {
            $readsNeeded++;
            $channel->send('AUTH', $this->config->getPassword());
        }

        if ($this->config->getDatabase() !== 0) {
            $readsNeeded++;
            $channel->send('SELECT', (string)$this->config->getDatabase());
        }

        for ($i = 0; $i < $readsNeeded; $i++) {
            if (!($channel->receive()?->unwrap())) {
                throw new RedisException('Failed to connect to redis instance (' . $this->config->getConnectUri() . ')');
            }
        }

        return $channel;
    }
}
