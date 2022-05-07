<?php

namespace Amp\Redis\Cluster;

use Amp\Promise;
use Amp\Redis\Config;
use Amp\Redis\QueryException;
use Amp\Redis\QueryExecutor;
use Amp\Redis\RemoteExecutor;
use Amp\Redis\SocketException;
use function Amp\call;

final class ClusterExecutor implements QueryExecutor
{
    /** @var QueryExecutor[] */
    private $defaultExecutors;

    /** @var ClusterRouter */
    private $router;

    public function execute(array $query, callable $responseTransform = null): Promise
    {
        // TODO Get from router

        return call(function () use ($query, $responseTransform) {
            $defaultAttempt = 0;
            $executor = $this->defaultExecutors[0];
            $mode = 'default';

            start:

            try {
                if ($mode === 'ask') {
                    $executor->execute(['ASKING']);
                }

                return yield $executor->execute($query, $responseTransform);
            } catch (QueryException $queryException) {
                if (\strpos($queryException->getMessage(), 'MOVED ') !== false) {
                    [, $slot, $ipAndPort] = \explode(' ', $queryException->getMessage());

                    $executor = new RemoteExecutor(Config::fromUri('redis://' . $ipAndPort));

                    $this->router->update($slot, $executor);

                    $mode = 'moved';

                    goto start;
                }

                if (\strpos($queryException->getMessage(), 'ASK ') !== false) {
                    [, $slot, $ipAndPort] = \explode(' ', $queryException->getMessage());

                    $executor = new RemoteExecutor(Config::fromUri('redis://' . $ipAndPort));

                    $mode = 'ask';

                    goto start;
                }

                throw $queryException;
            } catch (SocketException $socketException) {
                if ($mode === 'default') {
                    if (++$defaultAttempt < \count($this->defaultExecutors)) {
                        $executor = $this->defaultExecutors[$defaultAttempt];

                        goto start;
                    }
                }

                throw $socketException;
            }
        });
    }
}
