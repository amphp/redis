<?php

namespace Amp\Redis;

use function League\Uri\parse;

final class Config
{
    public const DEFAULT_HOST = 'localhost';
    public const DEFAULT_PORT = '6379';

    /**
     * @param string $uri
     *
     * @return self
     *
     * @throws RedisException
     */
    public static function fromUri(string $uri): self
    {
        if (\stripos($uri, 'tcp://') !== 0 && \stripos($uri, 'unix://') !== 0 && \stripos($uri, 'redis://') !== 0) {
            throw new RedisException('Invalid redis configuration URI, must start with tcp://, unix:// or redis://');
        }

        return new self($uri);
    }

    /** @var string */
    private $uri;
    /** @var string */
    private $password = '';
    /** @var int */
    private $database = 0;
    /** @var int */
    private $timeout = 5000;

    /**
     * @param string $uri
     *
     * @throws RedisException
     */
    private function __construct(string $uri)
    {
        $this->applyUri($uri);
    }

    public function getConnectUri(): string
    {
        return $this->uri;
    }

    public function getTimeout(): int
    {
        return $this->timeout;
    }

    public function getPassword(): string
    {
        return $this->password;
    }

    public function hasPassword(): bool
    {
        return $this->password !== '';
    }

    public function getDatabase(): int
    {
        return $this->database;
    }

    public function withTimeout(int $timeout): self
    {
        $clone = clone $this;
        $clone->timeout = $timeout;

        return $clone;
    }

    public function withPassword(string $password): self
    {
        $clone = clone $this;
        $clone->password = $password;

        return $clone;
    }

    public function withDatabase(int $database): self
    {
        $clone = clone $this;
        $clone->database = $database;

        return $clone;
    }

    /**
     * When using the "redis" schemes the URI is parsed according to the rules defined by the provisional registration
     * documents approved by IANA. If the URI has a password in its "user-information" part or a database number in the
     * "path" part these values override the values of "password" / "database" if they are present in the "query" part.
     *
     * @link http://www.iana.org/assignments/uri-schemes/prov/redis
     *
     * @param string $uri URI string.
     *
     * @throws RedisException
     */
    private function applyUri(string $uri): void
    {
        if ($uri === 'redis://') {
            $uri = 'redis://' . self::DEFAULT_HOST . ':' . self::DEFAULT_PORT;
        }

        try {
            $parsedUri = parse($uri);
        } catch (\Exception $exception) {
            throw new RedisException('Invalid redis configuration URI: ' . $uri);
        }

        \parse_str($parsedUri['query'] ?? '', $query);

        switch (\strtolower($parsedUri['scheme'])) {
            case 'tcp':
                $this->uri = 'tcp://' . \strtolower($parsedUri['host']) . ':' . (int) $parsedUri['port'];
                $this->database = (int) ($query['database'] ?? $query['db'] ?? 0);
                $this->password = $query['password'] ?? $query['pass'] ?? '';

                break;

            case 'unix':
                $this->uri = 'unix://' . $parsedUri['path'];
                $this->database = (int) ($query['database'] ?? $query['db'] ?? 0);
                $this->password = $query['password'] ?? $query['pass'] ?? '';

                break;

            case 'redis':
                $host = \strtolower($parsedUri['host'] ?? self::DEFAULT_HOST);
                $port = (int) ($parsedUri['port'] ?? self::DEFAULT_PORT);

                if ($host === '') {
                    $host = self::DEFAULT_HOST;
                }

                if ($port === 0) {
                    $port = self::DEFAULT_PORT;
                }

                $this->uri = 'tcp://' . $host . ':' . $port;

                if (\ltrim($parsedUri['path'], '/') !== '') {
                    $this->database = (int) \ltrim($parsedUri['path'], '/');
                } else {
                    $this->database = (int) ($query['db'] ?? 0);
                }

                if (isset($parsedUri['pass']) && $parsedUri['pass'] !== '') {
                    $this->password = $parsedUri['pass'];
                } else {
                    $this->password = $query['password'] ?? '';
                }

                break;
        }

        $this->timeout = (int) ($query['timeout'] ?? $this->timeout);
    }
}
