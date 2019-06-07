<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\Uri\InvalidUriException;
use Amp\Uri\Uri;

class ConnectionConfig
{
    const DEFAULT_HOST = "localhost";
    const DEFAULT_PORT = "6379";

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
     * @return ConnectionConfig
     * @throws InvalidUriException
     */
    public static function parse(string $uri): self
    {
        if (\strpos($uri, "tcp://") !== 0 &&
            \strpos($uri, "unix://") !== 0 &&
            \strpos($uri, "redis://") !== 0
        ) {
            throw new InvalidUriException("URI must start with tcp://, unix:// or redis://");
        }

        return new self($uri);
    }

    /**
     * @param string $uri
     * @throws InvalidUriException
     */
    public function __construct(string $uri)
    {
        $this->applyUri($uri);
    }

    /**
     * When using the "redis" schemes the URI is parsed according
     * to the rules defined by the provisional registration documents approved
     * by IANA. If the URI has a password in its "user-information" part or a
     * database number in the "path" part these values override the values of
     * "password" and "database" if they are present in the "query" part.
     *
     * @link http://www.iana.org/assignments/uri-schemes/prov/redis
     *
     * @param string $uri URI string.
     * @throws InvalidUriException
     */
    private function applyUri(string $uri)
    {
        $uri = new Uri($uri);

        switch ($uri->getScheme()) {
            case "tcp":
                $this->uri = "tcp://" . $uri->getHost() . ":" . $uri->getPort();
                $this->database = (int) ($uri->getQueryParameter("database") ?? 0);
                $this->password = $uri->getQueryParameter("password") ?? "";
                break;

            case "unix":
                $this->uri = "unix://" . $uri->getPath();
                $this->database = (int) ($uri->getQueryParameter("database") ?? 0);
                $this->password = $uri->getQueryParameter("password") ?? "";
                break;

            case "redis":
                $this->uri = \sprintf(
                    "tcp://%s:%d",
                    $uri->getHost() ?? self::DEFAULT_HOST,
                    $uri->getPort() ?? self::DEFAULT_PORT
                );
                if ($uri->getPath() !== "/") {
                    $this->database = (int) \ltrim($uri->getPath(), "/");
                } else {
                    $this->database = (int) ($uri->getQueryParameter("db") ?? 0);
                }
                if (!empty($uri->getPass())) {
                    $this->password = $uri->getPass();
                } else {
                    $this->password = $uri->getQueryParameter("password") ?? "";
                }
                break;
        }

        $this->timeout = $uri->getQueryParameter("timeout") ?? $this->timeout;
    }

    public function getUri(): string
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
        return (bool) \strlen($this->password);
    }

    public function getDatabase(): int
    {
        return $this->database;
    }
}
