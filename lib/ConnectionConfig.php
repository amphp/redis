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

    /** @var string|null */
    private $password;

    /** @var int */
    private $database = 0;

    /** @var int */
    private $timeout = 5000;

    /**
     * @param string $uri
     * @return ConnectionConfig
     * @throws InvalidUriException
     * @throws ConnectionConfigException
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
     * @throws ConnectionConfigException
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
     * @throws ConnectionConfigException
     */
    private function applyUri(string $uri)
    {
        $uri = new Uri($uri);

        switch ($uri->getScheme()) {
            case "tcp":
                $this->uri = "tcp://" . $uri->getHost() . ":" . $uri->getPort();
                $this->database = (int) ($uri->getQueryParameter("database") ?? 0);
                if ($uri->hasQueryParameter("database")) {
                    $this->setPassword($uri->getQueryParameter("password"));
                }
                break;

            case "unix":
                $this->uri = "unix://" . $uri->getPath();
                $this->database = (int) ($uri->getQueryParameter("database") ?? 0);
                if ($uri->hasQueryParameter("database")) {
                    $this->setPassword($uri->getQueryParameter("password"));
                }
                break;

            case "redis":
                $this->uri = \sprintf(
                    "tcp://%s:%d",
                    $uri->getHost() ?? self::DEFAULT_HOST,
                    $uri->getPort() ?? self::DEFAULT_PORT
                );

                $databaseInPath = false;
                if ($uri->getPath() !== "/") {
                    $this->database = (int) \ltrim($uri->getPath(), "/");
                    $databaseInPath = true;
                }
                if (!$databaseInPath && $uri->hasQueryParameter("db")) {
                    $this->database = (int) ($uri->getQueryParameter("db") ?? 0);
                } else {
                    throw new ConnectionConfigException(
                        "Passing a database name in path and query is not allowed"
                    );
                }

                if (!empty($uri->getPass())) {
                    $this->setPassword($uri->getPass());
                }
                if ($this->hasPassword() && $uri->hasQueryParameter("password")) {
                    throw new ConnectionConfigException(
                        "Passing a password in user-info and query is not allowed"
                    );
                }
                if (!$this->hasPassword() && $uri->hasQueryParameter("password")) {
                    $this->setPassword($uri->getQueryParameter("password"));
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

    /**
     * @throws ConnectionConfigException
     */
    private function setPassword(string $password)
    {
        if (empty($password)) {
            throw new ConnectionConfigException("Password cannot be an empty string");
        }
        $this->password = $password;
    }

    public function getPassword(): string
    {
        return $this->password;
    }

    public function hasPassword(): bool
    {
        return $this->password !== null;
    }

    public function getDatabase(): int
    {
        return $this->database;
    }
}
