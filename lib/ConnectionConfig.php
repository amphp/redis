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
        if (stripos($uri, 'unix://') === 0) {
            // parse_url() can parse unix:/path/to/sock so we do not need the
            // unix:///path/to/sock hack
            $uri = str_ireplace('unix://', 'unix:', $uri);
        }
        if (\strpos($uri, "tcp://") !== 0 &&
            \strpos($uri, "unix:") !== 0 &&
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
        if ($uri === "redis://") {
            // Amp/Uri/Uri fails when no host neither a port passed,
            // although this is still valid RedisURL
            $uri = \sprintf("redis://%s:%d", self::DEFAULT_HOST, self::DEFAULT_PORT);
        }
        $uri = new Uri($uri);

        switch ($uri->getScheme()) {
            case "tcp":
                $this->uri = "tcp://" . $uri->getHost() . ":" . $uri->getPort();
                if ($uri->hasQueryParameter("database")) {
                    $this->setDatabase($uri->getQueryParameter("database"));
                }
                if ($uri->hasQueryParameter("password")) {
                    $this->setPassword($uri->getQueryParameter("password"));
                }
                break;

            case "unix":
                $this->uri = "unix:" . $uri->getPath();
                if ($uri->hasQueryParameter("database")) {
                    $this->setDatabase($uri->getQueryParameter("database"));
                }
                if ($uri->hasQueryParameter("password")) {
                    $this->setPassword($uri->getQueryParameter("password"));
                }
                break;

            case "redis":
                $this->uri = \sprintf(
                    "tcp://%s:%d",
                    $uri->getHost() ?? self::DEFAULT_HOST,
                    $uri->getPort() ? $uri->getPort() : self::DEFAULT_PORT
                );

                $databaseInPath = false;
                if ($uri->getPath() !== "") {
                    $this->setDatabase(\ltrim($uri->getPath(), "/"));
                    $databaseInPath = true;
                }
                if ($databaseInPath && $uri->hasQueryParameter("db")) {
                    throw new ConnectionConfigException(
                        "Passing a database name in path and query is not allowed"
                    );
                } elseif ($uri->hasQueryParameter("db")) {
                    $this->setDatabase($uri->getQueryParameter("db"));
                }

                if ($uri->getPass() !== "") {
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
        if ($uri->hasQueryParameter("timeout")) {
            $this->setTimeout($uri->getQueryParameter("timeout"));
        }
    }

    public function getUri(): string
    {
        return $this->uri;
    }

    /**
     * @param string $timeout
     * @throws ConnectionConfigException
     */
    private function setTimeout(string $timeout)
    {
        if (!\is_numeric($timeout) || (int) $timeout < 0) {
            throw new ConnectionConfigException(
                "Timeout has to be positive integer"
            );
        }
        $this->timeout = (int) $timeout;
    }

    public function getTimeout(): int
    {
        return $this->timeout;
    }

    /**
     * @param string $password
     * @throws ConnectionConfigException
     */
    private function setPassword(string $password)
    {
        if ($password === "") {
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

    /**
     * @param string $database
     * @throws ConnectionConfigException
     */
    private function setDatabase(string $database)
    {
        if (!\is_numeric($database) || (int) $database < 0) {
            throw new ConnectionConfigException(
                "Database name should be integer 0-9"
            );
        }
        $this->database = (int) $database;
    }

    public function getDatabase(): int
    {
        return $this->database;
    }
}
