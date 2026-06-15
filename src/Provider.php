<?php

declare(strict_types=1);

namespace Cekta\Queue\Postgres;

use Psr\Container\ContainerInterface;
use RuntimeException;

class Provider implements HandlerProvider
{
    public function __construct(
        private ContainerInterface $container
    ) {
    }

    public function getHandler(string $name): Handler
    {
        $handler = $this->container->get($name);
        if (($handler instanceof Handler) === false) {
            throw new RuntimeException("$name is not a Handler");
        }
        return $handler;
    }
}
