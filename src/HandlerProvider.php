<?php

declare(strict_types=1);

namespace Cekta\Queue\Postgres;

interface HandlerProvider
{
    public function getHandler(string $name): Handler;
}
