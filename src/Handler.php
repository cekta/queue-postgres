<?php

declare(strict_types=1);

namespace Cekta\Queue\Postgres;

interface Handler
{
    public function handle(TaskDTO $taskDTO): bool;
}
