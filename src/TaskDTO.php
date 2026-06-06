<?php

declare(strict_types=1);

namespace Cekta\Queue\Postgres;

use DateTimeImmutable;

readonly class TaskDTO
{
    public function __construct(
        public string $uuid,
        public string $fqcn,
        public string $handler,
        public mixed $payload,
        public Status $status,
        public DateTimeImmutable $created_at,
        public ?DateTimeImmutable $started_at,
        public ?DateTimeImmutable $finished_at,
    ) {
    }
}
