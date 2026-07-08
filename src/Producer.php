<?php

namespace Cekta\Queue\Postgres;

use Cekta\Queue\Postgres\Exception\TaskHandlerNotFound;
use JsonSerializable;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidFactory;

readonly class Producer implements \Cekta\Queue\Producer
{
    /**
     * @param array<string, string> $handlers
     */
    public function __construct(
        private TaskRepository $taskRepository,
        private array $handlers,
        private UuidFactory $uuidFactory,
    ) {
    }

    public function push(JsonSerializable $payload): string
    {
        $uuid = $this->uuidFactory->uuid7();
        $fqcn = get_class($payload);
        if (!array_key_exists($fqcn, $this->handlers)) {
            throw new TaskHandlerNotFound($fqcn);
        }
        $this->taskRepository->push($uuid->toString(), $payload, $this->handlers[$fqcn]);
        return $uuid->__toString();
    }
}
