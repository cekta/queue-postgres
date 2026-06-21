<?php

namespace Cekta\Queue\Postgres;

use Cekta\Queue\Postgres\Exception\TaskHandlerNotFound;
use Cekta\Queue\Status;
use DateTimeInterface;
use JsonSerializable;
use PDO;
use Psr\Clock\ClockInterface;
use Ramsey\Uuid\Uuid;

readonly class Producer
{
    /**
     * @param PDO $pdo
     * @param array<string, string> $handlers
     * @param ClockInterface $clock
     * @param string $table
     * @param string $queueName
     */
    public function __construct(
        private PDO $pdo,
        private array $handlers,
        private ClockInterface $clock,
        private string $table = "tasks",
        private string $queueName = "queue_default",
    ) {
    }

    public function send(JsonSerializable $payload): string
    {
        $fqcn = get_class($payload);
        if (!array_key_exists($fqcn, $this->handlers)) {
            throw new TaskHandlerNotFound($fqcn);
        }
        $handler = $this->handlers[$fqcn];
        $uuid = Uuid::uuid7();
        $this->pdo->beginTransaction();
        $query = sprintf(
            '
INSERT INTO "%s" (
    "uuid",
    "queue_name",
    "handler",
    "fqcn",
    "payload",
    "created_at", 
    "status"
) VALUES (
    :uuid, 
    :queue_name,
    :handler,
    :fqcn,
    :payload,
    :created_at,
    :status
)',
            $this->table
        );
        $sth = $this->pdo->prepare($query);
        $sth->execute([
            'uuid' => $uuid->toString(),
            'queue_name' => $this->queueName,
            'fqcn' => $payload::class,
            'payload' => json_encode($payload),
            'created_at' => $this->clock->now()->format(DateTimeInterface::RFC3339),
            'status' => Status::PENDING->value,
            'handler' => $handler
        ]);

        $sth = $this->pdo->prepare("INSERT INTO {$this->queueName}(uuid) VALUES (:uuid)");
        $sth->execute([
            'uuid' => $uuid->toString()
        ]);

        $this->pdo->commit();
        return $uuid->__toString();
    }
}
