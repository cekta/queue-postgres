<?php

declare(strict_types=1);

namespace Cekta\Queue\Postgres\Test\Unit;

use Cekta\Queue\Postgres\Exception\TaskHandlerNotFound;
use Cekta\Queue\Postgres\Producer;
use Cekta\Queue\Postgres\TaskRepository;
use Cekta\Queue\Postgres\Test\Fixture\ExampleHandler;
use Cekta\Queue\Postgres\Test\Fixture\ExampleTask;
use JsonSerializable;
use Mockery;
use Mockery\MockInterface;
use Ramsey\Uuid\Uuid;
use Ramsey\Uuid\UuidFactory;
use Testo\Assert;
use Testo\Expect;
use Testo\Lifecycle\AfterTest;

class ProducerTest
{
    private Producer $producer;
    private UuidFactory&MockInterface $uuidFactory;

    private TaskRepository&MockInterface $repository;

    public function __construct()
    {
        $this->uuidFactory = mock(UuidFactory::class);
        $this->repository = mock(TaskRepository::class);
        $handlers = [
            ExampleTask::class => ExampleHandler::class
        ];
        $this->producer = new Producer($this->repository, $handlers, $this->uuidFactory);
    }

    #[AfterTest]
    public function afterTest(): void
    {
        Mockery::close();
    }

    public function testPush(): void
    {
        $task = new ExampleTask('test payload');
        $uuid = Uuid::fromString('019f42cb-6262-77d2-9382-77720137ad5e');
        $this->uuidFactory->allows(['uuid7' => $uuid]);
        $this->repository
            ->expects()
            ->push($uuid->toString(), $task, ExampleHandler::class)
            ->once();
        $result = $this->producer->push($task);
        Assert::equals($result, $uuid->toString());
    }

    public function testPushNotFoundHandler()
    {
        Expect::exception(TaskHandlerNotFound::class);
        $task = new class implements JsonSerializable {
            public function jsonSerialize(): mixed
            {
                return 'nothing';
            }
        };
        $this->producer->push($task);
    }
}
