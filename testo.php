<?php

declare(strict_types=1);

use Testo\Application\Config\ApplicationConfig;
use Testo\Application\Config\SuiteConfig;
use Testo\Codecov\CodecovPlugin;
use Testo\Codecov\Report\PhpUnitXmlReport;
use Testo\Convention\NamingConventionPlugin;

return new ApplicationConfig(
    src: ["src"],
    suites: [
        new SuiteConfig(name: "Unit", location: ["tests/Unit"]),
        new SuiteConfig(
            name: "Feature",
            location: ["tests/Feature"],
        ),
    ],
    plugins: [
        new CodecovPlugin(
            reports: [
                // required fpr infection
                new PhpUnitXmlReport("/tmp/infection/infection/coverage-xml"),
            ],
        ),
        new NamingConventionPlugin(),
    ],
);
