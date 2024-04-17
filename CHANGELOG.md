# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.

### [11.2.4](https://github.com/mojaloop/central-services-stream/compare/v11.2.3...v11.2.4) (2024-03-19)


### Bug Fixes

* fix peer deps version matercher, update deps and ci config ([#137](https://github.com/mojaloop/central-services-stream/issues/137)) ([11b83ae](https://github.com/mojaloop/central-services-stream/commit/11b83ae7f0d640dbc29a11ae731ca5632db14ce0))

### [11.2.3](https://github.com/mojaloop/central-services-stream/compare/v11.2.2...v11.2.3) (2024-03-15)


### Bug Fixes

* disconnect the EventEmitter ([#136](https://github.com/mojaloop/central-services-stream/issues/136)) ([d1cdab6](https://github.com/mojaloop/central-services-stream/commit/d1cdab6e3098bea3e00728fa4471c4e9e0537e9d))

### [11.2.2](https://github.com/mojaloop/central-services-stream/compare/v11.2.0...v11.2.2) (2024-03-15)


### Bug Fixes

* disconnect from Kafka on process exit ([#134](https://github.com/mojaloop/central-services-stream/issues/134)) ([21f9119](https://github.com/mojaloop/central-services-stream/commit/21f911902439e78d466f948411fc53d80dddfa83))

## [11.2.0](https://github.com/mojaloop/central-services-stream/compare/v11.1.1...v11.2.0) (2023-09-29)


### Features

* **mojaloop/#3529:** update isConnected function for consumers/producers ([#133](https://github.com/mojaloop/central-services-stream/issues/133)) ([5f6e3fc](https://github.com/mojaloop/central-services-stream/commit/5f6e3fc20da51f790a18d5122987fca2a8489752)), closes [mojaloop/#3529](https://github.com/mojaloop/project/issues/3529)

### [11.1.1](https://github.com/mojaloop/central-services-stream/compare/v11.1.0...v11.1.1) (2023-08-29)

## [11.1.0](https://github.com/mojaloop/central-services-stream/compare/v11.0.0...v11.1.0) (2023-08-24)

## [11.0.0](https://github.com/mojaloop/central-services-stream/compare/v10.7.0...v11.0.0) (2022-05-19)


### ⚠ BREAKING CHANGES

* **mojaloop/#2092:** major version bump for node v16 LTS support, and re-structuring of project directories to align to core Mojaloop repositories!

### Features

* **mojaloop/#2092:** upgrade nodeJS version for core services ([#125](https://github.com/mojaloop/central-services-stream/issues/125)) ([35313c3](https://github.com/mojaloop/central-services-stream/commit/35313c3ca7997c7dc8425d8f678aa74706494e13)), closes [mojaloop/#2092](https://github.com/mojaloop/project/issues/2092)


### Bug Fixes

* [Security] Bump glob-parent from 5.1.1 to 5.1.2 ([#118](https://github.com/mojaloop/central-services-stream/issues/118)) ([41a0b5e](https://github.com/mojaloop/central-services-stream/commit/41a0b5e935b888b358bc37d678526dc89ec1a503))
* Bump lodash from 4.17.19 to 4.17.21 ([#117](https://github.com/mojaloop/central-services-stream/issues/117)) ([d07dd2e](https://github.com/mojaloop/central-services-stream/commit/d07dd2e99c2b8f5f8ffa04c91ab8ad297e1504dc))
* Bump normalize-url from 4.5.0 to 4.5.1 ([#119](https://github.com/mojaloop/central-services-stream/issues/119)) ([ff3fbea](https://github.com/mojaloop/central-services-stream/commit/ff3fbea64707b74524350288293452cfddb87b2e))
* updated peerDependencies as NPM v7+ handles their resolution for us ([f22ece0](https://github.com/mojaloop/central-services-stream/commit/f22ece0d30d71e7f132ea3bdf6e561fc75ae73b7))
