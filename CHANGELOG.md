# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.

### [11.6.1](https://github.com/mojaloop/central-services-stream/compare/v11.6.0...v11.6.1) (2025-06-11)


### Bug Fixes

* **csi-1567:** switch back to using metadata for consumer connection tests ([#164](https://github.com/mojaloop/central-services-stream/issues/164)) ([45a5142](https://github.com/mojaloop/central-services-stream/commit/45a51423fe82b408cbf57b5c5f5d3ebfdbfd80f2))

## [11.6.0](https://github.com/mojaloop/central-services-stream/compare/v11.5.2...v11.6.0) (2025-05-14)


### Features

* bump up node to v22.15.0 ([#163](https://github.com/mojaloop/central-services-stream/issues/163)) ([ca1f92d](https://github.com/mojaloop/central-services-stream/commit/ca1f92d9560cba78fc48461fab151107ae296e49))

### [11.5.2](https://github.com/mojaloop/central-services-stream/compare/v11.5.1...v11.5.2) (2025-03-24)

### [11.5.1](https://github.com/mojaloop/central-services-stream/compare/v11.5.0...v11.5.1) (2025-02-21)

## [11.5.0](https://github.com/mojaloop/central-services-stream/compare/v11.4.4...v11.5.0) (2025-02-20)


### Features

* **csi-1161:** added otel to producer and consumer ([#155](https://github.com/mojaloop/central-services-stream/issues/155)) ([4244330](https://github.com/mojaloop/central-services-stream/commit/4244330b6fc76f0a99205053c2466682a25b389f))

### [11.4.4](https://github.com/mojaloop/central-services-stream/compare/v11.4.3...v11.4.4) (2025-02-20)

### [11.4.3](https://github.com/mojaloop/central-services-stream/compare/v11.4.2...v11.4.3) (2025-01-27)

### [11.4.2](https://github.com/mojaloop/central-services-stream/compare/v11.4.1...v11.4.2) (2025-01-07)

### [11.4.1](https://github.com/mojaloop/central-services-stream/compare/v11.4.0...v11.4.1) (2024-12-16)

## [11.4.0](https://github.com/mojaloop/central-services-stream/compare/v11.3.1...v11.4.0) (2024-12-13)


### Features

* throw SERVICE_CURRENTLY_UNAVAILABLE in Producer if the lag is above the configured maxLag ([#152](https://github.com/mojaloop/central-services-stream/issues/152)) ([3ff2bb7](https://github.com/mojaloop/central-services-stream/commit/3ff2bb7c06068fe99f11f8e317182d9e9c68c5c6))

### [11.3.1](https://github.com/mojaloop/central-services-stream/compare/v11.3.0...v11.3.1) (2024-06-12)

## [11.3.0](https://github.com/mojaloop/central-services-stream/compare/v11.2.6...v11.3.0) (2024-05-24)


### Features

* enable Kafka configuration via the environment or convict ([#143](https://github.com/mojaloop/central-services-stream/issues/143)) ([11cee90](https://github.com/mojaloop/central-services-stream/commit/11cee90bc8af52bdd2de7c41ea9edf5bf82a3cf4))


### Bug Fixes

* alpine image ([#144](https://github.com/mojaloop/central-services-stream/issues/144)) ([8aba68c](https://github.com/mojaloop/central-services-stream/commit/8aba68c7a7bed626725d925a208e4d46a18f1670))

### [11.2.6](https://github.com/mojaloop/central-services-stream/compare/v11.2.5...v11.2.6) (2024-05-16)


### Bug Fixes

* cannot build ml-api-adapter with current version of central-services-stream ([#141](https://github.com/mojaloop/central-services-stream/issues/141)) ([68e9cb9](https://github.com/mojaloop/central-services-stream/commit/68e9cb93fbb0f69dd0af2147b5a461357c0edf0a))

### [11.2.5](https://github.com/mojaloop/central-services-stream/compare/v11.2.4...v11.2.5) (2024-04-25)


### Bug Fixes

* **mojaloop/#3067:** removed disconnected producers from listOfProducers map ([#139](https://github.com/mojaloop/central-services-stream/issues/139)) ([352fc28](https://github.com/mojaloop/central-services-stream/commit/352fc28b3de7c62732510b10649f450802347889)), closes [mojaloop/#3067](https://github.com/mojaloop/project/issues/3067)

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
