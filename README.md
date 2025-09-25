# central-services-stream

[![Git Commit](https://img.shields.io/github/last-commit/mojaloop/central-services-stream.svg?style=flat)](https://github.com/mojaloop/central-services-stream/commits/main)
[![Git Releases](https://img.shields.io/github/release/mojaloop/central-services-stream.svg?style=flat)](https://github.com/mojaloop/central-services-stream/releases)
[![Npm Version](https://img.shields.io/npm/v/@mojaloop/central-services-stream.svg?style=flat)](https://www.npmjs.com/package/@mojaloop/central-services-stream)
[![NPM Vulnerabilities](https://img.shields.io/snyk/vulnerabilities/npm/@mojaloop/central-services-stream.svg?style=flat)](https://www.npmjs.com/package/@mojaloop/central-services-stream)
[![CircleCI](https://circleci.com/gh/mojaloop/central-services-stream.svg?style=svg)](https://circleci.com/gh/mojaloop/central-services-stream)

Streaming library for central services

## CI/CD

This repository uses the [mojaloop/build](https://github.com/mojaloop/ci-config-orb-build) CircleCI orb for standardized CI/CD workflows, including automated Grype vulnerability scanning for source code security.

## Usage

### Kafka

Refer to documentation in [src/kafka/readme.md](./src/kafka/readme.md)

### Manually installing Librdkafka lib dependency

You can optionally install [Librdkafka](https://github.com/confluentinc/librdkafka).

> Recommended that if you choose to install it manually, ensure that the version being installed matches the version in the [Node-Rdkafka](https://github.com/Blizzard/node-rdkafka/releases) release notes. You are most likely better off (or if you have any doubts) using `npm install` to build the library natively.

Then configure the following env var before running `npm install`:

```env
export BUILD_LIBRDKAFKA=0
```

## Auditing Dependencies

We use `audit-ci` along with `npm audit` to check dependencies for node vulnerabilities, and keep track of resolved dependencies with an `audit-ci.jsonc` file.

To start a new resolution process, run:

```bash
npm run audit:fix
```

You can then check to see if the CI will pass based on the current dependencies with:

```bash
npm run audit:check
```

The [audit-ci.jsonc](./audit-ci.jsonc) contains any audit-exceptions that cannot be fixed to ensure that CircleCI will build correctly.

## Automated Releases

As part of our CI/CD process, we use a combination of CircleCI, standard-version
npm package and github-release CircleCI orb to automatically trigger our releases
and image builds. This process essentially mimics a manual tag and release.

On a merge to main, CircleCI is configured to use the mojaloopci github account
to push the latest generated CHANGELOG and package version number.

Once those changes are pushed, CircleCI will pull the updated main, tag and
push a release triggering another subsequent build that also publishes a docker image.

### Potential problems

* There is a case where the merge to main workflow will resolve successfully, triggering
  a release. Then that tagged release workflow subsequently failing due to the image scan,
  audit check, vulnerability check or other "live" checks.

  This will leave main without an associated published build. Fixes that require
  a new merge will essentially cause a skip in version number or require a clean up
  of the main branch to the commit before the CHANGELOG and bump.

  This may be resolved by relying solely on the previous checks of the
  merge to main workflow to assume that our tagged release is of sound quality.
  We are still mulling over this solution since catching bugs/vulnerabilities/etc earlier
  is a boon.

* It is unknown if a race condition might occur with multiple merges with main in
  quick succession, but this is a suspected edge case.
