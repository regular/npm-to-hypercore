# pull-npm-registry

A simple data fetcher that provides all changes made to the npm registry as a pull-stream. Streams old and live data.

Inspired by [npm-to-hypercore](https://github.com/watson/npm-to-hypercore)

## Installation

```
npm install pull-npm-registry
```

## Usage

```
var pull = require('pull-stream')
var npm = require('pull-npm-registry')

pull(
  npm(0), // give it the first sequence number you want
  pull.log()
)
```

## License

MIT
