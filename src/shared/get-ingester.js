import debug from 'debug'
import Timer from 'tymer'
import assert from 'assert'
import minimist from 'minimist'
import {connect} from '../shared/helper'

const argv = minimist(process.argv.slice(2))
const url = argv.url || 'mongodb://localhost:27017/test'

export default function(
  {
    debugName,
    sourceName,
    targetName,
    targetIndices,
    steps
  }
) {
  const dbg = debug(`app:${debugName}`)
  dbg('argv=%o', argv)

  return async function() {
    dbg('run: url=%o', url)

    const mainTimer = new Timer('main')
    mainTimer.start()

    const _sourceName = argv.sourceName || sourceName
    const _targetName = argv.targetName || targetName

    dbg('run args: url=%o, source=%o, target=%o', url, _sourceName, _targetName)

    try {
      const db = await connect(url)
      const source = db.collection(_sourceName)
      const target = db.collection(_targetName)

      targetIndices && targetIndices.forEach((targetIndex)=>{
        target.createIndex(targetIndex)
      })

      const count = await source.count()
      const limit = argv.limit || count
      const query = argv.query ? JSON.parse(argv.query) : {}

      dbg('begin aggregation: source-count=%o, query=%o, limit=%o', count, query, limit)

      const result = await source.aggregate(
        [
          {$match: query},
          {$limit: limit}
        ]
        .concat(steps)
        .concat([{$out: _targetName}]),
        {allowDiskUse: true}
      )
      .toArray()

      assert(result)

      db.close()
      mainTimer.stop()
      dbg(
        'successfully aggregated [%o] records from [%s] to [%s] in [%s] seconds',
        count,
        _sourceName,
        _targetName,
        (mainTimer.total()/1000).toFixed(3)
      )
    }
    catch (caught) {
      dbg('connect: caught=%o', caught)
      process.exit(1)
    }
  }
}
