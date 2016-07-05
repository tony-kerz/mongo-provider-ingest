'use strict';

var _regenerator = require('babel-runtime/regenerator');

var _regenerator2 = _interopRequireDefault(_regenerator);

var _asyncToGenerator2 = require('babel-runtime/helpers/asyncToGenerator');

var _asyncToGenerator3 = _interopRequireDefault(_asyncToGenerator2);

var ingest = function () {
  var _ref = (0, _asyncToGenerator3.default)(_regenerator2.default.mark(function _callee(url) {
    var db, record, count;
    return _regenerator2.default.wrap(function _callee$(_context) {
      while (1) {
        switch (_context.prev = _context.next) {
          case 0:
            _context.prev = 0;
            _context.next = 3;
            return client.connect(url);

          case 3:
            db = _context.sent;

            (0, _assert2.default)(db, 'db required');
            record = void 0;
            count = 0;
            // eslint-disable-next-line no-cond-assign

          case 7:
            if (!(record = parser.read())) {
              _context.next = 14;
              break;
            }

            count++;
            dbg('ingest: record=%o', record);

            if (!(count == 10)) {
              _context.next = 12;
              break;
            }

            return _context.abrupt('return', null);

          case 12:
            _context.next = 7;
            break;

          case 14:
            _context.next = 19;
            break;

          case 16:
            _context.prev = 16;
            _context.t0 = _context['catch'](0);

            dbg('caught=%o', _context.t0);

          case 19:
            return _context.abrupt('return', null);

          case 20:
          case 'end':
            return _context.stop();
        }
      }
    }, _callee, this, [[0, 16]]);
  }));

  return function ingest(_x) {
    return _ref.apply(this, arguments);
  };
}();

var _debug = require('debug');

var _debug2 = _interopRequireDefault(_debug);

var _assert = require('assert');

var _assert2 = _interopRequireDefault(_assert);

var _fs = require('fs');

var _fs2 = _interopRequireDefault(_fs);

var _csvParse = require('csv-parse');

var _csvParse2 = _interopRequireDefault(_csvParse);

var _mongodb = require('mongodb');

var _mongodb2 = _interopRequireDefault(_mongodb);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var dbg = (0, _debug2.default)('app:provider-ingest');
//import Timer from './timer'
//import stringify from 'json-stringify-safe'

var input = _fs2.default.createReadStream('./src/providers.csv');
var parser = (0, _csvParse2.default)({ columns: true });
var client = _mongodb2.default.MongoClient;

parser.on('readable', function () {
  ingest('mongodb://localhost:27017/test').then(function () {
    dbg('ingest.then');
  });
  return null;
});

parser.on('error', function (err) {
  dbg('err=%o', err);
});

parser.on('finish', function () {
  dbg('done...');
});

input.pipe(parser);