'use strict';

var _regenerator = require('babel-runtime/regenerator');

var _regenerator2 = _interopRequireDefault(_regenerator);

var _extends2 = require('babel-runtime/helpers/extends');

var _extends3 = _interopRequireDefault(_extends2);

var _asyncToGenerator2 = require('babel-runtime/helpers/asyncToGenerator');

var _asyncToGenerator3 = _interopRequireDefault(_asyncToGenerator2);

var run = function () {
  var _ref = (0, _asyncToGenerator3.default)(_regenerator2.default.mark(function _callee2(url) {
    var _this = this;

    var mainTimer, idx, metrics;
    return _regenerator2.default.wrap(function _callee2$(_context2) {
      while (1) {
        switch (_context2.prev = _context2.next) {
          case 0:
            dbg('run: url=%o', url);

            mainTimer = new _timer2.default('main');

            mainTimer.start();
            idx = 0;
            metrics = {
              providerInsertCount: 0,
              providerUpdateCount: 0,
              locationInsertCount: 0,
              locationUpdateCount: 0,
              providerLocationUpsertCount: 0
            };
            _context2.prev = 5;
            return _context2.delegateYield(_regenerator2.default.mark(function _callee() {
              var db, cursor, timer, thresh, source, providerKey, provider, _result, locationKey, location, _result2, providerLocationKey, result;

              return _regenerator2.default.wrap(function _callee$(_context) {
                while (1) {
                  switch (_context.prev = _context.next) {
                    case 0:
                      _context.next = 2;
                      return client.connect(url);

                    case 2:
                      db = _context.sent;

                      (0, _assert2.default)(db);

                      // clean
                      _lodash2.default.forEach(['cmsProviders', 'cmsLocations', 'cmsProviderLocations'], function (c) {
                        db.dropCollection(c);
                      });

                      // indexes
                      db.collection('cmsProviders').createIndex({ npi: 1 }, { unique: true });
                      db.collection('cmsLocations').createIndex({
                        orgKey: 1,
                        'address.addressLine1': 1,
                        'address.city': 1,
                        'address.state': 1,
                        'address.zip': 1
                      }, { unique: true });
                      db.collection('cmsProviderLocations').createIndex({ providerId: 1, locationId: 1 }, { unique: true });

                      // transform
                      cursor = db.collection('cmsProviderLocationsOriginal').find({}).addCursorFlag('noCursorTimeout', true).batchSize(100);
                      //.limit(1000)

                      timer = new _timer2.default('xform');
                      thresh = 1000;

                    case 11:
                      _context.next = 13;
                      return cursor.hasNext();

                    case 13:
                      if (!_context.sent) {
                        _context.next = 57;
                        break;
                      }

                      timer.start();
                      _context.next = 17;
                      return cursor.next();

                    case 17:
                      source = _context.sent;

                      //dbg('source[%o]=%o', idx, toString(source))

                      // upsert provider
                      //
                      providerKey = { npi: source.npi };
                      _context.next = 21;
                      return db.collection('cmsProviders').findOne(providerKey);

                    case 21:
                      provider = _context.sent;

                      if (!provider) {
                        _context.next = 26;
                        break;
                      }

                      //dbg('found npi=%o', provider.npi)
                      // same provider diff location
                      // add to it's location array
                      metrics.providerUpdateCount++;
                      _context.next = 32;
                      break;

                    case 26:
                      provider = (0, _extends3.default)({}, providerKey, {
                        firstName: source.firstName,
                        lastName: source.lastName,
                        middleName: source.middleName,
                        specialties: [source.specialty]
                      });
                      _context.next = 29;
                      return db.collection('cmsProviders').insertOne(provider);

                    case 29:
                      _result = _context.sent;

                      _assert2.default.equal(_result.insertedCount, 1);
                      metrics.providerInsertCount++;

                    case 32:

                      // upsert location
                      //
                      locationKey = {
                        orgKey: source.groupPac || source.npi,
                        address: getAddress(source)
                      };
                      _context.next = 35;
                      return db.collection('cmsLocations').findOne(locationKey);

                    case 35:
                      location = _context.sent;

                      if (!location) {
                        _context.next = 40;
                        break;
                      }

                      //dbg('found location=%o', locationKey)
                      // same location diff provider
                      // add to it's provider array
                      metrics.locationUpdateCount++;
                      _context.next = 46;
                      break;

                    case 40:
                      location = (0, _extends3.default)({}, locationKey, {
                        orgName: source.orgName || getFullName(source),
                        phone: source.phone
                      });
                      _context.next = 43;
                      return db.collection('cmsLocations').insertOne(location);

                    case 43:
                      _result2 = _context.sent;

                      _assert2.default.equal(_result2.insertedCount, 1);
                      metrics.locationInsertCount++;

                    case 46:

                      // upsert provider-location
                      providerLocationKey = {
                        providerId: provider._id,
                        locationId: location._id
                      };
                      _context.next = 49;
                      return db.collection('cmsProviderLocations').updateOne(providerLocationKey, (0, _extends3.default)({}, providerLocationKey, {
                        npi: source.npi
                      }, locationKey), {
                        upsert: true
                      });

                    case 49:
                      result = _context.sent;

                      //dbg('result=%o', result)
                      (0, _assert2.default)(result.modifiedCount == 1 || result.upsertedCount == 1);
                      metrics.providerLocationUpsertCount++;
                      timer.stop();
                      if (timer.count() % thresh == 0) {
                        dbg('timer=%o', timer.toString());
                        dbg('heap-used=%o', process.memoryUsage().heapUsed);
                      }
                      idx++;
                      _context.next = 11;
                      break;

                    case 57:
                      db.close();
                      mainTimer.stop();
                      dbg('successfully xformed [%o] providers in [%s] seconds, metrics=%o', idx, (mainTimer.total() / 1000).toFixed(3), metrics);

                    case 60:
                    case 'end':
                      return _context.stop();
                  }
                }
              }, _callee, _this);
            })(), 't0', 7);

          case 7:
            _context2.next = 13;
            break;

          case 9:
            _context2.prev = 9;
            _context2.t1 = _context2['catch'](5);

            dbg('connect: caught=%o', _context2.t1);
            process.exit(1);

          case 13:
          case 'end':
            return _context2.stop();
        }
      }
    }, _callee2, this, [[5, 9]]);
  }));

  return function run(_x) {
    return _ref.apply(this, arguments);
  };
}();

var _debug = require('debug');

var _debug2 = _interopRequireDefault(_debug);

var _assert = require('assert');

var _assert2 = _interopRequireDefault(_assert);

var _mongodb = require('mongodb');

var _mongodb2 = _interopRequireDefault(_mongodb);

var _timer = require('./timer');

var _timer2 = _interopRequireDefault(_timer);

var _lodash = require('lodash');

var _lodash2 = _interopRequireDefault(_lodash);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

//import stringify from 'json-stringify-safe'

var client = _mongodb2.default.MongoClient;

var dbg = (0, _debug2.default)('app:provider-transformer');

run('mongodb://localhost:27017/test');

function getAddress(record) {
  // ignore addressLine2
  return {
    addressLine1: record.addressLine1,
    city: record.city,
    state: record.state,
    zip: record.zip
  };
}

function getFullName(o) {
  return o.lastName + ', ' + o.firstName + (o.middleName && ' ' + o.middleName);
}
// function toString(p) {
//   return `npi=${p.npi}, name=${p.lastName}, ${p.firstName} ${p.middleName || ''}`
// }